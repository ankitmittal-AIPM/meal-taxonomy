#!/usr/bin/env python3
"""Backfill embeddings for canonical meals.

This script assumes you ran the DB migration that adds:
  - meals.embedding (vector)
  - meals.is_canonical (boolean)
  - public.refresh_meal_search_doc(uuid) (optional)
  - public.match_canonical_meals(...) (optional)

It will:
  1) select canonical meals missing embeddings
  2) compute embeddings using enrichment.embeddings.get_meal_embedding
  3) update the meals row
"""

from __future__ import annotations

import argparse
from typing import List, Dict

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.enrichment.embeddings import get_meal_embedding


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--batch", type=int, default=50)
    ap.add_argument("--only_missing", action="store_true", default=True)
    args = ap.parse_args()

    client = get_supabase_client()

    q = client.table("meals").select("id,title,search_text,embedding").eq("is_canonical", True)
    res = q.limit(args.limit).execute()

    rows = res.data or []
    print(f"Found {len(rows)} canonical meals to embed")

    updates: List[Dict] = []
    for r in rows:
        if args.only_missing and r.get("embedding") is not None:
            continue
        text = (r.get("search_text") or r.get("title") or "").strip()
        emb = get_meal_embedding(text)
        if not emb:
            continue
        updates.append({"id": r["id"], "embedding": emb})

        if len(updates) >= args.batch:
            client.table("meals").upsert(updates, on_conflict="id").execute()
            updates = []

    if updates:
        client.table("meals").upsert(updates, on_conflict="id").execute()

    print("Done.")


if __name__ == "__main__":
    main()
