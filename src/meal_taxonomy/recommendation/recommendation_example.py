"""
recommendation_example.py

Example usage of the MealRecommender.

Run:
  python -m src.meal_taxonomy.recommendation.recommendation_example --user-id <uuid>

Requires:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY (or anon/authenticated key with RLS policies)
"""
from __future__ import annotations

import argparse

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.recommendation.recommender import MealRecommender, RecommendationRequest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--user-id", required=True)
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    client = get_supabase_client()
    rec = MealRecommender(client)

    out = rec.recommend_for_user(RecommendationRequest(user_id=args.user_id, limit=args.limit))
    for i, m in enumerate(out, start=1):
        print(f"{i:02d}. {m.title}  score={m.score:.3f}")
        for r in m.reasons:
            print("    -", r)


if __name__ == "__main__":
    main()
