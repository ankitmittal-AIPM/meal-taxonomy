from __future__ import annotations

from typing import Dict, Set

from config import get_supabase_client


def upsert_ontology_node(client, iri: str | None, label: str, source: str, kind: str) -> str:
    """
    Simple SELECT-then-INSERT to avoid ON CONFLICT headaches.
    iri can be None for Kaggle concepts; we use (label, source, kind) as key.
    """
    q = (
        client.table("ontology_nodes")
        .select("id")
        .eq("label", label)
        .eq("source", source)
        .eq("kind", kind)
        .limit(1)
        .execute()
    )
    if q.data:
        return q.data[0]["id"]

    res = client.table("ontology_nodes").insert(
        {"iri": iri, "label": label, "source": source, "kind": kind}
    ).execute()
    return res.data[0]["id"]


def link_meals_to_node(client, node_id: str, meal_ids: Set[str], link_source: str) -> None:
    rows = []
    for meal_id in meal_ids:
        rows.append(
            {
                "entity_type": "meal",
                "entity_id": meal_id,
                "ontology_node_id": node_id,
                "confidence": 0.9,
                "source": link_source,
            }
        )
    if rows:
        client.table("entity_ontology_links").upsert(rows).execute()


def main() -> None:
    client = get_supabase_client()

    # 1) Pull relevant fields from meals.meta if you stored them,
    #    or from tags if you prefer. Here we assume meta contains them.
    res = client.table("meals").select("id, meta").execute()
    meals = res.data or []

    # Collect mapping: (kind, value) -> set(meal_id)
    buckets: Dict[tuple[str, str], Set[str]] = {}

    for row in meals:
        mid = row["id"]
        meta = row.get("meta") or {}
        region = (meta.get("region") or "").strip()
        course = (meta.get("course") or "").strip()
        diet = (meta.get("diet") or "").strip()

        if region:
            key = ("cuisine", region)
            buckets.setdefault(key, set()).add(mid)
        if course:
            key = ("course", course)
            buckets.setdefault(key, set()).add(mid)
        if diet:
            key = ("diet", diet)
            buckets.setdefault(key, set()).add(mid)

    # 2) For each distinct (kind, label), create ontology_node
    for (kind, label), meal_ids in buckets.items():
        node_id = upsert_ontology_node(
            client,
            iri=None,
            label=label,
            source="Kaggle",
            kind=kind,
        )
        link_meals_to_node(client, node_id, meal_ids, link_source="Kaggle")

    print("[kaggle] Linked meals to Kaggle cuisine/course/diet ontology nodes.")


if __name__ == "__main__":
    main()
