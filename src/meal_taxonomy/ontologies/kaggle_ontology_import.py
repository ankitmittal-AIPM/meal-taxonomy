"""
kaggle_ontology_import.py

Purpose (High-Level):
    Promote Kaggle dataset metadata (region, cuisine, course, diet) into
    ontology_nodes and link meals to those nodes via entity_ontology_links.

Why:
    - Creates a lightweight ontology from source metadata.
    - Enables unified graph reasoning across Kaggle, FoodOn, FKG, RecipeDB.
    - Supports semantic search and recommendations.

Logging:
    Structured logs via logging_utils.get_logger()
"""

from __future__ import annotations

from typing import Dict, Set
from src.meal_taxonomy.config import get_supabase_client
from taxonomy.taxonomy_seed import ensure_tag_type, ensure_tag
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("kaggle_ontology_import")


def upsert_ontology_node(client, iri, label, source, kind) -> str:
    """
    Insert/lookup ontology_nodes row for a Kaggle concept (no IRI needed).

    Args:
        iri:    None for Kaggle concepts
        label:  e.g. 'South Indian'
        source: 'Kaggle'
        kind:   'cuisine' | 'course' | 'diet'

    Returns:
        ontology_nodes.id (UUID)
    """
    lookup = (
        client.table("ontology_nodes")
        .select("id")
        .eq("label", label)
        .eq("source", source)
        .eq("kind", kind)
        .limit(1)
        .execute()
    )

    if lookup.data:
        return lookup.data[0]["id"]

    ins = client.table("ontology_nodes").insert(
        {"iri": iri, "label": label, "source": source, "kind": kind}
    ).execute()
    return ins.data[0]["id"]


def link_meals_to_node(client, node_id: str, meal_ids: Set[str]) -> None:
    """
    Create entity_ontology_links for meals → ontology node.

    Args:
        node_id: ontology_nodes.id
        meal_ids: set of Supabase meal IDs
    """
    rows = []
    for m in meal_ids:
        rows.append(
            {
                "entity_type": "meal",
                "entity_id": m,
                "ontology_node_id": node_id,
                "confidence": 0.9,
                "source": "Kaggle",
            }
        )

    if rows:
        client.table("entity_ontology_links").upsert(rows).execute()


def main() -> None:
    """
    1) Load meals and extract region/course/diet fields from meals.meta.
    2) Build buckets of concepts.
    3) Create ontology_nodes for each concept.
    4) Link meals to the nodes.
    """
    client = get_supabase_client()

    logger.info(
        "Starting Kaggle ontology import",
        extra={
            "invoking_func": "main",
            "invoking_purpose": "Create ontology nodes from Kaggle metadata",
            "next_step": "Query meals and process metadata",
            "resolution": "",
        },
    )

    meals_res = client.table("meals").select("id, meta").execute()
    meals = meals_res.data or []

    buckets: Dict[tuple[str, str], Set[str]] = {}

    for m in meals:
        mid = m["id"]
        meta = m.get("meta") or {}

        region = meta.get("region")
        course = meta.get("course")
        diet = meta.get("diet")

        if region:
            buckets.setdefault(("cuisine", region), set()).add(mid)
        if course:
            buckets.setdefault(("course", course), set()).add(mid)
        if diet:
            buckets.setdefault(("diet", diet), set()).add(mid)

    # Create ontology_nodes + links
    for (kind, label), meal_ids in buckets.items():
        try:
            node_id = upsert_ontology_node(
                client, iri=None, label=label, source="Kaggle", kind=kind
            )

            link_meals_to_node(client, node_id, meal_ids)

            logger.info(
                "Linked %d meals to Kaggle concept '%s' (%s)",
                len(meal_ids),
                label,
                kind,
                extra={
                    "invoking_func": "main",
                    "invoking_purpose": "Ontology creation for Kaggle metadata",
                    "next_step": "Process next concept",
                    "resolution": "",
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to import Kaggle ontology concept '%s' (%s): %s",
                label,
                kind,
                exc,
                extra={
                    "invoking_func": "main",
                    "invoking_purpose": "Ontology creation for Kaggle metadata",
                    "next_step": "Skip concept, continue with next",
                    "resolution": (
                        "Inspect meals.meta formatting, DB constraints; fix and retry"
                    ),
                },
                exc_info=True,
            )

    logger.info(
        "Kaggle ontology import complete",
        extra={
            "invoking_func": "main",
            "invoking_purpose": "Ontology creation for Kaggle metadata",
            "next_step": "Exit script",
            "resolution": "",
        },
    )


if __name__ == "__main__":
    main()
