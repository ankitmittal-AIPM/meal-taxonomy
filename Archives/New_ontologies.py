from __future__ import annotations

# ontologies.py
"""
Module to map ingredients and recipes to ontology nodes (FoodOn, custom).

This module provides helpers to:

- load ontology nodes (e.g. FoodOn) from CSV
- upsert nodes into the `ontology_nodes` table
- create `entity_ontology_links` between ingredients/recipes and ontology nodes
- inspect a sample of ontology nodes from Supabase.
"""

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

import csv
from pathlib import Path

from supabase import Client

from meal_taxonomy.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class OntologyNode:
    id: str
    label: str
    path: str
    ontology_type: str


def load_foodon_terms(
    csv_path: Path,
    id_col: str = "id",
    label_col: str = "label",
    path_col: str = "path",
) -> List[OntologyNode]:
    """Load FoodOn terms from a CSV exported from Neo4j/ETL step."""
    nodes: List[OntologyNode] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            node = OntologyNode(
                id=row[id_col],
                label=row[label_col],
                path=row[path_col],
                ontology_type="FOODON",
            )
            nodes.append(node)
    logger.info("Loaded %d FoodOn nodes from %s", len(nodes), csv_path)
    return nodes


def upsert_ontology_nodes(
    supabase: Client,
    nodes: Iterable[OntologyNode],
) -> None:
    """Upsert ontology nodes into the 'ontology_nodes' table."""
    batch: List[Dict] = []
    for node in nodes:
        batch.append(
            {
                "ontology_type": node.ontology_type,
                "node_id": node.id,
                "label": node.label,
                "path": node.path,
            }
        )

        if len(batch) >= 500:
            _flush_ontology_batch(supabase, batch)
            batch.clear()

    if batch:
        _flush_ontology_batch(supabase, batch)


def _flush_ontology_batch(supabase: Client, batch: List[Dict]) -> None:
    """Helper to write a batch of ontology nodes."""
    logger.info("Upserting %d ontology nodes...", len(batch))
    supabase.table("ontology_nodes").upsert(
        batch, on_conflict="ontology_type,node_id"
    ).execute()


def link_ingredient_to_ontology(
    supabase: Client,
    ingredient_id: int,
    node: OntologyNode,
) -> None:
    """Create or update link between an ingredient and an ontology node."""
    logger.debug(
        "Linking ingredient %s to ontology node %s (%s)",
        ingredient_id,
        node.id,
        node.label,
    )
    supabase.table("entity_ontology_links").upsert(
        {
            "entity_type": "ingredient",
            "entity_id": ingredient_id,
            "ontology_type": node.ontology_type,
            "node_id": node.id,
        },
        on_conflict="entity_type,entity_id,ontology_type,node_id",
    ).execute()


def link_recipe_to_ontology(
    supabase: Client,
    recipe_id: int,
    node: OntologyNode,
) -> None:
    """Create or update link between a recipe and an ontology node."""
    logger.debug(
        "Linking recipe %s to ontology node %s (%s)",
        recipe_id,
        node.id,
        node.label,
    )
    supabase.table("entity_ontology_links").upsert(
        {
            "entity_type": "recipe",
            "entity_id": recipe_id,
            "ontology_type": node.ontology_type,
            "node_id": node.id,
        },
        on_conflict="entity_type,entity_id,ontology_type,node_id",
    ).execute()


def get_foodon_node_for_ingredient_label(
    nodes: Sequence[OntologyNode],
    ingredient_label: str,
) -> Optional[OntologyNode]:
    """Simple heuristic match of an ingredient label to a FoodOn node."""
    norm_label = ingredient_label.strip().lower()
    for node in nodes:
        if node.label.strip().lower() == norm_label:
            return node
    return None


def bulk_link_ingredients_to_foodon(
    supabase: Client,
    nodes: Sequence[OntologyNode],
    ingredient_id_to_label: Dict[int, str],
) -> None:
    """Try linking many ingredients to FoodOn nodes using simple label match."""
    for ingredient_id, label in ingredient_id_to_label.items():
        node = get_foodon_node_for_ingredient_label(nodes, label)
        if node is None:
            continue
        link_ingredient_to_ontology(supabase, ingredient_id, node)


def list_ontology_nodes(
    supabase: Client,
    ontology_type: str,
    limit: int = 200,
) -> List[OntologyNode]:
    """Fetch a sample of ontology nodes from the DB for debugging/inspection."""
    result = (
        supabase.table("ontology_nodes")
        .select("ontology_type,node_id,label,path")
        .eq("ontology_type", ontology_type)
        .limit(limit)
        .execute()
    )

    nodes: List[OntologyNode] = []
    for row in result.data or []:
        nodes.append(
            OntologyNode(
                id=row["node_id"],
                label=row["label"],
                path=row["path"],
                ontology_type=row["ontology_type"],
            )
        )

    return nodes
