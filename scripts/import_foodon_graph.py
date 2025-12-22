# scripts/import_foodon_graph.py
"""
Purpose:
    Import the FoodOn graph (class → parent-class edges) into Supabase so that:
    a. ontology_nodes contains FoodOn concepts (IRIs + labels)
    b. ontology_relations contains “is_a” edges between those nodes

Assumptions:
    a. You've FoodOn OWL/TTL file downloaded locally (e.g., data/foodon.owl)
    b. SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are set in your environment (.env)
    c. The Supabase DB has ontology_nodes and ontology_relations tables set up.

What it does:
    - Parses the FoodOn OWL/TTL file using rdflib.
    - For each class and its rdfs:subClassOf relations, ensures nodes exist in  ontology_nodes and Upserts nodes:
        * Inserts new nodes in ontology_nodes table for FoodOn classes if not already present
    - Inserts subclass edges into ontology_relations with predicate="is_a"
"""

from __future__ import annotations

from typing import Dict, Optional
import sys
from pathlib import Path

import rdflib
from rdflib.namespace import RDFS
from supabase import Client

# --- Make project root importable so `src.*` imports work even when this
# --- script is executed from the `scripts/` directory.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.meal_taxonomy.config import get_supabase_client


def get_or_create_node_id(
    client: Client,
    iri: str,
    label: Optional[str],
    kind: str = "class",
    source: str = "FoodOn",
    cache: Optional[Dict[str, str]] = None,
) -> str:
    """
    Return ontology_nodes.id (UUID string) for a given IRI+source.
    Caches by IRI for performance.
    """
    if cache is not None and iri in cache:
        return cache[iri]

    res = (
        client.table("ontology_nodes")
        .select("id")
        .eq("iri", iri)
        .eq("source", source)
        .limit(1)
        .execute()
    )
    data = res.data or []
    if data:
        node_id = str(data[0]["id"])
    else:
        insert_payload = {
            "iri": iri,
            "label": label or iri,
            "kind": kind,
            "source": source,
        }
        ins = client.table("ontology_nodes").insert(insert_payload).execute()
        if not ins.data:
            raise RuntimeError(f"Failed to insert ontology_nodes for iri={iri}")
        node_id = str(ins.data[0]["id"])

    if cache is not None:
        cache[iri] = node_id
    return node_id


def import_foodon_graph(ontology_path: str, namespace_filter: str | None = None) -> None:
    """
    Parse FoodOn OWL/TTL and import subclass edges as is_a relations
    into ontology_relations (predicate TEXT, not predicate_iri).
    """
    client = get_supabase_client()

    print(f"Loading FoodOn ontology from {ontology_path}")
    g = rdflib.Graph()
    g.parse(ontology_path)
    print(f"Graph loaded: {len(g)} RDF triples")

    def in_namespace(iri: rdflib.term.Identifier) -> bool:
        if not isinstance(iri, rdflib.URIRef):
            return False
        if namespace_filter is None:
            return True
        return str(iri).startswith(namespace_filter)

    print("Building label map...")
    label_map: Dict[str, str] = {}
    for s, _, lbl in g.triples((None, RDFS.label, None)):
        if isinstance(s, rdflib.URIRef):
            label_map[str(s)] = str(lbl)

    node_cache: Dict[str, str] = {}
    relations_to_insert = []

    print("Collecting subclass (is_a) relations...")
    count = 0
    for child, _, parent in g.triples((None, RDFS.subClassOf, None)):
        if not (isinstance(child, rdflib.URIRef) and isinstance(parent, rdflib.URIRef)):
            continue
        if not in_namespace(child) or not in_namespace(parent):
            continue

        child_iri = str(child)
        parent_iri = str(parent)

        child_label = label_map.get(child_iri)
        parent_label = label_map.get(parent_iri)

        child_id = get_or_create_node_id(client, child_iri, child_label, cache=node_cache)
        parent_id = get_or_create_node_id(client, parent_iri, parent_label, cache=node_cache)

        relations_to_insert.append(
            {
                "subject_id": child_id,
                "object_id": parent_id,
                "predicate": "is_a",
                "source": "FoodOn",
            }
        )

        count += 1
        if count % 1000 == 0:
            print(f"Collected {count} relations...")

    print(f"Upserting {len(relations_to_insert)} relations into ontology_relations...")

    # Requires UNIQUE(subject_id, predicate, object_id, source) (created in 000_base_schema.sql)
    BATCH = 2000
    for i in range(0, len(relations_to_insert), BATCH):
        batch = relations_to_insert[i : i + BATCH]
        client.table("ontology_relations").upsert(
            batch,
            on_conflict="subject_id,predicate,object_id,source",
        ).execute()
        if (i // BATCH) % 5 == 0:
            print(f"Upserted {min(i + BATCH, len(relations_to_insert))}/{len(relations_to_insert)}")

    print("Done.")


if __name__ == "__main__":
    # Example:
    # python scripts/import_foodon_graph.py data/foodon.owl http://purl.obolibrary.org/obo/FOODON_
    args = sys.argv[1:]
    if not args:
        raise SystemExit("Usage: python scripts/import_foodon_graph.py <path_to_owl_or_ttl> [namespace_prefix]")
    path = args[0]
    ns = args[1] if len(args) > 1 else None
    import_foodon_graph(path, namespace_filter=ns)
