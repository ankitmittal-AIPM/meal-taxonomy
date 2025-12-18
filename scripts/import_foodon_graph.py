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
    - Inserts "is_a" relations into ontology_relations.

Logging:
    Structured logs per company logging standard

Usage:
    python import_foodon_graph.py --file data/foodon.owl --namespace http://purl.obolibrary.org/obo/FOODON_

"""

import os
import argparse
from typing import Dict
import dotenv
dotenv.load_dotenv()

import rdflib
from rdflib.namespace import RDFS, OWL

from supabase import create_client  # or import your own helper if you have one

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # service key recommended

client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_or_create_node_id(
    iri: str,
    label: str | None,
    kind: str = "class",
    source: str = "FoodOn",
    cache: Dict[str, int] | None = None,
) -> int:
    """
    Ensure a row exists in ontology_nodes for this IRI and return its id.
    Uses a local cache to avoid repeated DB hits.
    """
    if cache is not None and iri in cache:
        return cache[iri]

    # Try to fetch existing node
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
        node_id = data[0]["id"]
    else:
        # Insert new node
        insert_payload = {
            "iri": iri,
            "label": label or iri,
            "kind": kind,
            "source": source,
        }
        ins = client.table("ontology_nodes").insert(insert_payload).execute()
        node_id = ins.data[0]["id"]

    if cache is not None:
        cache[iri] = node_id
    return node_id


def import_foodon_graph(ontology_path: str, namespace_filter: str | None = None):
    """
    Parse FoodOn OWL/TTL and import subclass edges as is_a relations
    into ontology_relations.
    """
    print(f"Loading FoodOn ontology from {ontology_path}")
    g = rdflib.Graph()
    g.parse(ontology_path)

    print(f"Graph loaded: {len(g)} RDF triples")

    # Optionally limit to IRIs in a specific namespace (e.g., FoodOn)
    def in_namespace(iri: rdflib.term.Identifier) -> bool:
        if not isinstance(iri, rdflib.URIRef):
            return False
        if namespace_filter is None:
            return True
        return str(iri).startswith(namespace_filter)

    # Preload labels for nicer node labels
    print("Building label map...")
    label_map: Dict[str, str] = {}
    for s, _, label in g.triples((None, RDFS.label, None)):
        if isinstance(s, rdflib.URIRef):
            label_map[str(s)] = str(label)

    node_cache: Dict[str, int] = {}
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

        child_id = get_or_create_node_id(child_iri, child_label, cache=node_cache)
        parent_id = get_or_create_node_id(parent_iri, parent_label, cache=node_cache)

        # Currently only handling direct subclass relations; ignore complex expressions.
        # TO DO: handle OWL restrictions, intersections, unions, etc. if needed. Include more type of relations other than is_a.
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
            print(f"Collected {count} relations so far...")

        # Limit relational insert for testing; remove or increase as needed
        if count >= 10000:  
            break   

    print(f"Total is_a relations collected: {len(relations_to_insert)}")

    # Insert in batches to avoid huge single-payload
    BATCH_SIZE = 1000
    for i in range(0, len(relations_to_insert), BATCH_SIZE):
        batch = relations_to_insert[i : i + BATCH_SIZE]
        print(f"Inserting relations {i}–{i+len(batch)-1}...")
        try:
            db_response = client.table("ontology_relations").upsert(batch).execute()
            print(f"Inserted {len(db_response.data)} relations.")
        except Exception as e:
            print(f"Error inserting batch starting at {i}: {e}")
            continue

    print("FoodOn graph import completed.")

# name
def main():
    parser = argparse.ArgumentParser(description="Import FoodOn graph into Supabase.")
    parser.add_argument(
        "--file",
        required=True,
        help="Path to FoodOn OWL/TTL file (e.g. data/foodon.owl)",
    )
    parser.add_argument(
        "--namespace",
        default=None,
        help="Optional IRI prefix to restrict to FoodOn classes only",
    )
    args = parser.parse_args()

    import_foodon_graph(args.file, namespace_filter=args.namespace)


if __name__ == "__main__":
    main()
