# scripts/patch_foodon_category_roots.py
# Path to hardcode the source in Ontology_Nodes table in Supabase DB
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.ontologies.build_ingredient_category_tags import (
    build_category_roots,
)
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("patch_foodon_category_roots")


def main() -> None:
    client = get_supabase_client()

    # 1) Get the canonical list of category roots (same as the tagging script uses)
    roots = build_category_roots()
    iris = list(roots.values())

    # 2) Show what currently exists in ontology_nodes for these IRIs
    res = (
        client.table("ontology_nodes")
        .select("id, iri, label, source, kind")
        .in_("iri", iris)
        .execute()
    )
    rows = res.data or []

    print("=== Before patch: ontology_nodes for FoodOn category roots ===")
    for r in rows:
        print(
            f"- id={r['id']} | iri={r['iri']} | label={r.get('label')} "
            f"| source={r.get('source')} | kind={r.get('kind')}"
        )

    # 3) Force source='FoodOn' and set a reasonable kind if missing
    update_payload = {"source": "FoodOn"}
    # you can also enforce a kind if you want:
    # update_payload["kind"] = update_payload.get("kind") or "class"

    upd = (
        client.table("ontology_nodes")
        .update(update_payload)
        .in_("iri", iris)
        .execute()
    )

    print("\nUpdated rows:", len(upd.data or []))

    # 4) Re-read to confirm
    res2 = (
        client.table("ontology_nodes")
        .select("id, iri, label, source, kind")
        .in_("iri", iris)
        .execute()
    )
    rows2 = res2.data or []

    print("\n=== After patch: ontology_nodes for FoodOn category roots ===")
    for r in rows2:
        print(
            f"- id={r['id']} | iri={r['iri']} | label={r.get('label')} "
            f"| source={r.get('source')} | kind={r.get('kind')}"
        )


if __name__ == "__main__":
    main()
