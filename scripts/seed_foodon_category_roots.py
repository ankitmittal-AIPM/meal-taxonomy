import sys
from pathlib import Path
# --- Make project root importable so `src.*` imports work even when this
# --- script is executed from the `scripts/` directory.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.ontologies.build_ingredient_category_tags import build_category_roots


def main() -> None:
    client = get_supabase_client()
    roots = build_category_roots()  # category_value -> root_iri

    print("Seeding ontology_nodes for FoodOn category roots...")
    for cat, iri in roots.items():
        # 1) Check if node already exists
        existing = (
            client.table("ontology_nodes")
            .select("id, iri, label, source, kind")
            .eq("iri", iri)
            .eq("source", "FoodOn")
            .limit(1)
            .execute()
        )

        if existing.data:
            row = existing.data[0]
            print(f"[exists] {cat}: {iri} -> id={row['id']}")
            # Optional: update label/kind if you want them normalized
            # client.table("ontology_nodes").update(
            #     {"label": f"{cat} (category root)", "kind": "ingredient_class"}
            # ).eq("id", row["id"]).execute()
            continue

        # 2) Insert new row if not found
        payload = {
            "iri": iri,
            "source": "FoodOn",
            "label": f"{cat} (category root)",
            "kind": "ingredient_class",  # adjust if your schema uses a different enum/text
        }
        inserted = client.table("ontology_nodes").insert(payload).execute()
        if inserted.data:
            new_id = inserted.data[0]["id"]
            print(f"[inserted] {cat}: {iri} -> id={new_id}")
        else:
            print(f"[warn] insert returned no data for {cat}: {iri}")


if __name__ == "__main__":
    main()