# foodon_import.py

from __future__ import annotations

from config import get_supabase_client
from ontologies import link_ingredients_via_foodon_synonyms


def main() -> None:
    client = get_supabase_client()
    # Adjust path if your TSV is somewhere else
    tsv_path = "data/foodon-synonyms.tsv"
    link_ingredients_via_foodon_synonyms(client, tsv_path)


if __name__ == "__main__":
    main()
