# link_ingredients_to_ontology.py
from src.meal_taxonomy.config import get_supabase_client
from ontologies.ontologies import link_all_ingredients

def main() -> None:
    client = get_supabase_client()
    link_all_ingredients(client)

if __name__ == "__main__":
    main()
