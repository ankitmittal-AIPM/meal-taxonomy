import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.ontologies.build_ingredient_category_tags import debug_show_auto_roots

def main():
    client = get_supabase_client()
    debug_show_auto_roots(client, min_descendants=20, limit=30)

if __name__ == "__main__":
    main()
