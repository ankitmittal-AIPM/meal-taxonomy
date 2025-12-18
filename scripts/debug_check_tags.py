from __future__ import annotations
"""
Purpose:
    Simple script to debug/check existing tags and tag_types in the Supabase DB.

Usage:
    python scripts/debug_check_tags.py post execution of taxonomy_seed.py

Output: Expected Output in CLI Run
    Tag types:
    - 1: diet - Dietary patterns and restrictions (vegan, vegetarian, Jain, gluten_free, etc.)
    - 2: meal_type - Meal context (breakfast, lunch, dinner, snack, tiffin, dessert, beverage)
        etc.
    Diet tags:
    - 1: vegan
    - 2: vegetarian
    - 3: eggetarian
        etc.
    Meal type tags:
    - 10: beverage
    - 11: breakfast
    - 12: dinner
        etc.

"""
import sys
from pathlib import Path
# --- Make project root importable so `src.*` imports work even when this
# --- script is executed from the `scripts/` directory.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.meal_taxonomy.config import get_supabase_client

def main():
    client = get_supabase_client()

    # 1) Check tag_types
    tt = client.table("tag_types").select("id,name,description").order("name").execute()
    print("Tag types:")
    for row in tt.data:
        print(f"- {row['id']}: {row['name']} - {row.get('description', '')}")

    # 2) Check some tags for a couple of key tag_types
    diet_tags = (
        client.table("tags")
        .select("id,value,tag_types!inner(name)")
        .eq("tag_types.name", "diet")
        .order("value")
        .execute()
    )

    print("\nDiet tags:")
    for row in diet_tags.data:
        print(f"- {row['id']}: {row['value']}")

    meal_type_tags = (
        client.table("tags")
        .select("id,value,tag_types!inner(name)")
        .eq("tag_types.name", "meal_type")
        .order("value")
        .execute()
    )

    print("\nMeal type tags:")
    for row in meal_type_tags.data:
        print(f"- {row['id']}: {row['value']}")

if __name__ == "__main__":
    main()
