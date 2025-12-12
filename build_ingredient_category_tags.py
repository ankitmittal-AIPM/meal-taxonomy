# build_ingredient_category_tags.py

from __future__ import annotations

from typing import Dict, Set

from supabase import Client

from config import get_supabase_client
from taxonomy_seed import ensure_tag_type, ensure_tag
from ontologies import FOODON_INGREDIENT_MAPPING, normalize_ingredient_name


def ensure_ingredient_category_tags(client: Client) -> Dict[str, str]:
    """
    Ensure tag_type 'ingredient_category' exists and create one tag
    per distinct category_value in FOODON_INGREDIENT_MAPPING.

    Returns:
        dict mapping category_value -> tag_id
    """
    # 1) Ensure tag_type
    tag_type_id = ensure_tag_type(
        client,
        name="ingredient_category",
        description="High-level ingredient categories (legume, dairy, grain, etc.)",
    )

    # 2) Collect all distinct (value, label) pairs from the mapping
    categories: Dict[str, str] = {}
    for mapping in FOODON_INGREDIENT_MAPPING.values():
        value = mapping.category_value.strip().lower()
        label = mapping.category_label.strip()
        if not value:
            continue
        categories[value] = label

    # 3) Ensure a tag for each category
    value_to_tag_id: Dict[str, str] = {}
    for value, label in categories.items():
        tag_id = ensure_tag(
            client,
            tag_type_id=tag_type_id,
            value=value,
            label_en=label,
            label_hi=None,
            label_hinglish=None,
        )
        value_to_tag_id[value] = tag_id

    print(f"[ingredient_category] Ensured {len(value_to_tag_id)} category tags.")
    return value_to_tag_id


def attach_ingredient_category_tags(client: Client, value_to_tag_id: Dict[str, str]) -> None:
    """
    For each meal, look at its ingredients, map them to ontology categories,
    and attach ingredient_category tags to that meal via meal_tags (source='ontology').
    """
    # 1) Build ingredient_id -> category_value mapping from DB + FOODON_INGREDIENT_MAPPING
    res_ing = client.table("ingredients").select("id, name_en").execute()
    ingredients = res_ing.data or []

    ingredient_id_to_category: Dict[str, str] = {}

    for row in ingredients:
        ing_id = row["id"]
        name = row.get("name_en") or ""
        norm = normalize_ingredient_name(name)
        mapping = FOODON_INGREDIENT_MAPPING.get(norm)
        if not mapping:
            continue

        cat_value = mapping.category_value.strip().lower()
        if not cat_value:
            continue

        if cat_value not in value_to_tag_id:
            # Category not turned into a tag for some reason; skip
            continue

        ingredient_id_to_category[ing_id] = cat_value

    if not ingredient_id_to_category:
        print("[ingredient_category] No ingredients had ontology categories mapped.")
        return

    # 2) Fetch meal_ingredients to know which meals use which ingredients
    res_mi = client.table("meal_ingredients").select("meal_id, ingredient_id").execute()
    meal_ingredients = res_mi.data or []

    # 3) Build meal_id -> set(category_value)
    meal_to_categories: Dict[str, Set[str]] = {}

    for row in meal_ingredients:
        meal_id = row["meal_id"]
        ing_id = row["ingredient_id"]

        cat_value = ingredient_id_to_category.get(ing_id)
        if not cat_value:
            continue

        if meal_id not in meal_to_categories:
            meal_to_categories[meal_id] = set()
        meal_to_categories[meal_id].add(cat_value)

    if not meal_to_categories:
        print("[ingredient_category] No meals found that use mapped ingredients.")
        return

    # 4) Prepare meal_tags rows to upsert
    rows_to_upsert = []
    for meal_id, cat_values in meal_to_categories.items():
        for cat_value in cat_values:
            tag_id = value_to_tag_id.get(cat_value)
            if not tag_id:
                continue

            rows_to_upsert.append(
                {
                    "meal_id": meal_id,
                    "tag_id": tag_id,
                    "confidence": 0.9,       # ontology-based, pretty strong
                    "is_primary": False,
                    "source": "ontology",
                }
            )

    if not rows_to_upsert:
        print("[ingredient_category] No category tags to attach.")
        return

    # Optional: chunk to avoid too-large payloads (here simple dataset so one shot is fine)
    client.table("meal_tags").upsert(rows_to_upsert).execute()
    print(f"[ingredient_category] Attached {len(rows_to_upsert)} ingredient_category tags to meals.")


def main() -> None:
    client = get_supabase_client()

    # 1) Make sure ingredient_category tags exist
    value_to_tag_id = ensure_ingredient_category_tags(client)

    # 2) Attach them to meals using ingredients + ontology mapping
    attach_ingredient_category_tags(client, value_to_tag_id)


if __name__ == "__main__":
    main()
