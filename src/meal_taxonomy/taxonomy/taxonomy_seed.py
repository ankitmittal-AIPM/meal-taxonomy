"""
taxonomy_seed.py

Purpose:
    Seed the Supabase database with:
      - tag_types (diet, meal_type, cuisine_region, etc.)
      - initial tags (vegan, vegetarian, breakfast, South India, etc.)

Usage:
    python -m meal_taxonomy.taxonomy.taxonomy_seed
"""

from __future__ import annotations

from typing import Dict
from supabase import Client

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.logging_utils import log_info, log_error

MODULE_PURPOSE = "Seed initial tag_types and tags for the meal taxonomy."

TAG_TYPES: Dict[str, str] = {
    "diet": "Dietary patterns and restrictions (vegan, vegetarian, Jain, gluten_free, etc.)",
    "cuisine_region": "Geographical region of cuisine (South India, North India, Punjabi, Tamil, etc.)",
    "cuisine_national": "National cuisines (Indian, Italian, Chinese, etc.)",
    "meal_type": "Meal context (breakfast, lunch, dinner, snack, dessert, beverage)",
    "course": "Course within a meal (starter, main, side, dessert, drink)",
    "taste_profile": "Flavor profile (spicy, sweet, sour, umami, tangy, bitter, etc.)",
    "time_bucket": "Prep/cook time buckets (under_15_min, under_30_min, under_60_min, slow_cook, etc.)",
    "difficulty": "Cooking difficulty (easy, medium, hard)",
    "technique": "Primary cooking techniques (frying, baking, steaming, tadka, pressure_cooking, etc.)",
    "equipment": "Key tools (pressure_cooker, oven, microwave, tawa, kadai, air_fryer, etc.)",
    "occasion": "Occasion/festival (Diwali, Eid, Holi, party, kids_lunchbox, etc.)",
    "ingredient_category": "High-level ingredient groups (vegetables, dairy, meat, grains, pulses, masala)",
    "allergen": "Allergens and intolerance-related tags (contains_nuts, dairy_free, gluten_free)",
}

# Minimal seed tags to illustrate; ontologies & datasets will add more.
SEED_TAGS = [
    # Diet
    dict(tag_type="diet", value="vegan",
         label_en="Vegan", label_hi="शाकाहारी (वीगन)", label_hinglish="Vegan"),
    dict(tag_type="diet", value="vegetarian",
         label_en="Vegetarian", label_hi="शाकाहारी", label_hinglish="Veg"),

    # Meal types
    dict(tag_type="meal_type", value="breakfast",
         label_en="Breakfast", label_hi="नाश्ता", label_hinglish="Breakfast"),
    dict(tag_type="meal_type", value="lunch",
         label_en="Lunch", label_hi="दोपहर का भोजन", label_hinglish="Lunch"),
    dict(tag_type="meal_type", value="dinner",
         label_en="Dinner", label_hi="रात का खाना", label_hinglish="Dinner"),
    dict(tag_type="meal_type", value="snack",
         label_en="Snack", label_hi="नाश्ता / स्नैक", label_hinglish="Snack"),

    # Region
    dict(tag_type="cuisine_region", value="south_india",
         label_en="South Indian", label_hi="दक्षिण भारतीय", label_hinglish="South Indian"),
    dict(tag_type="cuisine_region", value="north_india",
         label_en="North Indian", label_hi="उत्तरी भारतीय", label_hinglish="North Indian"),

    # Time buckets
    dict(tag_type="time_bucket", value="under_15_min",
         label_en="Under 15 minutes", label_hi="15 मिनट से कम", label_hinglish="<15 mins"),
    dict(tag_type="time_bucket", value="under_30_min",
         label_en="Under 30 minutes", label_hi="30 मिनट से कम", label_hinglish="<30 mins"),

    # Taste
    dict(tag_type="taste_profile", value="spicy",
         label_en="Spicy", label_hi="मसालेदार", label_hinglish="Teekha"),
    dict(tag_type="taste_profile", value="sweet",
         label_en="Sweet", label_hi="मीठा", label_hinglish="Meetha"),
]


def ensure_tag_type(client: Client, name: str, description: str) -> int:
    """
    Upsert a tag_type and return its id.
    Compatible with supabase Python client v2.
    """
    # Try upsert; by default it returns the row representation
    res = client.table("tag_types").upsert(
        {"name": name, "description": description},
        on_conflict="name",
    ).execute()

    if res.data:
        # row contains id, name, description, created_at
        return res.data[0]["id"]

    # Fallback: fetch by name (in case returning is disabled)
    res = client.table("tag_types").select("id").eq("name", name).execute()
    return res.data[0]["id"]


def ensure_tag(
    client: Client,
    *,
    tag_type_id: int,
    value: str,
    label_en: str,
    label_hi: str | None = None,
    label_hinglish: str | None = None,
) -> str:
    """
    Upsert a tag and return its id.
    Compatible with supabase Python client v2.
    """
    payload = {
        "tag_type_id": tag_type_id,
        "value": value,
        "label_en": label_en,
        "label_hi": label_hi,
        "label_hinglish": label_hinglish,
    }

    res = client.table("tags").upsert(
        payload,
        on_conflict="tag_type_id,value",
    ).execute()

    if res.data:
        return res.data[0]["id"]

    # Fallback: fetch existing row if upsert didn’t return data
    res = (
        client.table("tags")
        .select("id")
        .eq("tag_type_id", tag_type_id)
        .eq("value", value)
        .execute()
    )
    return res.data[0]["id"]


def seed_core_taxonomy() -> None:
    client = get_supabase_client()

    tag_type_ids: dict[str, int] = {}
    for name, desc in TAG_TYPES.items():
        tag_type_ids[name] = ensure_tag_type(client, name, desc)

    for tag in SEED_TAGS:
        tt_id = tag_type_ids[tag["tag_type"]]
        ensure_tag(
            client,
            tag_type_id=tt_id,
            value=tag["value"],
            label_en=tag["label_en"],
            label_hi=tag.get("label_hi"),
            label_hinglish=tag.get("label_hinglish"),
        )


if __name__ == "__main__":
    seed_core_taxonomy()
    logger.info(
        "Core taxonomy seeded",
        extra={
            "invoking_func": "__main__",
            "invoking_purpose": "Seed core taxonomy tags and tag types",
            "next_step": "Exit",
            "resolution": "",
        },
    )
