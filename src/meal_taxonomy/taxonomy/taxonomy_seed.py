from __future__ import annotations
"""
taxonomy_seed.py

Purpose:
    Seed the Supabase database with:
      - tag_types (diet, meal_type, cuisine_region, etc.)
      - initial tags (vegan, vegetarian, breakfast, South India, etc.)

Usage:
    python -m meal_taxonomy.taxonomy.taxonomy_seed

Output: Expected Output in CLI Run
    HttpsHTTP/1.1 200 OK on tag_types upserts → tag type already existed, updated or noop.
    HTTP/1.1 201 Created on some tag_types → new tag types were created.
    HTTP/1.1 200 OK on tags upserts → tag already existed, updated or noop.
    HTTP/1.1 201 Created on some tags → new tags were created.
    Remember to get above output the HTTP silence to be removed from Logging_utils.py
"""

from typing import Dict
from supabase import Client

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("taxonomy_seed")

MODULE_PURPOSE = "Seed initial tag_types and tags for the meal taxonomy."

# -----------------------------------------------------------------------------
# Tag Types (Category Roots)
# -----------------------------------------------------------------------------
# NOTE:
# - These names MUST stay in sync with:
#   a) nlp_tagging.TagCandidate.tag_type
#   b) rule-based taggers in RecipeNLP (diet, taste_profile, dish_type, etc.)
#   c) ontology-based taggers (ingredient_category)
#
# Think of each key here as a “category root” in your taxonomy.
# -----------------------------------------------------------------------------
# TO DO : Align Category Roots at all places - Both Hardcoded and NLP Based
TAG_TYPES: Dict[str, str] = {
    # Core roots
    "diet": "Dietary patterns and restrictions (vegan, vegetarian, Jain, gluten_free, etc.)",
    "cuisine_region": "Geographical region of cuisine (South India, North India, Punjabi, Tamil, etc.)",
    "cuisine_national": "National cuisines (Indian, Italian, Chinese, etc.)",
    "meal_type": "Meal context (breakfast, lunch, dinner, snack, tiffin, dessert, beverage)",
    "course": "Course within a meal (starter, main, side, dessert, drink, chutney)",
    "taste_profile": "Flavor profile (spicy, sweet, sour, tangy, umami, chatpata, bitter, etc.)",
    "time_bucket": "Prep/cook time buckets (under_15_min, under_30_min, under_60_min, over_60_min, slow_cook, etc.)",
    "difficulty": "Cooking difficulty (easy, medium, hard)",
    "technique": "Primary cooking techniques (fried, baked, steamed, grilled, tadka, pressure_cooking, etc.)",
    "equipment": "Key tools (pressure_cooker, oven, microwave, tawa, kadai, air_fryer, etc.)",
    "occasion": "Occasion/festival (Diwali, Eid, Holi, party, kids_lunchbox, etc.)",
    "ingredient_category": "High-level ingredient groups (vegetables, dairy, meat, grains, pulses, masala) "
                           "often derived from FoodOn ontology.",
    "allergen": "Allergens and intolerance-related tags (contains_nuts, dairy_free, gluten_free)",

    # Supporting roots (already used by NLP)
    "dish_type": "Shape/form of dish (curry/sabzi, dal, soup, salad, biryani, street_food, raita, chutney, etc.)",
    "nutrition_profile": "Nutritional patterns (high_protein, low_carb, high_fiber, diabetic_friendly, etc.)",
    "ingredient_quality": "Physical qualities / textures (crispy, crunchy, creamy, smooth, etc.)",
    "color": "Visual color cues (red_gravy, green_gravy, yellow_dal, brown_gravy, etc.)",
}

# -----------------------------------------------------------------------------
# Minimal, opinionated Indian-first seed tags
# -----------------------------------------------------------------------------
# Ontologies & datasets will add hundreds more; this file just “primes” the
# taxonomy so that:
#   - the UI has some tags to start with
#   - NLP-produced values already exist as tags
# -----------------------------------------------------------------------------
SEED_TAGS = [
    # -------------------------------------------------------------------------
    # Diet
    # -------------------------------------------------------------------------
    dict(
        tag_type="diet",
        value="vegan",
        label_en="Vegan",
        label_hi="शुद्ध वीगन",
        label_hinglish="Vegan",
    ),
    dict(
        tag_type="diet",
        value="vegetarian",
        label_en="Vegetarian",
        label_hi="शाकाहारी",
        label_hinglish="Veg",
    ),
    dict(
        tag_type="diet",
        value="eggetarian",
        label_en="Eggetarian",
        label_hi="अंडा शाकाहारी",
        label_hinglish="Eggetarian",
    ),
    dict(
        tag_type="diet",
        value="non_veg",
        label_en="Non‑Vegetarian",
        label_hi="मांसाहारी",
        label_hinglish="Non‑veg",
    ),
    dict(
        tag_type="diet",
        value="jain",
        label_en="Jain",
        label_hi="जैन भोजन",
        label_hinglish="Jain",
    ),
    dict(
        tag_type="diet",
        value="no_onion_garlic",
        label_en="No Onion / No Garlic",
        label_hi="बिना प्याज़ लहसुन",
        label_hinglish="No onion‑garlic",
    ),
    dict(
        tag_type="diet",
        value="gluten_free",
        label_en="Gluten Free",
        label_hi="ग्लूटेन मुक्त",
        label_hinglish="Gluten free",
    ),
    dict(
        tag_type="diet",
        value="keto",
        label_en="Keto / Low Carb",
        label_hi="कीटो / कम कार्ब",
        label_hinglish="Keto / Low carb",
    ),

    # -------------------------------------------------------------------------
    # Meal types (Netflix-like contexts)
    # -------------------------------------------------------------------------
    dict(
        tag_type="meal_type",
        value="breakfast",
        label_en="Breakfast",
        label_hi="नाश्ता",
        label_hinglish="Breakfast",
    ),
    dict(
        tag_type="meal_type",
        value="lunch",
        label_en="Lunch",
        label_hi="दोपहर का भोजन",
        label_hinglish="Lunch",
    ),
    dict(
        tag_type="meal_type",
        value="dinner",
        label_en="Dinner",
        label_hi="रात का खाना",
        label_hinglish="Dinner",
    ),
    dict(
        tag_type="meal_type",
        value="snack",
        label_en="Snack",
        label_hi="स्नैक / हल्का नाश्ता",
        label_hinglish="Snack",
    ),
    dict(
        tag_type="meal_type",
        value="tiffin",
        label_en="Tiffin / Lunchbox",
        label_hi="टिफ़िन / लंचबॉक्स",
        label_hinglish="Tiffin",
    ),
    dict(
        tag_type="meal_type",
        value="tea_time",
        label_en="Tea-time snack",
        label_hi="चाय के साथ स्नैक",
        label_hinglish="Chai time snack",
    ),

    # -------------------------------------------------------------------------
    # Cuisine region (India-heavy)
    # -------------------------------------------------------------------------
    dict(
        tag_type="cuisine_region",
        value="north_india",
        label_en="North Indian",
        label_hi="उत्तरी भारतीय",
        label_hinglish="North Indian",
    ),
    dict(
        tag_type="cuisine_region",
        value="south_india",
        label_en="South Indian",
        label_hi="दक्षिण भारतीय",
        label_hinglish="South Indian",
    ),
    dict(
        tag_type="cuisine_region",
        value="punjabi",
        label_en="Punjabi",
        label_hi="पंजाबी",
        label_hinglish="Punjabi",
    ),
    dict(
        tag_type="cuisine_region",
        value="gujarati",
        label_en="Gujarati",
        label_hi="गुजराती",
        label_hinglish="Gujarati",
    ),
    dict(
        tag_type="cuisine_region",
        value="bengali",
        label_en="Bengali",
        label_hi="बंगाली",
        label_hinglish="Bengali",
    ),
    dict(
        tag_type="cuisine_region",
        value="maharashtrian",
        label_en="Maharashtrian",
        label_hi="मराठी",
        label_hinglish="Maharashtrian",
    ),
    dict(
        tag_type="cuisine_region",
        value="hyderabadi",
        label_en="Hyderabadi",
        label_hi="हैदराबादी",
        label_hinglish="Hyderabadi",
    ),

    # -------------------------------------------------------------------------
    # National cuisines
    # -------------------------------------------------------------------------
    dict(
        tag_type="cuisine_national",
        value="indian",
        label_en="Indian",
        label_hi="भारतीय",
        label_hinglish="Indian",
    ),
    dict(
        tag_type="cuisine_national",
        value="indo_chinese",
        label_en="Indo‑Chinese",
        label_hi="इंडो‑चाइनीज़",
        label_hinglish="Indo‑Chinese",
    ),
    dict(
        tag_type="cuisine_national",
        value="italian",
        label_en="Italian",
        label_hi="इटैलियन",
        label_hinglish="Italian",
    ),
    dict(
        tag_type="cuisine_national",
        value="chinese",
        label_en="Chinese",
        label_hi="चाइनीज़",
        label_hinglish="Chinese",
    ),

    # -------------------------------------------------------------------------
    # Course
    # -------------------------------------------------------------------------
    dict(
        tag_type="course",
        value="starter",
        label_en="Starter / Appetiser",
        label_hi="स्टार्टर",
        label_hinglish="Starter",
    ),
    dict(
        tag_type="course",
        value="main",
        label_en="Main course",
        label_hi="मुख्य भोजन",
        label_hinglish="Main course",
    ),
    dict(
        tag_type="course",
        value="side",
        label_en="Side dish / Sabzi",
        label_hi="सब्ज़ी / साइड",
        label_hinglish="Side / Sabzi",
    ),
    dict(
        tag_type="course",
        value="dessert",
        label_en="Dessert / Mithai",
        label_hi="मिठाई",
        label_hinglish="Dessert",
    ),
    dict(
        tag_type="course",
        value="drink",
        label_en="Drink / Beverage",
        label_hi="पेय",
        label_hinglish="Drink",
    ),
    dict(
        tag_type="course",
        value="chutney",
        label_en="Chutney / Dip",
        label_hi="चटनी",
        label_hinglish="Chutney",
    ),

    # -------------------------------------------------------------------------
    # Taste profile (aligned to TASTE_KEYWORDS in nlp_tagging.py)
    # -------------------------------------------------------------------------
    dict(
        tag_type="taste_profile",
        value="spicy",
        label_en="Spicy",
        label_hi="मसालेदार",
        label_hinglish="Teekha",
    ),
    dict(
        tag_type="taste_profile",
        value="sweet",
        label_en="Sweet",
        label_hi="मीठा",
        label_hinglish="Meetha",
    ),
    dict(
        tag_type="taste_profile",
        value="tangy",
        label_en="Tangy / Chatpata",
        label_hi="खट्टा‑मीठा / चटपटा",
        label_hinglish="Chatpata",
    ),
    dict(
        tag_type="taste_profile",
        value="savory",
        label_en="Savory / Umami",
        label_hi="नमकीन / उमामी",
        label_hinglish="Savory",
    ),

    # -------------------------------------------------------------------------
    # Time buckets (aligned to RecipeNLP.bucket_time)
    # -------------------------------------------------------------------------
    dict(
        tag_type="time_bucket",
        value="under_15_min",
        label_en="Under 15 minutes",
        label_hi="15 मिनट से कम",
        label_hinglish="<15 mins",
    ),
    dict(
        tag_type="time_bucket",
        value="under_30_min",
        label_en="Under 30 minutes",
        label_hi="30 मिनट से कम",
        label_hinglish="<30 mins",
    ),
    dict(
        tag_type="time_bucket",
        value="under_60_min",
        label_en="Under 60 minutes",
        label_hi="60 मिनट से कम",
        label_hinglish="<60 mins",
    ),
    dict(
        tag_type="time_bucket",
        value="over_60_min",
        label_en="Over 60 minutes",
        label_hi="60 मिनट से अधिक",
        label_hinglish=">60 mins",
    ),

    # -------------------------------------------------------------------------
    # Difficulty
    # -------------------------------------------------------------------------
    dict(
        tag_type="difficulty",
        value="easy",
        label_en="Easy",
        label_hi="आसान",
        label_hinglish="Easy",
    ),
    dict(
        tag_type="difficulty",
        value="medium",
        label_en="Medium",
        label_hi="मध्यम",
        label_hinglish="Medium",
    ),
    dict(
        tag_type="difficulty",
        value="hard",
        label_en="Hard",
        label_hi="कठिन",
        label_hinglish="Hard",
    ),

    # -------------------------------------------------------------------------
    # Technique (aligned with TECHNIQUE_KEYWORDS)
    # -------------------------------------------------------------------------
    dict(
        tag_type="technique",
        value="fried",
        label_en="Fried / Stir-fried",
        label_hi="तला हुआ",
        label_hinglish="Fried",
    ),
    dict(
        tag_type="technique",
        value="baked",
        label_en="Baked",
        label_hi="बेक्ड",
        label_hinglish="Baked",
    ),
    dict(
        tag_type="technique",
        value="steamed",
        label_en="Steamed",
        label_hi="स्टीम्ड",
        label_hinglish="Steamed",
    ),
    dict(
        tag_type="technique",
        value="grilled",
        label_en="Grilled / Tandoori",
        label_hi="तंदूरी / ग्रिल्ड",
        label_hinglish="Grilled / Tandoori",
    ),
    dict(
        tag_type="technique",
        value="pressure_cooked",
        label_en="Pressure Cooked",
        label_hi="प्रेशर कुकर में पका",
        label_hinglish="Pressure cooked",
    ),

    # -------------------------------------------------------------------------
    # Equipment
    # -------------------------------------------------------------------------
    dict(
        tag_type="equipment",
        value="pressure_cooker",
        label_en="Pressure cooker",
        label_hi="प्रेशर कुकर",
        label_hinglish="Pressure cooker",
    ),
    dict(
        tag_type="equipment",
        value="tawa",
        label_en="Tawa / Griddle",
        label_hi="तवा",
        label_hinglish="Tawa",
    ),
    dict(
        tag_type="equipment",
        value="kadai",
        label_en="Kadai / Wok",
        label_hi="कड़ाही",
        label_hinglish="Kadhai",
    ),
    dict(
        tag_type="equipment",
        value="oven",
        label_en="Oven",
        label_hi="ओवन",
        label_hinglish="Oven",
    ),
    dict(
        tag_type="equipment",
        value="air_fryer",
        label_en="Air fryer",
        label_hi="एयर फ्रायर",
        label_hinglish="Air fryer",
    ),

    # -------------------------------------------------------------------------
    # Occasion
    # -------------------------------------------------------------------------
    dict(
        tag_type="occasion",
        value="everyday",
        label_en="Everyday home meal",
        label_hi="रोज़मर्रा का खाना",
        label_hinglish="Daily ghar ka khana",
    ),
    dict(
        tag_type="occasion",
        value="kids_lunchbox",
        label_en="Kids lunchbox",
        label_hi="बच्चों का टिफ़िन",
        label_hinglish="Kids tiffin",
    ),
    dict(
        tag_type="occasion",
        value="party",
        label_en="Party / Get-together",
        label_hi="पार्टी",
        label_hinglish="Party",
    ),
    dict(
        tag_type="occasion",
        value="diwali",
        label_en="Diwali",
        label_hi="दीवाली",
        label_hinglish="Diwali",
    ),
    dict(
        tag_type="occasion",
        value="eid",
        label_en="Eid",
        label_hi="ईद",
        label_hinglish="Eid",
    ),
    dict(
        tag_type="occasion",
        value="holi",
        label_en="Holi",
        label_hi="होली",
        label_hinglish="Holi",
    ),

    # -------------------------------------------------------------------------
    # Ingredient category (these overlap with FoodOn-driven categories but
    # having seeds helps the UI even before ontology is wired).
    # -------------------------------------------------------------------------
    dict(
        tag_type="ingredient_category",
        value="vegetable",
        label_en="Vegetable",
        label_hi="सब्ज़ी",
        label_hinglish="Sabzi / Veg",
    ),
    dict(
        tag_type="ingredient_category",
        value="fruit",
        label_en="Fruit",
        label_hi="फल",
        label_hinglish="Fruit",
    ),
    dict(
        tag_type="ingredient_category",
        value="dairy",
        label_en="Dairy",
        label_hi="डेयरी",
        label_hinglish="Dairy",
    ),
    dict(
        tag_type="ingredient_category",
        value="legume",
        label_en="Legume / Dal",
        label_hi="दाल / फलियाँ",
        label_hinglish="Dal / Legume",
    ),
    dict(
        tag_type="ingredient_category",
        value="cereal_grain",
        label_en="Cereal / Grain",
        label_hi="अनाज",
        label_hinglish="Grain",
    ),
    dict(
        tag_type="ingredient_category",
        value="spice",
        label_en="Spice / Masala",
        label_hi="मसाला",
        label_hinglish="Masala",
    ),

    # -------------------------------------------------------------------------
    # Allergen
    # -------------------------------------------------------------------------
    dict(
        tag_type="allergen",
        value="contains_nuts",
        label_en="Contains nuts",
        label_hi="मेवे शामिल हैं",
        label_hinglish="Contains nuts",
    ),
    dict(
        tag_type="allergen",
        value="contains_dairy",
        label_en="Contains dairy",
        label_hi="डेयरी शामिल है",
        label_hinglish="Contains dairy",
    ),
    dict(
        tag_type="allergen",
        value="contains_egg",
        label_en="Contains egg",
        label_hi="अंडा शामिल है",
        label_hinglish="Contains egg",
    ),
    dict(
        tag_type="allergen",
        value="contains_gluten",
        label_en="Contains gluten",
        label_hi="ग्लूटेन शामिल है",
        label_hinglish="Contains gluten",
    ),

    # -------------------------------------------------------------------------
    # Dish type (supporting root; aligned to DISH_TYPE_KEYWORDS)
    # -------------------------------------------------------------------------
    dict(
        tag_type="dish_type",
        value="curry",
        label_en="Curry / Sabzi",
        label_hi="करी / सब्ज़ी",
        label_hinglish="Curry / Sabzi",
    ),
    dict(
        tag_type="dish_type",
        value="dal",
        label_en="Dal",
        label_hi="दाल",
        label_hinglish="Dal",
    ),
    dict(
        tag_type="dish_type",
        value="rice_dish",
        label_en="Rice dish (Biryani, Pulao)",
        label_hi="चावल की डिश (बिरयानी, पुलाव)",
        label_hinglish="Rice dish",
    ),
    dict(
        tag_type="dish_type",
        value="bread",
        label_en="Bread / Flatbread",
        label_hi="रोटी / ब्रेड",
        label_hinglish="Roti / Bread",
    ),
    dict(
        tag_type="dish_type",
        value="snack",
        label_en="Snack / Starter",
        label_hi="स्नैक / स्टार्टर",
        label_hinglish="Snack / Starter",
    ),
    dict(
        tag_type="dish_type",
        value="soup",
        label_en="Soup / Shorba",
        label_hi="सूप / शोरबा",
        label_hinglish="Soup",
    ),
    dict(
        tag_type="dish_type",
        value="salad",
        label_en="Salad",
        label_hi="सलाद",
        label_hinglish="Salad",
    ),
    dict(
        tag_type="dish_type",
        value="chaat",
        label_en="Chaat",
        label_hi="चाट",
        label_hinglish="Chaat",
    ),

    # -------------------------------------------------------------------------
    # Nutrition profile (supporting root; aligned to NUTRITION_KEYWORDS)
    # -------------------------------------------------------------------------
    dict(
        tag_type="nutrition_profile",
        value="high_protein",
        label_en="High protein",
        label_hi="उच्च प्रोटीन",
        label_hinglish="High protein",
    ),
    dict(
        tag_type="nutrition_profile",
        value="low_carb",
        label_en="Low carb / Keto",
        label_hi="कम कार्ब / कीटो",
        label_hinglish="Low carb",
    ),
    dict(
        tag_type="nutrition_profile",
        value="high_fiber",
        label_en="High fibre",
        label_hi="उच्च फाइबर",
        label_hinglish="High fibre",
    ),

    # -------------------------------------------------------------------------
    # Ingredient quality (supporting root; aligned to PHYSICAL_QUALITY from NER)
    # -------------------------------------------------------------------------
    dict(
        tag_type="ingredient_quality",
        value="crispy",
        label_en="Crispy",
        label_hi="कुरकुरा",
        label_hinglish="Crispy",
    ),
    dict(
        tag_type="ingredient_quality",
        value="crunchy",
        label_en="Crunchy",
        label_hi="क्रंची",
        label_hinglish="Crunchy",
    ),
    dict(
        tag_type="ingredient_quality",
        value="creamy",
        label_en="Creamy",
        label_hi="क्रीमी",
        label_hinglish="Creamy",
    ),

    # -------------------------------------------------------------------------
    # Color (supporting root; aligned to COLOR from NER)
    # -------------------------------------------------------------------------
    dict(
        tag_type="color",
        value="red",
        label_en="Red gravy",
        label_hi="लाल ग्रेवी",
        label_hinglish="Red gravy",
    ),
    dict(
        tag_type="color",
        value="yellow",
        label_en="Yellow / Haldi rich",
        label_hi="पीली ग्रेवी",
        label_hinglish="Yellow",
    ),
    dict(
        tag_type="color",
        value="green",
        label_en="Green / Palak / Hari chutney style",
        label_hi="हरी ग्रेवी",
        label_hinglish="Green",
    ),
]

# -----------------------------------------------------------------------------
# Helpers to upsert tag types and tags
# -----------------------------------------------------------------------------
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
    """
    Seed all tag_types and initial tags for the meal taxonomy.
    Safe to run multiple times (upsert-based).
    """
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
