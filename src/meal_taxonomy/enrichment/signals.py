# src/meal_taxonomy/enrichment/signals.py
from __future__ import annotations

"""
signals.py

Purpose:
    Layer-0 (deterministic) enrichment for Indian meals.

This module intentionally does NOT require any ML assets.
It provides:
  - keyword/regex based inference for:
      * meal_type (breakfast/lunch/dinner/snack/tiffin/dessert)
      * cuisine_region hierarchy (South Indian -> Karnataka -> Udupi, etc.)
      * diet (vegetarian/non_vegetarian/eggetarian/vegan/jain/no_onion_garlic)
      * technique (tawa_roasted, tempering, etc.) where missing from NLP
      * equipment (pressure_cooker, kadai, tawa, idli_stand, oven, mixer, ...)
      * spice_level (1-5) + kids_friendly heuristic
      * nutrition_profile + occasion hints (festive, satvik, diabetic_friendly)
    Layer-1 ML and Layer-2 LLM can override/augment these.

Design rules:
  - Keep tags low-cardinality and reusable.
  - Use the existing taxonomy tag types from taxonomy_seed.py where possible.
"""

import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from src.meal_taxonomy.nlp_tagging import TagCandidate


# ---------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------
def _cand(
    tag_type: str,
    value: str,
    label_en: Optional[str] = None,
    confidence: float = 0.75,
    is_primary: bool = False,
    source: str = "layer0_rule",
) -> TagCandidate:
    return TagCandidate(
        tag_type=tag_type,
        value=value,
        label_en=label_en or value,
        confidence=confidence,
        is_primary=is_primary,
        source=source,
    )


def _contains_any(text_l: str, keywords: Sequence[str]) -> bool:
    return any(k in text_l for k in keywords)


# ---------------------------------------------------------------------
# Meal-type (breakfast/lunch/dinner/snack/tiffin/dessert)
# ---------------------------------------------------------------------
MEAL_TYPE_KEYWORDS: Dict[str, List[str]] = {
    "breakfast": [
        "idli",
        "dosa",
        "poha",
        "upma",
        "pongal",
        "paratha",
        "thepla",
        "puri",
        "uttapam",
        "appam",
        "puttu",
        "sheera",
        "halwa",
    ],
    "tiffin": [
        "idli",
        "dosa",
        "uttapam",
        "vada",
        "bonda",
        "samosa",
        "kachori",
        "pakora",
        "bhajji",
        "sandwich",
        "chaat",
        "pani puri",
        "bhel",
        "sev puri",
    ],
    "snack": [
        "snack",
        "chaat",
        "pakora",
        "bhajji",
        "bonda",
        "cutlet",
        "tikki",
        "toast",
    ],
    "dessert": [
        "halwa",
        "kheer",
        "payasam",
        "laddu",
        "ladoo",
        "barfi",
        "gulab jamun",
        "rasgulla",
        "jalebi",
        "kulfi",
        "shrikhand",
    ],
    "dinner": [
        "biryani",
        "pulao",
        "curry",
        "gravy",
        "dal",
        "roti",
        "naan",
        "chapati",
    ],
    # lunch is hard to infer from name reliably; we keep it conservative.
}


def infer_meal_type(title: str) -> List[TagCandidate]:
    text_l = (title or "").lower()
    tags: List[TagCandidate] = []
    for meal_type, kws in MEAL_TYPE_KEYWORDS.items():
        if _contains_any(text_l, kws):
            tags.append(_cand("meal_type", meal_type, label_en=meal_type.title(), confidence=0.70))
    return tags


# ---------------------------------------------------------------------
# Region hierarchy
# We emit a list like ["South Indian", "Karnataka", "Udupi"].
# Attach each as cuisine_region tag. You can later connect parent_id in tags table.
# ---------------------------------------------------------------------
REGION_LEXICON: List[Tuple[str, List[str]]] = [
    ("South Indian|Karnataka|Udupi", ["udupi"]),
    ("South Indian|Karnataka", ["karnataka", "mysore", "mangalore", "mangalore", "bisi bele bath"]),
    ("South Indian|Tamil Nadu|Chettinad", ["chettinad"]),
    ("South Indian|Tamil Nadu", ["tamil", "madras"]),
    ("South Indian|Kerala|Malabar", ["malabar"]),
    ("South Indian|Kerala", ["kerala", "naadan"]),
    ("South Indian|Andhra", ["andhra", "gongura", "pappu"]),
    ("South Indian|Telangana|Hyderabadi", ["hyderabadi", "nizami"]),
    ("North Indian|Punjab", ["punjab", "punjabi", "amritsari"]),
    ("North Indian|Rajasthan", ["rajasthan", "rajasthani", "dal baati", "gatte"]),
    ("North Indian|Uttar Pradesh|Awadhi", ["awadhi", "lucknowi"]),
    ("West Indian|Gujarat", ["gujarat", "gujarati", "dhokla", "khandvi", "undhiyu", "thepla"]),
    ("West Indian|Maharashtra", ["maharashtra", "maharashtrian", "kolhapuri", "misal", "vada pav"]),
    ("West Indian|Goa", ["goa", "goan", "xacuti", "vindaloo"]),
    ("East Indian|West Bengal", ["bengal", "bengali", "kosha", "shorshe"]),
    ("East Indian|Odisha", ["odisha", "oriya", "odia"]),
    ("East Indian|Assam", ["assam", "assamese"]),
]


def infer_region_path(title: str, ingredients: str = "", instructions: str = "") -> List[str]:
    text_l = " ".join([title or "", ingredients or "", instructions or ""]).lower()
    for path, kws in REGION_LEXICON:
        if _contains_any(text_l, kws):
            return [p.strip() for p in path.split("|") if p.strip()]
    return []

# Append new tag i.e. region tags based on region path
# Invoked Address : layer0_candidates in enrichment_pipeline.py
def region_tags_as_candidates(region_path: Sequence[str]) -> List[TagCandidate]:
    tags: List[TagCandidate] = []
    for i, node in enumerate(region_path):
        tags.append(
            _cand(
                "cuisine_region",
                node,
                label_en=node,
                confidence=0.80 if i == len(region_path) - 1 else 0.70,
                is_primary=(i == len(region_path) - 1),
            )
        )
    return tags


# ---------------------------------------------------------------------
# Diet inference (veg/non-veg/egg/vegan/Jain/no-onion-garlic)
# ---------------------------------------------------------------------
MEAT_KEYWORDS = [
    "chicken",
    "mutton",
    "lamb",
    "beef",
    "pork",
    "fish",
    "prawn",
    "shrimp",
    "crab",
]
EGG_KEYWORDS = [" egg", "eggs", "omelette", "omelet", "anda", "anda "]
DAIRY_KEYWORDS = ["milk", "curd", "yogurt", "ghee", "butter", "paneer", "cheese", "cream"]
ONION_GARLIC_KEYWORDS = ["onion", "garlic", "lehsun", "pyaaz"]
NO_ONION_GARLIC_HINTS = ["no onion", "no garlic", "without onion", "without garlic", "satvik", "satvic"]
JAIN_HINTS = ["jain", "jainism"]


def infer_diet(title: str, ingredients: str, instructions: str = "") -> List[TagCandidate]:
    text_l = " ".join([title or "", ingredients or "", instructions or ""]).lower()

    tags: List[TagCandidate] = []

    has_meat = _contains_any(text_l, MEAT_KEYWORDS)
    has_egg = _contains_any(text_l, EGG_KEYWORDS) and ("eggless" not in text_l)
    has_dairy = _contains_any(text_l, DAIRY_KEYWORDS)

    # Primary diet label (single)
    if has_meat:
        tags.append(_cand("diet", "non_vegetarian", "Non-vegetarian", confidence=0.90, is_primary=True))
    elif has_egg:
        tags.append(_cand("diet", "eggetarian", "Eggetarian", confidence=0.85, is_primary=True))
    elif has_dairy:
        tags.append(_cand("diet", "vegetarian", "Vegetarian", confidence=0.80, is_primary=True))
    else:
        tags.append(_cand("diet", "vegan", "Vegan", confidence=0.70, is_primary=True))

    # Jain / no-onion-garlic
    if _contains_any(text_l, JAIN_HINTS):
        tags.append(_cand("diet", "jain", "Jain", confidence=0.85))
    if _contains_any(text_l, NO_ONION_GARLIC_HINTS) or ("no-onion" in text_l) or ("no garlic" in text_l):
        tags.append(_cand("diet", "no_onion_garlic", "No onion-garlic", confidence=0.80))

    # If onion/garlic absent AND satvik cues: still tag no_onion_garlic (we keep conservative)
    if ("satvik" in text_l or "satvic" in text_l) and not _contains_any(text_l, ONION_GARLIC_KEYWORDS):
        tags.append(_cand("diet", "no_onion_garlic", "No onion-garlic", confidence=0.65))

    return tags


# ---------------------------------------------------------------------
# Technique / equipment inference
# ---------------------------------------------------------------------
EQUIPMENT_KEYWORDS: Dict[str, List[str]] = {
    "pressure_cooker": ["pressure cooker", "cooker"],
    "kadai": ["kadai", "karahi", "kadhai"],
    "tawa": ["tawa", "tava", "skillet"],
    "idli_stand": ["idli stand", "idli mould", "idli mold"],
    "oven": ["oven", "bake", "baked"],
    "mixer_grinder": ["grind", "blender", "mixer", "mixer grinder"],
    "steamer": ["steam", "steamed"],
}

TECHNIQUE_KEYWORDS: Dict[str, List[str]] = {
    "steamed": ["steam", "steamed"],
    "deep_fried": ["deep fry", "deep-fry", "deepfried"],
    "shallow_fried": ["shallow fry", "pan fry", "pan-fry"],
    "tawa_roasted": ["tawa", "roast", "roasted"],
    "tempering": ["tadka", "tempering", "phoron"],
    "pressure_cooked": ["pressure cook", "pressure cooker"],
    "baked": ["bake", "baked", "oven"],
}


def infer_equipment_and_technique(ingredients: str, instructions: str) -> List[TagCandidate]:
    text_l = " ".join([ingredients or "", instructions or ""]).lower()
    tags: List[TagCandidate] = []

    for eq, kws in EQUIPMENT_KEYWORDS.items():
        if _contains_any(text_l, kws):
            tags.append(_cand("equipment", eq, eq.replace("_", " ").title(), confidence=0.70))

    for tech, kws in TECHNIQUE_KEYWORDS.items():
        if _contains_any(text_l, kws):
            tags.append(_cand("technique", tech, tech.replace("_", " ").title(), confidence=0.70))

    return tags


# ---------------------------------------------------------------------
# Spice level (1-5) + kids-friendly
# ---------------------------------------------------------------------
SPICY_INGREDIENTS = [
    "green chilli",
    "green chili",
    "red chilli",
    "red chili",
    "chilli powder",
    "chili powder",
    "mirchi",
    "pepper",
    "black pepper",
    "schezwan",
    "garam masala",
]
MILD_HINTS = ["kids", "kid-friendly", "kids-friendly", "mild", "low spice", "less spicy"]
SPICE_BOOSTERS = ["extra spicy", "very spicy", "spicy", "hot"]


def infer_spice_level_and_kids_friendly(title: str, ingredients: str, instructions: str) -> Tuple[Optional[int], Optional[bool], List[TagCandidate]]:
    text_l = " ".join([title or "", ingredients or "", instructions or ""]).lower()

    spice_score = 0
    for k in SPICY_INGREDIENTS:
        if k in text_l:
            spice_score += 1

    # If explicit boosters exist, push higher
    if _contains_any(text_l, SPICE_BOOSTERS):
        spice_score += 2

    # Map score to 1..5 (heuristic)
    if spice_score <= 0:
        spice_level = 1
    elif spice_score == 1:
        spice_level = 2
    elif spice_score == 2:
        spice_level = 3
    elif spice_score == 3:
        spice_level = 4
    else:
        spice_level = 5

    kids_friendly: Optional[bool] = None
    if _contains_any(text_l, MILD_HINTS):
        kids_friendly = True
        spice_level = min(spice_level, 2)
    else:
        # If very spicy, likely not kids friendly
        if spice_level >= 4:
            kids_friendly = False

    tags: List[TagCandidate] = []
    tags.append(_cand("spice_level", str(spice_level), f"Spice {spice_level}", confidence=0.65))
    if kids_friendly is not None:
        tags.append(
            _cand(
                "kids_friendly",
                "kids_friendly" if kids_friendly else "not_kids_friendly",
                "Kids-friendly" if kids_friendly else "Not kids-friendly",
                confidence=0.65,
            )
        )

    return spice_level, kids_friendly, tags


# ---------------------------------------------------------------------
# Health / occasion hints (lightweight)
# ---------------------------------------------------------------------
HEALTH_HINTS: Dict[str, List[str]] = {
    "diabetic_friendly": ["diabetic", "low sugar", "no sugar"],
    "high_protein": ["protein", "paneer", "chana", "rajma", "dal", "lentil", "soya", "soy"],
    "high_fiber": ["fiber", "millet", "ragi", "bajra", "jowar", "oats"],
    "low_oil": ["baked", "steamed", "air fryer", "air-fryer", "low oil"],
    "satvik": ["satvik", "satvic"],
}
OCCASION_HINTS: Dict[str, List[str]] = {
    "festive": ["diwali", "holi", "eid", "navratri", "pongal", "onam", "festival"],
    "kids_lunchbox": ["lunchbox", "tiffin", "school"],
}


def infer_health_and_occasion(title: str, ingredients: str, instructions: str) -> List[TagCandidate]:
    text_l = " ".join([title or "", ingredients or "", instructions or ""]).lower()
    tags: List[TagCandidate] = []

    for key, kws in HEALTH_HINTS.items():
        if _contains_any(text_l, kws):
            tags.append(_cand("health", key, key.replace("_", " ").title(), confidence=0.60))

    for key, kws in OCCASION_HINTS.items():
        if _contains_any(text_l, kws):
            tags.append(_cand("occasion", key, key.replace("_", " ").title(), confidence=0.60))

    return tags


# ---------------------------------------------------------------------
# Public orchestration
# ---------------------------------------------------------------------
def layer0_candidates(title: str, ingredients: str, instructions: str) -> Tuple[List[TagCandidate], Dict[str, object]]:
    """
    Returns:
      (tags, derived_scalars)

    derived_scalars contains:
      - region_path
      - spice_level
      - kids_friendly
    """
    tags: List[TagCandidate] = []

    tags.extend(infer_meal_type(title))

    region_path = infer_region_path(title, ingredients, instructions)
    tags.extend(region_tags_as_candidates(region_path))

    tags.extend(infer_diet(title, ingredients, instructions))
    tags.extend(infer_equipment_and_technique(ingredients, instructions))

    spice_level, kids_friendly, spice_tags = infer_spice_level_and_kids_friendly(title, ingredients, instructions)
    tags.extend(spice_tags)

    tags.extend(infer_health_and_occasion(title, ingredients, instructions))

    derived = {
        "region_path": region_path,
        "spice_level": spice_level,
        "kids_friendly": kids_friendly,
    }
    return tags, derived
