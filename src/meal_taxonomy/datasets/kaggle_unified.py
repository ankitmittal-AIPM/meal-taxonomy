# Kaggle_Loader/kaggle_unified.py

"""
What this does:
1. Handles different Kaggle schemas by using synonym sets (e.g., diet, diet_type, veg_or_nonveg are all treated as “diet”).
2. Produces RecipeRecord objects that pipeline.py already knows how to handle.
3. Populates meta with canonical keys: region, cuisine, course, diet, flavor, dataset_name.
4. Sets source (for Supabase external_source) to Kaggle:<file_name_without_extension>.

Because pipeline.dataset_tags() already expects these meta keys, it will:
1. Create tags under existing tag types like diet, cuisine_region, meal_type, taste_profile.
2. Not create any new tag_type names, so no duplicates there.
"""


from __future__ import annotations

from typing import Dict, List, Optional
import os

import pandas as pd

from src.meal_taxonomy.datasets.base import RecipeRecord


def _normalize_col_name(col: str) -> str:
    """
    Normalize column names so we can match them across different Kaggle datasets.
    Examples:
      "Recipe Name" -> "recipe_name"
      "Cook-Time(min)" -> "cook_time_min"
    """
    c = col.strip().lower()
    for ch in [" ", "-", ".", "(", ")", "[", "]"]:
        c = c.replace(ch, "_")
    while "__" in c:
        c = c.replace("__", "_")
    return c.strip("_")


# Canonical field synonym sets (normalized)
TITLE_COLS = {
    "name",
    "recipe_name",
    "title",
    "dish_name",
}
INGREDIENTS_COLS = {
    "ingredients",
    "ingredient_list",
    "recipe_ingredients",
    "translated_ingredients",
    "cleaned_ingredients",
}
INSTRUCTIONS_COLS = {
    "instructions",
    "directions",
    "steps",
    "method",
    "procedure",
    "cooking_directions",
}
CUISINE_COLS = {
    "cuisine",
    "region",
    "cuisine_region",
    "recipe_cuisine",
}
COURSE_COLS = {
    "course",
    "meal",
    "dish_type",
    "recipe_category",
    "category",
}
DIET_COLS = {
    "diet",
    "diet_type",
    "dietary_preference",
    "veg_or_nonveg",
    "veg_nonveg",
    "is_vegetarian",
    "is_veg",
}
FLAVOR_COLS = {
    "flavor",
    "flavour",
    "flavor_profile",
    "taste_profile",
    "taste",
}
PREP_TIME_COLS = {
    "prep_time",
    "preparation_time",
    "prep_time_min",
    "prep_time_mins",
    "preparation_time_min",
    "preparation_time_mins",
}
COOK_TIME_COLS = {
    "cook_time",
    "cooking_time",
    "cook_time_min",
    "cook_time_mins",
    "cooking_time_min",
    "cooking_time_mins",
}
TOTAL_TIME_COLS = {
    "total_time",
    "total_time_min",
    "total_time_mins",
    "ready_in_min",
    "ready_in_mins",
}


def _find_col(norm_to_orig: Dict[str, str], candidates: set[str]) -> Optional[str]:
    """
    Given a mapping of normalized -> original column names, return the original
    name for the first candidate that exists.
    """
    for cand in candidates:
        if cand in norm_to_orig:
            return norm_to_orig[cand]
    return None


def _parse_int_maybe(value) -> Optional[int]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        # Sometimes values like "15 mins"
        for token in ["mins", "min", "minutes", "minute"]:
            s = s.replace(token, "")
        s = s.strip()
        return int(float(s))
    except Exception:
        return None


def _normalize_diet(raw: Optional[str], row: pd.Series) -> Optional[str]:
    """
    Try to normalize any diet-related column(s) into one of:
      'vegan', 'vegetarian', 'non_vegetarian', 'eggetarian'
    """
    # 1) If we directly got something
    if raw:
        s = str(raw).strip().lower()
        if "vegan" in s:
            return "vegan"
        if s in {"veg", "vegetarian", "pure veg", "lacto-vegetarian", "ovo-vegetarian"}:
            return "vegetarian"
        if s in {"non veg", "non-veg", "nonvegetarian", "non vegetarian"}:
            return "non_vegetarian"

    # 2) Heuristics based on boolean columns
    # is_vegetarian / is_veg like 1/0 or True/False or Yes/No
    for col in row.index:
        n = _normalize_col_name(col)
        if n in {"is_vegetarian", "is_veg"}:
            v = str(row[col]).strip().lower()
            if v in {"1", "true", "yes"}:
                return "vegetarian"
            if v in {"0", "false", "no"}:
                return "non_vegetarian"

    return None


def load_kaggle_csv(path: str, dataset_name: Optional[str] = None) -> List[RecipeRecord]:
    """
    Load a Kaggle-like CSV and normalize it into a list of RecipeRecord objects.

    - Handles different column names across files using synonym sets above
    - Populates meta keys consistent with pipeline.dataset_tags(): region, cuisine, course, diet, flavor
    - Sets rec.source to dataset_name (or file stem) so Supabase external_source can track origin
    """
    if dataset_name is None:
        dataset_name = os.path.splitext(os.path.basename(path))[0]

    df = pd.read_csv(path)
    # Normalize column names
    norm_to_orig: Dict[str, str] = {}
    for orig in df.columns:
        norm = _normalize_col_name(orig)
        norm_to_orig[norm] = orig

    title_col = _find_col(norm_to_orig, TITLE_COLS)
    ingredients_col = _find_col(norm_to_orig, INGREDIENTS_COLS)
    instructions_col = _find_col(norm_to_orig, INSTRUCTIONS_COLS)
    cuisine_col = _find_col(norm_to_orig, CUISINE_COLS)
    course_col = _find_col(norm_to_orig, COURSE_COLS)
    diet_col = _find_col(norm_to_orig, DIET_COLS)
    flavor_col = _find_col(norm_to_orig, FLAVOR_COLS)
    prep_col = _find_col(norm_to_orig, PREP_TIME_COLS)
    cook_col = _find_col(norm_to_orig, COOK_TIME_COLS)
    total_col = _find_col(norm_to_orig, TOTAL_TIME_COLS)

    records: List[RecipeRecord] = []

    for idx, row in df.iterrows():
        # Title
        title = str(row[title_col]).strip() if title_col else f"Recipe {idx}"

        # Ingredients
        raw_ing = ""
        if ingredients_col:
            raw_ing = row.get(ingredients_col) or ""
        ingredients: List[str] = []
        if isinstance(raw_ing, str):
            # Very common pattern: comma-separated ingredients
            ingredients = [i.strip() for i in raw_ing.split(",") if i.strip()]
        elif isinstance(raw_ing, (list, tuple)):
            ingredients = [str(i).strip() for i in raw_ing if str(i).strip()]

        # Instructions
        instructions = ""
        if instructions_col:
            instructions = str(row.get(instructions_col) or "").strip()

        # Meta fields that pipeline.dataset_tags() understands
        region = None
        cuisine = None
        if cuisine_col:
            cuisine_val = str(row.get(cuisine_col) or "").strip()
            # Some datasets use "Indian" vs "South Indian" etc.
            region = cuisine_val  # we treat it as region/cuisine_region for now
            cuisine = cuisine_val

        course = str(row.get(course_col) or "").strip() if course_col else None
        raw_diet = str(row.get(diet_col) or "").strip() if diet_col else None
        diet = _normalize_diet(raw_diet, row)
        flavor = str(row.get(flavor_col) or "").strip() if flavor_col else None

        prep_time = _parse_int_maybe(row.get(prep_col) if prep_col else None)
        cook_time = _parse_int_maybe(row.get(cook_col) if cook_col else None)
        total_time = _parse_int_maybe(row.get(total_col) if total_col else None)
        # If total_time is provided but not split, you can decide whether to use it;
        # for now we keep prep/cook as they are and let bucket_time handle it.

        meta = {
            "region": region,
            "cuisine": cuisine,
            "course": course,
            "diet": diet,
            "flavor": flavor,
            "dataset_name": dataset_name,
        }

        rec = RecipeRecord(
            title=title,
            description=None,
            ingredients=ingredients,
            instructions=instructions,
            meta=meta,
            source=f"Kaggle:{dataset_name}",
            external_id=f"{dataset_name}_{idx}",
            language_code="en",
            cook_time_minutes=cook_time or total_time,
            prep_time_minutes=prep_time,
        )
        records.append(rec)

    return records
