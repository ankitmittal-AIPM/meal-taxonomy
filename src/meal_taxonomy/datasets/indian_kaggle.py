# Assuming a CSV with columns like: id, name, ingredients, instructions, region, course, diet, flavor, prep_time, cook_time
# Gives you a clear error showing Available columns: [...] if it still can’t find it.
# Handles different names for title, region, course, diet, flavor, times, and instructions.

# datasets/indian_kaggle.py
from __future__ import annotations

from typing import List, Optional
import pandas as pd
import csv

from .base import RecipeRecord

# Reads CSV data and insert into datatable
# This reads the data as it is from the csv file and just add each cell value in double quotes in Data table
# No major activity - Cleans up the records and add all records from CSV file as it is in double quotes
def _load_csv_robust(path: str) -> pd.DataFrame:
    """
    Load your specific CSV format:

    - File is UTF-8 with BOM (\ufeff)
    - Each line (header + rows) is wrapped in outer double quotes:
        "name,ingredients,instructions,..."
        "Spicy Chickpea Curry,""chickpeas,tomatoes"",..."
    - Inside, text fields use double-double-quotes ""value""
    - Commas separate columns
    """

    # Read all raw lines, stripping BOM automatically
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        raw_lines = [line.strip() for line in f if line.strip()]

    if not raw_lines:
        raise ValueError("CSV file is empty")

    # -----------------------
    # Parse HEADER
    # -----------------------
    header_line = raw_lines[0]

    # Remove outer quotes if they exist
    if header_line.startswith('"') and header_line.endswith('"'):
        header_line = header_line[1:-1]

    # Normalize repeated quotes
    header_line = header_line.replace('""', '"')

    # Split header by comma → produces column list
    header = [h.strip() for h in header_line.split(",")]

    # -----------------------
    # Parse DATA rows
    # -----------------------
    rows = []

    for line in raw_lines[1:]:
        # Remove surrounding quotes
        if line.startswith('"') and line.endswith('"'):
            inner = line[1:-1]
        else:
            inner = line

        # Convert ""text"" → "text"
        inner_clean = inner.replace('""', '"')

        # Let csv.reader correctly parse quoted fields
        reader = csv.reader([inner_clean])
        row = next(reader)

        rows.append(row)

    # -----------------------
    # Build DataFrame
    # -----------------------
    df = pd.DataFrame(rows, columns=header)
    return df

# Invoked Address : ingest_indian_kaggle from pipeline.py
# This normalizes the columns from the data and set the data table records in predefined
# To Do: Enhance the range for each columns in column normalization
def load_indian_kaggle_csv(path: str, limit: Optional[int] = None,) -> List[RecipeRecord]:
    # Load data from CSV file as each cell in double quotes
    df = _load_csv_robust(path)

    #--Start Cleaning & Normalizing Columns ingested from CSV file----------------------------------------------------------------------------------
    # Normalize column names to lowercase, stripped
    df.columns = [c.strip().lower() for c in df.columns]

    # Matching Column search and mapping. It's heuristic rule based not ML or LLM based
    # Ingredient Column Search ---
    # Detect the ingredients column from the data file
    possible_ingredient_cols = ["ingredients", "ingredient", "ingredient_list", "ingr"]
    ingredient_col = None
    for col in possible_ingredient_cols:
        if col in df.columns:
            ingredient_col = col
            break
    # Raise warning on matching ingredient column not found
    if ingredient_col is None:
        raise ValueError(
            f"Could not find an ingredients column. Available columns: {list(df.columns)}. "
            "Please rename your ingredients column to one of: "
            f"{possible_ingredient_cols}"
        )

    # Meal Title Column Search ---
    # Title column
    title_col = None
    for candidate in ["name", "recipe_name", "title"]:
        if candidate in df.columns:
            title_col = candidate
            break
    # Raise warning on matching Title column not found
    if title_col is None:
        raise ValueError(
            f"Could not find a title column. Available columns: {list(df.columns)}. "
            "Please rename your recipe title column to one of: ['name', 'recipe_name', 'title']"
        )

    # Region Column Search ---
    # Optional columns
    region_col = next((c for c in ["region", "cuisine_region"] if c in df.columns), None)
    course_col = next((c for c in ["course", "meal_type"] if c in df.columns), None)
    diet_col = next((c for c in ["diet", "diet_type"] if c in df.columns), None)
    flavor_col = next((c for c in ["flavor", "flavour", "flavor_profile"] if c in df.columns), None)
    prep_time_col = next((c for c in ["prep_time", "preptime", "preparation_time"] if c in df.columns), None)
    cook_time_col = next((c for c in ["cook_time", "cooktime", "cooking_time"] if c in df.columns), None)
    instructions_col = next((c for c in ["instructions", "steps", "directions", "method"] if c in df.columns), None)
    id_col = next((c for c in ["id", "recipe_id"] if c in df.columns), None)

    #--End Cleaning & Normalizing Columns ingested from CSV file----------------------------------------------------------------------------------
    records: list[RecipeRecord] = []
    # Reading rows from dataset and constructing records to ingest in DB
    for _, row in df.iterrows():
        # Ingredients list
        raw_ing = str(row[ingredient_col])
        ingredients = [i.strip() for i in raw_ing.split(",") if i and str(i).strip()]

        meta = {
            "region": (str(row[region_col]).strip() if region_col else None),
            "course": (str(row[course_col]).strip() if course_col else None),
            "diet": (str(row[diet_col]).strip() if diet_col else None),
            "flavor": (str(row[flavor_col]).strip() if flavor_col else None),
        }

        cook_time = None
        if cook_time_col and pd.notna(row[cook_time_col]):
            try:
                cook_time = int(row[cook_time_col])
            except ValueError:
                cook_time = None

        prep_time = None
        if prep_time_col and pd.notna(row[prep_time_col]):
            try:
                prep_time = int(row[prep_time_col])
            except ValueError:
                prep_time = None

        rec = RecipeRecord(
            title=str(row[title_col]),
            description=None,
            ingredients=ingredients,
            instructions=str(row[instructions_col]) if instructions_col else None,
            meta=meta,
            source="IndianKaggle",
            external_id=str(row[id_col] if id_col else row[title_col]),
            language_code="en",
            cook_time_minutes=cook_time,
            prep_time_minutes=prep_time,
        )
        records.append(rec)
    return records


# Example usage:
# recipes = load_indian_kaggle_csv("indian_recipes.csv")
# You’d create similar adapters for RecipeDB, Food.com, etc., normalizing their columns into RecipeRecord. (RecipeDB conceptually – you’ll align column names locally.)