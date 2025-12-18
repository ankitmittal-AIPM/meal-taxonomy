# datasets/base.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
# Defining how meal will be defined and will be inserted in Supabase DB
class RecipeRecord:
    title: str
    description: Optional[str]
    ingredients: List[str]            # each row as free text line
    instructions: Optional[str]
    meta: Dict                        # arbitrary metadata from dataset
    source: str                       # 'RecipeDB', 'Food.com', 'IndianKaggle', etc.
    external_id: str                  # dataset-specific id
    language_code: str = "en"
    cook_time_minutes: Optional[int] = None
    prep_time_minutes: Optional[int] = None
