# src/meal_taxonomy/brain/schema.py
from __future__ import annotations

"""
schema.py

Purpose:
    Shared dataclasses for the Meal Brain / enrichment pipeline.

    These are the "internal contracts" between:
      - source adapters (CSV, user form, chat, APIs),
      - enrichment layer (ML + LLM),
      - brain layer (dedupe + canonical/variant upsert).

    Nothing in this module talks to Supabase directly.

Purpose:
  Pydantic models / dataclasses for:
      - RawMeal (unified input)
      - EnrichedMealVariant (output of enrichment pipeline)

  This is the schema boundary between:
    - Dataset ingestion / user ingestion
    - Enrichment pipeline
    - Meal Brain upsert logic
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RawMeal:
    """
    RawMeal represents a meal as it appears in a single source
    (CSV row, user form, chat-extracted recipe, etc.)

    Unified meal record from any source:
      - Kaggle CSV / FoodOn / scraped sources
      - future user-submitted meals (form or chat)
    """

    source_type: str          # e.g. "kaggle_indian_full", "user_form", "user_chat"
    source_id: str            # row id / unique key in that source

    name: str
    description: Optional[str]

    ingredients_text: str     # free-text ingredients (possibly messy)
    instructions_text: str    # free-text instructions

    cuisine: Optional[str] = None
    course: Optional[str] = None
    diet: Optional[str] = None

    prep_time_mins: Optional[float] = None
    cook_time_mins: Optional[float] = None
    total_time_mins: Optional[float] = None
    servings: Optional[float] = None
    language_code: str = "en"

    extra: Dict[str, object] = field(default_factory=dict)


@dataclass
class EnrichedMealVariant:
    """
    EnrichedMealVariant is the "brain-ready" version of RawMeal.

    It contains:
      - cleaned / normalized text fields,
      - ML/LLM predictions (course, diet, region, spice, description, etc.),
      - embedding for similarity search,
      - tag candidates to feed into Meal Taxonomy (tags + meal_tags).

    Output of enrichment pipeline.
    This is the object you pass to Meal Brain.

    NOTE:
      - embeddings are stored as list[float] for supabase-py JSON serialization.
      - tags are stored separately in the taxonomy pipeline.
    """

    raw: RawMeal

    # Cleaned text
    canonical_name: str
    alt_names: List[str]
    ingredients_norm: str
    instructions_norm: str

    # Predictions
    predicted_course: Optional[str]
    predicted_diet: Optional[str]
    region_tags: List[str]
    spice_level: Optional[int]
    difficulty: Optional[str]
    kids_friendly: Optional[bool]

    occasion_tags: List[str]
    health_tags: List[str]
    utensil_tags: List[str]

    # Time & servings
    prep_time_mins: Optional[float]
    cook_time_mins: Optional[float]
    total_time_mins: Optional[float]
    servings: Optional[float]

    # NLP / tagging outputs
    tag_candidates: Dict[str, List[str]]  # tag_type -> list of tag values

    # Embedding for similarity search/dedupe
    embedding: Optional[List[float]] = None

    # Free-form extra info (nutrition, goes well with, etc.)
    extra: Dict[str, object] = field(default_factory=dict)

    # For observability / debugging
    debug: Dict[str, Any]
