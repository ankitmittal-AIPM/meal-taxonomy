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

    The goal is to keep these dataclasses stable so that each layer can evolve
    independently without breaking the end-to-end pipeline.

Objects:
      - RawMeal (unified input)
      - EnrichedMealVariant (output of enrichment pipeline)

  This is the schema boundary between:
    - Dataset ingestion / user ingestion
    - Enrichment pipeline
    - Meal Brain upsert logic
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    # Imported only for type checking to avoid pulling optional heavy deps
    # (e.g., transformers) on import of this module.
    from src.meal_taxonomy.nlp_tagging import TagCandidate


@dataclass
class RawMeal:
    """Unified input payload for a meal (dataset row, user form, chat parse, etc.)."""

    # Provenance / source identity
    source_type: str               # e.g. "Kaggle:indian_food" or "user_form"
    source_id: str                 # dataset-specific key / external id

    # Core text fields
    name: str
    description: Optional[str]
    ingredients_text: str
    instructions_text: str

    # Optional structured metadata
    cuisine: Optional[str] = None
    course: Optional[str] = None
    diet: Optional[str] = None

    prep_time_mins: Optional[float] = None
    cook_time_mins: Optional[float] = None
    total_time_mins: Optional[float] = None
    servings: Optional[float] = None

    # Any additional info from dataset/user (region, flavor, images, urls, etc.)
    extra: Dict[str, object] = field(default_factory=dict)


@dataclass
class EnrichedMealVariant:
    """Output of the enrichment pipeline for a given RawMeal."""

    raw: RawMeal

    # Canonicalization results
    canonical_name: str
    alt_names: List[str]

    # Cleaned/normalized fields
    ingredients_norm: str
    instructions_norm: str

    # Derived features / predictions
    predicted_course: Optional[str]
    predicted_diet: Optional[str]
    region_tags: List[str]

    spice_level: Optional[int]
    difficulty: Optional[str]
    kids_friendly: Optional[bool]

    occasion_tags: List[str]
    health_tags: List[str]
    utensil_tags: List[str]

    # Time signals (can be filled from dataset or predicted)
    prep_time_mins: Optional[float]
    cook_time_mins: Optional[float]
    total_time_mins: Optional[float]
    servings: Optional[float]

    # NLP / tagging outputs
    # tag_candidates: Dict[str, List[str]]  # tag_type -> list of tag values
    tag_candidates: List["TagCandidate"]  # unified TagCandidate list across layers

    # Embedding for similarity search/dedupe
    embedding: Optional[List[float]] = None

    # Free-form extra info (nutrition, goes well with, etc.)
    extra: Dict[str, object] = field(default_factory=dict)

    # For observability / debugging
    debug: Dict[str, Any] = field(default_factory=dict)
