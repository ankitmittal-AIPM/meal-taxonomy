# src/meal_taxonomy/etl/user_ingest.py
from __future__ import annotations

"""
user_ingest.py

Purpose:
    Convenience helpers for ingesting *future* user-submitted meals (form + chat)
    through the same pipeline used for datasets.

Why this exists:
  - You want one canonical/variant + enrichment path for everything.
  - User-submitted meals can be partially structured (form) or unstructured (chat).
  - We keep these utilities thin: they convert input -> RecipeRecord -> MealETL.ingest_recipe().

Note:
  - Chat extraction is intentionally optional and should be done with an LLM.
    The LLM is not enabled by default; see env vars in README.
"""

import uuid
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from supabase import Client

from src.meal_taxonomy.datasets.base import RecipeRecord
from src.meal_taxonomy.etl.pipeline import MealETL
from src.meal_taxonomy.enrichment.llm_enrichment import MealLLMEnricher


@dataclass
class UserMealForm:
    title: str
    ingredients: List[str]
    instructions: str
    description: Optional[str] = None
    cuisine: Optional[str] = None
    course: Optional[str] = None
    diet: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


def ingest_user_form(client: Client, user_id: str, form: UserMealForm) -> str:
    """Ingest a user meal form submission.

    Returns canonical_meal_id.
    """
    etl = MealETL(client)

    source_id = f"user:{user_id}:{uuid.uuid4()}"
    rec = RecipeRecord(
        source="user_form",
        source_id=source_id,
        name=form.title,
        ingredients=form.ingredients,
        instructions=form.instructions,
        description=form.description,
        cuisine=form.cuisine,
        course=form.course,
        diet=form.diet,
        meta=form.meta or {},
    )
    return etl.ingest_recipe(rec)


def extract_recipe_from_chat(chat_text: str) -> Optional[UserMealForm]:
    """(Optional) Extract a structured recipe draft from user chat text.

    This uses the optional LLM enricher if configured.
    If LLM is not available, returns None.
    """
    llm = MealLLMEnricher()
    if not llm.enabled():
        return None

    # Minimal extraction prompt: reuse MealLLMEnricher.enrich() by treating chat as instructions.
    # For production: create a dedicated structured extraction prompt (title/ingredients/instructions).
    res = llm.enrich(title="User submitted meal", ingredients="", instructions=chat_text, coarse={})
    if not res:
        return None

    title = res.canonical_name or "User meal"
    # We don't hallucinate ingredients; user should confirm/enter ingredients via UI if missing.
    return UserMealForm(
        title=title,
        ingredients=[],
        instructions=chat_text,
        description=res.description,
        cuisine="|".join(res.region_path) if res.region_path else None,
        course=res.meal_type,
        diet=res.diet,
        meta={"llm_extracted": True},
    )
