# src/meal_taxonomy/enrichment/enrichment_pipeline.py
from __future__ import annotations

"""
enrichment_pipeline.py

Purpose:
    Take a RawMeal and produce an EnrichedMealVariant that is ready
    for the Meal Brain (canonical/variant upsert) and Meal Taxonomy
    (tags + ontology).

    It combines:
      - deterministic cleaning (cleaning.py),
      - ML predictions (course, diet, region, etc.) - optional/stubbed,
      - existing NLP tagging (RecipeNLP),
      - optional LLM enrichment (synonyms, description, occasion tags),
      - embedding computation.
"""

from typing import Dict, List, Optional

from src.meal_taxonomy.logging_utils import get_logger, RUN_ID
from src.meal_taxonomy.nlp_tagging import RecipeNLP, TagCandidate
from src.meal_taxonomy.enrichment.cleaning import (
    clean_meal_name,
    normalize_ingredients,
    normalize_instructions,
)
from src.meal_taxonomy.enrichment.embeddings import get_meal_embedding
from src.meal_taxonomy.brain.schema import RawMeal, EnrichedMealVariant

MODULE_PURPOSE = (
    "Enrichment layer converting RawMeal into EnrichedMealVariant "
    "using cleaning, NLP tagging, ML predictions, and optional LLM metadata."
)

logger = get_logger("enrichment_pipeline")


class MealEnrichmentPipeline:
    def __init__(self, use_llm: bool = False) -> None:
        self.use_llm = use_llm
        self.nlp = RecipeNLP()
        # You can plug ML models here later (course/diet/region classifiers, etc.)
        self._init_llm_if_needed()

    def _init_llm_if_needed(self) -> None:
        if not self.use_llm:
            self.llm_client = None
            return
        # TODO: wire this to your LLM of choice (e.g. OpenAI client)
        self.llm_client = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def enrich(self, raw: RawMeal) -> EnrichedMealVariant:
        """
        Main entry: RawMeal -> EnrichedMealVariant.
        """
        logger.info(
            "Enriching RawMeal '%s' (source_type=%s, source_id=%s, run_id=%s)",
            raw.name,
            raw.source_type,
            raw.source_id,
            RUN_ID,
            extra={
                "invoking_func": "MealEnrichmentPipeline.enrich",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "clean text & run NLP tagging",
                "resolution": "",
            },
        )

        canonical_name = clean_meal_name(raw.name) or raw.name
        ingredients_norm = normalize_ingredients(raw.ingredients_text)
        instructions_norm = normalize_instructions(raw.instructions_text)

        # ---------------- NLP tagging (existing RecipeNLP) ----------------
        tag_candidates = self._tag_with_nlp(
            title=canonical_name,
            ingredients=ingredients_norm,
            instructions=instructions_norm,
        )

        # ---------------- ML predictions (stubs for now) ------------------
        predicted_course = raw.course
        predicted_diet = raw.diet
        region_tags: List[str] = []
        spice_level: Optional[int] = None
        difficulty: Optional[str] = None
        kids_friendly: Optional[bool] = None
        health_tags: List[str] = []
        utensil_tags: List[str] = []

        # ---------------- Optional LLM enrichment -------------------------
        alt_names: List[str] = []
        occasion_tags: List[str] = []
        extra: Dict[str, object] = {}

        if self.llm_client is not None:
            # TODO: implement LLM call to enrich synonyms/description/tags
            pass

        # ---------------- Time + embedding --------------------------------
        total_time = raw.total_time_mins
        prep_time = raw.prep_time_mins
        cook_time = raw.cook_time_mins

        # If only total_time is present, you can estimate prep/cook later.
        # For now, keep what RawMeal gives you.

        embedding_text = " ".join(
            [
                canonical_name or "",
                ingredients_norm,
                raw.cuisine or "",
                raw.course or "",
                raw.diet or "",
            ]
        )
        embedding = get_meal_embedding(embedding_text)

        enriched = EnrichedMealVariant(
            raw=raw,
            canonical_name=canonical_name,
            alt_names=alt_names,
            ingredients_norm=ingredients_norm,
            instructions_norm=instructions_norm,
            predicted_course=predicted_course,
            predicted_diet=predicted_diet,
            region_tags=region_tags,
            spice_level=spice_level,
            difficulty=difficulty,
            kids_friendly=kids_friendly,
            occasion_tags=occasion_tags,
            health_tags=health_tags,
            utensil_tags=utensil_tags,
            prep_time_mins=prep_time,
            cook_time_mins=cook_time,
            total_time_mins=total_time,
            servings=raw.servings,
            tag_candidates=tag_candidates,
            embedding=embedding or None,
            extra=extra,
        )

        logger.info(
            "Enrichment done for RawMeal '%s' -> canonical_name='%s'",
            raw.name,
            enriched.canonical_name,
            extra={
                "invoking_func": "MealEnrichmentPipeline.enrich",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "hand off to Meal Brain upsert_meal",
                "resolution": "",
            },
        )

        return enriched

    # ------------------------------------------------------------------
    # Helper(s)
    # ------------------------------------------------------------------
    def _tag_with_nlp(
        self,
        title: str,
        ingredients: str,
        instructions: str,
    ) -> Dict[str, List[str]]:
        """
        Use existing RecipeNLP to generate TagCandidate objects and then map them
        into a simple dictionary: tag_type -> [values].
        """
        candidates: List[TagCandidate] = self.nlp.generate_tag_candidates_for_recipe(
            title=title,
            ingredients=ingredients,
            instructions=instructions,
        )
        result: Dict[str, List[str]] = {}
        for cand in candidates:
            bucket = result.setdefault(cand.tag_type, [])
            if cand.value not in bucket:
                bucket.append(cand.value)
        return result
