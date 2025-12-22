# src/meal_taxonomy/enrichment/enrichment_pipeline.py
from __future__ import annotations

"""
enrichment_pipeline.py

Purpose:
    Take a RawMeal and produce an EnrichedMealVariant that is ready for:
      - Meal Brain (canonical/variant clustering + upsert)
      - Meal Taxonomy (tags + ontology linking)

This implements the 3-layer enrichment strategy:

Layer 0 (deterministic, always-on by default)
  - Clean meal names (remove noise, normalize spellings)
  - Normalize ingredients/instructions into single blocks
  - Rule-based inference for:
      * meal_type
      * cuisine_region hierarchy
      * diet (veg/non-veg/egg/vegan/Jain/no-onion-garlic)
      * equipment
      * spice_level (1-5) + kids_friendly
      * basic health/occasion cues

Layer 1 (supervised ML, optional)
  - Small fast scikit-learn models trained on Indian recipe datasets:
      * meal_type / course classifier
      * diet classifier
      * region classifier
      * spice classifier
      * health multi-label classifier
      * prep/cook time regressors
  - If models are not present, this layer gracefully no-ops.

Layer 2 (LLM semantic enrichment, optional)
  - Alternate names (languages + spellings + colloquial)
  - Better region path suggestions
  - Brand-voice description + pairing suggestions
  - Extra tags that are hard to model classically

Important:
  - This pipeline is designed to be "toggle based".
  - It should never crash ingestion if ML/LLM is unavailable.

"""

import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from src.meal_taxonomy.logging_utils import get_logger
from src.meal_taxonomy.nlp_tagging import RecipeNLP, TagCandidate

from src.meal_taxonomy.brain.schema import RawMeal, EnrichedMealVariant
from src.meal_taxonomy.enrichment.cleaning import (
    clean_meal_name,
    normalize_ingredients,
    normalize_instructions,
    normalize_title,
    split_ingredient_lines,
)
from src.meal_taxonomy.enrichment.embeddings import get_meal_embedding
from src.meal_taxonomy.enrichment.signals import layer0_candidates
from src.meal_taxonomy.enrichment.ml_models import IndianMLModels
from src.meal_taxonomy.enrichment.llm_enrichment import MealLLMEnricher

logger = get_logger("enrichment_pipeline")

RUN_ID = str(uuid.uuid4())
MODULE_PURPOSE = "Meal Enrichment (Layer0 rules + optional Layer1 ML + optional Layer2 LLM)"


@dataclass
class MealEnrichmentConfig:
    enable_layer0: bool = True
    enable_layer1_ml: bool = True
    enable_layer2_llm: bool = False
    enable_embeddings: bool = True

    # Layer-1 ML
    models_dir: str = "models_store"
    health_ml_threshold: float = 0.45

    # Merge policy
    ml_min_confidence: float = 0.45


class MealEnrichmentPipeline:
    def __init__(self, config: Optional[MealEnrichmentConfig] = None) -> None:
        self.config = config or MealEnrichmentConfig()
        self.nlp = RecipeNLP()

        self.ml_models = IndianMLModels(self.config.models_dir) if self.config.enable_layer1_ml else None
        self.llm = MealLLMEnricher() if self.config.enable_layer2_llm else None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def enrich(self, raw: RawMeal) -> EnrichedMealVariant:
        """Main entry: RawMeal -> EnrichedMealVariant."""

        logger.info(
            "Enriching RawMeal '%s' (source_type=%s, source_id=%s, run_id=%s)",
            raw.name,
            raw.source_type,
            raw.source_id,
            RUN_ID,
            extra={
                "invoking_func": "MealEnrichmentPipeline.enrich",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "clean text + Layer0 + NLP + (optional ML/LLM)",
                "resolution": "",
            },
        )

        canonical_name = clean_meal_name(raw.name) or raw.name
        ingredients_norm = normalize_ingredients(raw.ingredients_text)
        instructions_norm = normalize_instructions(raw.instructions_text)

        # Accumulate TagCandidate objects (unified)
        candidates: List[TagCandidate] = []

        # ---------------- Dataset meta -> tags ----------------
        candidates.extend(self._dataset_meta_candidates(raw))

        # ---------------- NLP tags (existing RecipeNLP) ----------------
        candidates.extend(self._nlp_candidates(raw, canonical_name, instructions_norm))

        # ---------------- Layer 0: deterministic signals ----------------
        region_path: List[str] = []
        spice_level: Optional[int] = None
        kids_friendly: Optional[bool] = None

        if self.config.enable_layer0:
            layer0_tags, derived = layer0_candidates(
                title=canonical_name,
                ingredients=ingredients_norm,
                instructions=instructions_norm,
            )
            candidates.extend(layer0_tags)
            region_path = derived.get("region_path") or []
            spice_level = derived.get("spice_level")
            kids_friendly = derived.get("kids_friendly")

        # ---------------- Layer 1: supervised ML (optional) ----------------
        predicted_course = raw.course
        predicted_diet = raw.diet

        ml_debug: Dict[str, object] = {}

        if self.config.enable_layer1_ml and self.ml_models and self.ml_models.enabled():
            ml_text = self._ml_text(raw, canonical_name, ingredients_norm, instructions_norm)
            candidates, predicted_course, predicted_diet, spice_level, region_path, ml_debug = self._apply_ml(
                candidates=candidates,
                raw=raw,
                ml_text=ml_text,
                current_course=predicted_course,
                current_diet=predicted_diet,
                current_spice_level=spice_level,
                current_region_path=region_path,
            )

        # ---------------- Layer 2: LLM semantic enrichment (optional) -----
        alt_names: List[str] = []
        occasion_tags: List[str] = []
        health_tags: List[str] = []
        utensil_tags: List[str] = []
        difficulty: Optional[str] = None

        llm_debug: Dict[str, object] = {}

        if self.config.enable_layer2_llm and self.llm and self.llm.enabled():
            coarse = {
                "predicted_course": predicted_course,
                "predicted_diet": predicted_diet,
                "region_path": region_path,
                "spice_level": spice_level,
                "kids_friendly": kids_friendly,
            }
            llm_res = self.llm.enrich(
                title=canonical_name,
                ingredients=ingredients_norm,
                instructions=instructions_norm,
                coarse=coarse,
            )
            if llm_res:
                llm_debug = {
                    "llm_model": getattr(self.llm, "model", None),
                }

                # Canonical name override (optional)
                if llm_res.canonical_name and llm_res.canonical_name.strip():
                    canonical_name = llm_res.canonical_name.strip()

                # Alt names (store flat list + detailed in extra)
                alt_names = [d.get("name", "").strip() for d in (llm_res.alt_names or []) if d.get("name")]
                alt_names = [x for x in alt_names if x and x.lower() != canonical_name.lower()]

                # LLM description
                if llm_res.description:
                    llm_debug["description"] = llm_res.description

                # Region path override
                if llm_res.region_path:
                    region_path = [str(x).strip() for x in llm_res.region_path if str(x).strip()]

                # Apply LLM-derived tags (equipment/technique/health/occasion/spice)
                candidates.extend(self._llm_candidates(llm_res))

                # Override a few scalar fields if present
                if llm_res.meal_type:
                    predicted_course = llm_res.meal_type
                if llm_res.diet:
                    predicted_diet = llm_res.diet
                if llm_res.spice_level:
                    spice_level = int(llm_res.spice_level)
                if llm_res.kids_friendly is not None:
                    kids_friendly = bool(llm_res.kids_friendly)

                occasion_tags = llm_res.occasion_tags or []
                health_tags = llm_res.health_tags or []
                utensil_tags = llm_res.equipment or []
                # difficulty is not yet produced as a dedicated field in LLM schema; keep None for now

        # ---------------- Ingredient extraction (for ontology linking) -----
        ingredient_lines = split_ingredient_lines(ingredients_norm)

        # ---------------- Time handling -----------------------------------
        prep_time = raw.prep_time_mins
        cook_time = raw.cook_time_mins
        total_time = raw.total_time_mins

        # If ML models provide time regressors and raw times are missing
        if self.config.enable_layer1_ml and self.ml_models and self.ml_models.enabled():
            if (prep_time is None or cook_time is None) and (raw.total_time_mins is not None):
                # We use the same ml_text from earlier if available; else rebuild.
                ml_text = self._ml_text(raw, canonical_name, ingredients_norm, instructions_norm)
                times = self.ml_models.predict_prep_cook_time(ml_text)
                if prep_time is None and times.prep_time_mins is not None:
                    prep_time = float(times.prep_time_mins)
                if cook_time is None and times.cook_time_mins is not None:
                    cook_time = float(times.cook_time_mins)

        # Compute total if missing
        if total_time is None and prep_time is not None and cook_time is not None:
            total_time = float(prep_time) + float(cook_time)

        # ---------------- Embeddings (for search/dedupe) -------------------
        embedding = None
        if self.config.enable_embeddings:
            # Build a compact text doc
            doc = self._embedding_text(canonical_name, ingredients_norm, instructions_norm, predicted_course, predicted_diet, region_path)
            embedding = get_meal_embedding(doc)

        # ---------------- Merge debug/derived ------------------------------
        debug: Dict[str, object] = {
            "run_id": RUN_ID,
            "canonical_name_norm": normalize_title(canonical_name),
            "ingredient_lines": ingredient_lines[:30],
            "region_path": region_path,
            "spice_level": spice_level,
            "kids_friendly": kids_friendly,
            "ml_debug": ml_debug,
            "llm_debug": llm_debug,
        }

        # If llm_debug contains description, surface in debug for Meal Brain insert
        if isinstance(llm_debug.get("description"), str):
            debug["description"] = llm_debug.get("description")

        # Collect region tags as flat list
        region_tags = region_path

        # Also collect health/occasion from ML layer if present
        if not health_tags and isinstance(ml_debug.get("health_tags"), list):
            health_tags = [str(x) for x in ml_debug.get("health_tags") if x]
        if not occasion_tags and isinstance(ml_debug.get("occasion_tags"), list):
            occasion_tags = [str(x) for x in ml_debug.get("occasion_tags") if x]

        return EnrichedMealVariant(
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
            tag_candidates=candidates,
            embedding=embedding,
            extra={},
            debug=debug,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _dataset_meta_candidates(self, raw: RawMeal) -> List[TagCandidate]:
        """
        Convert any dataset-provided fields to TagCandidate if they align.

        Example:
          raw.course / raw.diet / raw.cuisine can seed tags with moderate confidence.
        """
        tags: List[TagCandidate] = []
        if raw.course:
            tags.append(TagCandidate(tag_type="course", value=str(raw.course), label_en=str(raw.course), confidence=0.60, is_primary=True, source="dataset"))
        if raw.diet:
            tags.append(TagCandidate(tag_type="diet", value=str(raw.diet), label_en=str(raw.diet), confidence=0.60, is_primary=True, source="dataset"))
        if raw.cuisine:
            tags.append(TagCandidate(tag_type="cuisine_region", value=str(raw.cuisine), label_en=str(raw.cuisine), confidence=0.55, is_primary=False, source="dataset"))
        return tags

    def _nlp_candidates(self, raw: RawMeal, canonical_name: str, instructions: str) -> List[TagCandidate]:
        """Existing NLP tagger candidates (pattern-based)."""
        txt = f"{canonical_name}\n{raw.ingredients_text}\n{instructions}"
        return self.nlp.tag_recipe_text(txt)

    def _ml_text(self, raw: RawMeal, canonical_name: str, ingredients: str, instructions: str) -> str:
        """Text fed to ML models."""
        # Keep it consistent with training script: title + ingredients + instructions + coarse fields
        coarse = f"{raw.cuisine or ''} {raw.course or ''} {raw.diet or ''}"
        return f"{canonical_name}\n{ingredients}\n{instructions}\n{coarse}".strip()

    def _apply_ml(
        self,
        *,
        candidates: List[TagCandidate],
        raw: RawMeal,
        ml_text: str,
        current_course: Optional[str],
        current_diet: Optional[str],
        current_spice_level: Optional[int],
        current_region_path: List[str],
    ) -> Tuple[List[TagCandidate], Optional[str], Optional[str], Optional[int], List[str], Dict[str, object]]:
        """
        Apply ML predictions and merge into candidates and scalar fields.

        Merge policy:
          - only accept if confidence >= config.ml_min_confidence
          - ML can override dataset values (but not LLM, which runs later)
        """
        debug: Dict[str, object] = {}

        if not self.ml_models:
            return candidates, current_course, current_diet, current_spice_level, current_region_path, debug

        # Course
        course = self.ml_models.predict_course(ml_text)
        if course and course.confidence >= self.config.ml_min_confidence:
            current_course = course.value
            candidates.append(TagCandidate(tag_type="course", value=course.value, label_en=course.value, confidence=course.confidence, is_primary=True, source="layer1_ml"))
            debug["course"] = {"value": course.value, "confidence": course.confidence}

        # Diet
        diet = self.ml_models.predict_diet(ml_text)
        if diet and diet.confidence >= self.config.ml_min_confidence:
            current_diet = diet.value
            candidates.append(TagCandidate(tag_type="diet", value=diet.value, label_en=diet.value, confidence=diet.confidence, is_primary=True, source="layer1_ml"))
            debug["diet"] = {"value": diet.value, "confidence": diet.confidence}

        # Region (cuisine)
        region = self.ml_models.predict_region(ml_text)
        if region and region.confidence >= self.config.ml_min_confidence:
            # region value could be either a label or a path encoded with "|"
            val = region.value
            if "|" in val:
                current_region_path = [p.strip() for p in val.split("|") if p.strip()]
            else:
                current_region_path = [val]
            candidates.append(TagCandidate(tag_type="cuisine_region", value=val, label_en=val, confidence=region.confidence, is_primary=True, source="layer1_ml"))
            debug["region"] = {"value": val, "confidence": region.confidence}

        # Spice
        spice = self.ml_models.predict_spice_level_1_to_5(ml_text)
        if spice and spice.confidence >= self.config.ml_min_confidence:
            try:
                current_spice_level = int(float(spice.value))
                candidates.append(TagCandidate(tag_type="spice_level", value=str(current_spice_level), label_en=f"Spice {current_spice_level}", confidence=spice.confidence, is_primary=False, source="layer1_ml"))
                debug["spice"] = {"value": current_spice_level, "confidence": spice.confidence}
            except Exception:
                pass

        # Health multi-label
        health = self.ml_models.predict_health_tags(ml_text, threshold=self.config.health_ml_threshold)
        if health:
            debug["health_tags"] = [h.value for h in health]
            # We do not inject these as TagCandidate here because your taxonomy may have a different root.
            # We keep them as debug for now and surface in final EnrichedMealVariant.health_tags.

        # Times
        if raw.total_time_mins is not None:
            times = self.ml_models.predict_prep_cook_time(ml_text)
            debug["times"] = {"prep": times.prep_time_mins, "cook": times.cook_time_mins}

        return candidates, current_course, current_diet, current_spice_level, current_region_path, debug

    def _llm_candidates(self, llm_res) -> List[TagCandidate]:
        tags: List[TagCandidate] = []
        for eq in llm_res.equipment or []:
            tags.append(TagCandidate(tag_type="equipment", value=str(eq), label_en=str(eq), confidence=0.70, is_primary=False, source="layer2_llm"))
        for tech in llm_res.techniques or []:
            tags.append(TagCandidate(tag_type="technique", value=str(tech), label_en=str(tech), confidence=0.70, is_primary=False, source="layer2_llm"))
        for h in llm_res.health_tags or []:
            tags.append(TagCandidate(tag_type="health", value=str(h), label_en=str(h), confidence=0.70, is_primary=False, source="layer2_llm"))
        for o in llm_res.occasion_tags or []:
            tags.append(TagCandidate(tag_type="occasion", value=str(o), label_en=str(o), confidence=0.70, is_primary=False, source="layer2_llm"))
        return tags

    def _embedding_text(
        self,
        title: str,
        ingredients: str,
        instructions: str,
        course: Optional[str],
        diet: Optional[str],
        region_path: List[str],
    ) -> str:
        parts = [
            title or "",
            " ".join(region_path or []),
            course or "",
            diet or "",
            ingredients or "",
            instructions or "",
        ]
        # Keep it short-ish; embeddings don't need full instructions.
        txt = "\n".join(parts)
        # Hard truncate to keep cheap
        return txt[:5000]
