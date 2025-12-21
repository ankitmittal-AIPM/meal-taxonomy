# src/meal_taxonomy/enrichment/llm_enrichment.py
from __future__ import annotations

"""
llm_enrichment.py

Purpose:
    Layer-2 semantic enrichment using an LLM.

This layer is meant for things that are hard to label reliably with classic datasets:
  - alternate names (spellings + multiple Indian languages + colloquial),
  - short, brand-voice description,
  - "goes well with" pairings,
  - subtle tags like kids-friendly, festive, satvik, etc.

It is OPTIONAL and should be toggleable.

Implementation notes:
  - This module is written for OpenAI's Python SDK (optional dependency).
  - If openai is not installed or OPENAI_API_KEY is missing, the enricher disables itself.
  - The enrichment pipeline should treat LLM outputs as *suggestions* and merge them carefully.
"""

import os
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("llm_enrichment")


def _safe_import_openai():
    try:
        from openai import OpenAI  # type: ignore
    except Exception:  # noqa: BLE001
        return None
    return OpenAI


@dataclass
class LLMEnrichmentResult:
    canonical_name: Optional[str]
    alt_names: List[Dict[str, str]]  # [{"name": "...", "language_code": "en"}]
    description: Optional[str]
    region_path: List[str]
    meal_type: Optional[str]
    diet: Optional[str]
    spice_level: Optional[int]
    kids_friendly: Optional[bool]
    health_tags: List[str]
    occasion_tags: List[str]
    equipment: List[str]
    techniques: List[str]
    prep_time_mins: Optional[float]
    cook_time_mins: Optional[float]
    extra: Dict[str, Any]


class MealLLMEnricher:
    def __init__(self, model: Optional[str] = None) -> None:
        OpenAI = _safe_import_openai()
        api_key = os.getenv("OPENAI_API_KEY")

        if OpenAI is None or not api_key:
            self.client = None
            self.model = None
            return

        self.client = OpenAI(api_key=api_key)
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    def enabled(self) -> bool:
        return self.client is not None and self.model is not None

    def enrich(
        self,
        *,
        title: str,
        ingredients: str,
        instructions: str,
        coarse: Dict[str, Any],
    ) -> Optional[LLMEnrichmentResult]:
        if not self.enabled():
            return None

        system = (
            "You are an expert Indian food taxonomist. "
            "Given a recipe, produce structured enrichment suitable for a meal database. "
            "Be conservative: do NOT hallucinate ingredients. "
            "If unsure, leave fields empty."
        )

        # Keep prompt concise for speed/cost; send coarse predictions to guide.
        user = {
            "task": "enrich_indian_meal",
            "input": {
                "title": title,
                "ingredients_text": ingredients,
                "instructions_text": instructions,
                "coarse_predictions": coarse,
            },
            "output_schema": {
                "canonical_name": "string or null",
                "alt_names": [{"name": "string", "language_code": "en|hi|kn|ta|te|ml|mr|gu|bn|pa|or|as|ur|hinglish|other"}],
                "description": "string or null (1-2 sentences, brand-voice, neutral)",
                "region_path": ["string"],
                "meal_type": "breakfast|tiffin|lunch|dinner|snack|dessert|beverage|null",
                "diet": "vegetarian|non_vegetarian|eggetarian|vegan|jain|no_onion_garlic|null",
                "spice_level": "integer 1-5 or null",
                "kids_friendly": "boolean or null",
                "health_tags": ["diabetic_friendly|high_fiber|high_protein|satvik|festive|low_oil|..."],
                "occasion_tags": ["diwali|navratri|kids_lunchbox|party|..."],
                "equipment": ["pressure_cooker|kadai|tawa|idli_stand|oven|mixer_grinder|..."],
                "techniques": ["steamed|fried|baked|tempering|tawa_roasted|pressure_cooked|..."],
                "prep_time_mins": "number or null",
                "cook_time_mins": "number or null",
                "extra": "object"
            },
            "constraints": [
                "Prefer Indian language/romanization variants in alt_names when relevant.",
                "If the dish is clearly regional (e.g., Udupi), give region_path as hierarchy (e.g., South Indian, Karnataka, Udupi).",
                "Do not output duplicates in alt_names.",
            ],
        }

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
            )
            raw = resp.choices[0].message.content or "{}"
            data = json.loads(raw)

            return LLMEnrichmentResult(
                canonical_name=data.get("canonical_name"),
                alt_names=data.get("alt_names") or [],
                description=data.get("description"),
                region_path=data.get("region_path") or [],
                meal_type=data.get("meal_type"),
                diet=data.get("diet"),
                spice_level=data.get("spice_level"),
                kids_friendly=data.get("kids_friendly"),
                health_tags=data.get("health_tags") or [],
                occasion_tags=data.get("occasion_tags") or [],
                equipment=data.get("equipment") or [],
                techniques=data.get("techniques") or [],
                prep_time_mins=data.get("prep_time_mins"),
                cook_time_mins=data.get("cook_time_mins"),
                extra=data.get("extra") or {},
            )

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "LLM enrichment failed: %s",
                exc,
                extra={
                    "invoking_func": "MealLLMEnricher.enrich",
                    "invoking_purpose": "Layer-2 semantic enrichment",
                    "next_step": "Continue without LLM; ensure API key/model are configured",
                    "resolution": "",
                },
            )
            return None
