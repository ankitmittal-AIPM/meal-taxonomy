"""
ETL pipeline for meals:
1. Upsert meal
2. Upsert ingredients + meal_ingredients
3. Build tags from:
   a. dataset metadata
   b. NLP (NER + time buckets)
   c. ontology mappings
4. Attach tags via meal_tags
"""

from __future__ import annotations

import logging
from typing import Dict, List

from supabase import Client

from src.meal_taxonomy.config import get_supabase_client
from taxonomy.taxonomy_seed import ensure_tag_type, ensure_tag
from datasets.base import RecipeRecord
from datasets.indian_kaggle import load_indian_kaggle_csv
from meal_taxonomy.nlp_tagging import RecipeNLP, TagCandidate
from src.meal_taxonomy.logging_utils import get_logger

MODULE_PURPOSE = (
    "ETL pipeline that creates meals, ingredients and attaches tags "
    "for the Indian meal ontology / taxonomy."
)

"""Meal ETL pipeline orchestration.

This module coordinates ingestion of RecipeRecord objects into Supabase
and attaches ingredients and tags. Logging uses the shared structured
formatter from `src.meal_taxonomy.logging_utils`.
"""

# ---------------------------------------------------------
# LOGGING: use structured logger from logging_utils
# ---------------------------------------------------------
logger = get_logger("pipeline")

class MealETL:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.nlp = RecipeNLP()
        self.tag_type_cache: Dict[str, int] = {}
        self.tag_cache: Dict[tuple[int, str], str] = {}

    # -----------------------------------------------------
    # Tag helpers
    # -----------------------------------------------------
    def get_tag_type_id(self, name: str, description: str = "") -> int:
        if name in self.tag_type_cache:
            return self.tag_type_cache[name]

        tt_id = ensure_tag_type(self.client, name, description or name)
        self.tag_type_cache[name] = tt_id
        return tt_id

    def get_or_create_tag(self, candidate: TagCandidate) -> str:
        tt_id = self.get_tag_type_id(candidate.tag_type)
        key = (tt_id, candidate.value)

        if key in self.tag_cache:
            return self.tag_cache[key]

        tag_id = ensure_tag(
            self.client,
            tag_type_id=tt_id,
            value=candidate.value,
            label_en=candidate.label_en,
            label_hi=candidate.label_hi,
            label_hinglish=candidate.label_hinglish,
        )

        self.tag_cache[key] = tag_id
        return tag_id

    # -----------------------------------------------------
    # Persistence: meals, ingredients
    # -----------------------------------------------------
    def upsert_meal(self, rec: RecipeRecord) -> str:
        payload = {
            "title": rec.title,
            "description": rec.description,
            "instructions": rec.instructions,
            "source": "dataset",
            "external_source": rec.source,
            "external_id": rec.external_id,
            "language_code": rec.language_code,
            "cook_time_minutes": rec.cook_time_minutes,
            "prep_time_minutes": rec.prep_time_minutes,
            "servings": rec.meta.get("servings"),
        }

        res = self.client.table("meals").upsert(
            payload, on_conflict="external_source,external_id"
        ).execute()

        if res.data:
            return res.data[0]["id"]

        # Fallback lookup
        res = (
            self.client.table("meals")
            .select("id")
            .eq("external_source", rec.source)
            .eq("external_id", rec.external_id)
            .execute()
        )
        return res.data[0]["id"]

    def get_or_create_ingredient(self, name_en: str) -> str:
        name_en = name_en.strip()

        res = (
            self.client.table("ingredients")
            .select("id")
            .ilike("name_en", name_en)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0]["id"]

        res = self.client.table("ingredients").insert(
            {"name_en": name_en}
        ).execute()

        if res.data:
            return res.data[0]["id"]

        # Fallback
        res = (
            self.client.table("ingredients")
            .select("id")
            .eq("name_en", name_en)
            .execute()
        )
        return res.data[0]["id"]

    def attach_ingredients(self, meal_id: str, ingredients: List[str]) -> None:
        rows = []
        for line in ingredients:
            clean = line.strip()
            if not clean:
                continue

            ing_id = self.get_or_create_ingredient(clean)
            rows.append(
                {
                    "meal_id": meal_id,
                    "ingredient_id": ing_id,
                    "raw_text": clean,
                }
            )

        if rows:
            self.client.table("meal_ingredients").upsert(rows).execute()

    # -----------------------------------------------------
    # Tag extraction
    # -----------------------------------------------------
    def dataset_tags(self, rec: RecipeRecord) -> List[TagCandidate]:
        meta = rec.meta or {}
        tags: List[TagCandidate] = []

        region = (meta.get("region") or "").strip()
        if region:
            tags.append(
                TagCandidate(
                    tag_type="cuisine_region",
                    value=region.lower().replace(" ", "_"),
                    label_en=region.title(),
                    is_primary=True,
                    confidence=1.0,
                )
            )

        cuisine = (meta.get("cuisine") or "").strip()
        if cuisine:
            tags.append(
                TagCandidate(
                    tag_type="cuisine_national",
                    value=cuisine.lower().replace(" ", "_"),
                    label_en=cuisine.title(),
                    confidence=1.0,
                )
            )

        diet = meta.get("diet")
        if diet:
            d = str(diet).strip()
            if d:
                tags.append(
                    TagCandidate(
                        tag_type="diet",
                        value=d.lower().replace(" ", "_"),
                        label_en=d.title(),
                        confidence=1.0,
                    )
                )

        flavor = meta.get("flavor")
        if flavor:
            f = str(flavor).strip()
            if f:
                tags.append(
                    TagCandidate(
                        tag_type="taste_profile",
                        value=f.lower().replace(" ", "_"),
                        label_en=f.title(),
                        confidence=0.8,
                    )
                )

        # Meal type
        meal_type = meta.get("course")
        if not meal_type:
            t = rec.title.lower()
            if any(x in t for x in ["breakfast", "idli", "dosa", "poha"]):
                meal_type = "breakfast"

        if meal_type:
            tags.append(
                TagCandidate(
                    tag_type="meal_type",
                    value=str(meal_type).lower().replace(" ", "_"),
                    label_en=str(meal_type).title(),
                    confidence=0.9,
                    is_primary=True,
                )
            )

        # Time bucket
        total_time = (rec.cook_time_minutes or 0) + (rec.prep_time_minutes or 0)
        time_tag = self.nlp.bucket_time(total_time)
        if time_tag:
            tags.append(time_tag)

        return tags

    #def nlp_tags(self, rec: RecipeRecord) -> List[TagCandidate]:
     #   return self.nlp.nlp_tags_for_recipe(rec.ingredients)

    def nlp_tags(self, rec: RecipeRecord) -> List[TagCandidate]:
        extra_parts = [
            rec.title or "",
            rec.description or "",
            rec.instructions or "",
        ]
        extra_text = "\n".join(p for p in extra_parts if p.strip())
        return self.nlp.nlp_tags_for_recipe(rec.ingredients, extra_text=extra_text)

    # -----------------------------------------------------
    # Tag persistence
    # -----------------------------------------------------
    def attach_tags(self, meal_id: str, candidates: List[TagCandidate], source: str) -> None:
        rows = []
        for cand in candidates:
            tag_id = self.get_or_create_tag(cand)
            rows.append(
                {
                    "meal_id": meal_id,
                    "tag_id": tag_id,
                    "confidence": cand.confidence,
                    "is_primary": cand.is_primary,
                    "source": source,
                }
            )
        if rows:
            self.client.table("meal_tags").upsert(rows).execute()

    # -----------------------------------------------------
    # Main recipe ingestion
    # -----------------------------------------------------
    def ingest_recipe(self, rec: RecipeRecord, index: int | None = None) -> str:
        meal_id = self.upsert_meal(rec)
        self.attach_ingredients(meal_id, rec.ingredients)

        ds_tags = self.dataset_tags(rec)
        nlp_tags = self.nlp_tags(rec)

        self.attach_tags(meal_id, ds_tags, source="dataset")
        self.attach_tags(meal_id, nlp_tags, source="nlp")

        # Milestone logging: every 50 rows
        if index is not None and index % 50 == 0:
            logger.warning(
                "Milestone: %d recipes ingested. Last='%s'",
                index,
                rec.title,
                extra={
                    "invoking_func": "ingest_indian_kaggle",
                    "invoking_purpose": "Batch ingest a single Kaggle-style CSV file into Supabase",
                    "next_step": "Continue ingestion loop with next recipe",
                    "resolution": "",
                },
            )

        return meal_id


# ---------------------------------------------------------
# Batch ingestion
# ---------------------------------------------------------
def ingest_indian_kaggle(path: str) -> None:
    client = get_supabase_client()
    etl = MealETL(client)
    recipes = load_indian_kaggle_csv(path)
    # Limiting to first 20 for testing only
    #recipes = recipes[:20]  # limit for testing for first 20 recipes

    #logger.warning(f"Starting ingestion of {len(recipes)} recipes...")
    
    logger.info(
        "Starting ingestion of %d recipes from '%s'",
        len(recipes),
        path,
        extra={
            "invoking_func": "ingest_indian_kaggle",
            "invoking_purpose": "Batch ingest legacy Indian Kaggle CSV via MealETL",
            "next_step": "Loop over RecipeRecord objects and ingest them",
            "resolution": "",
        },
    )

    for idx, rec in enumerate(recipes):
        try:
            etl.ingest_recipe(rec, index=idx)
        except Exception as exc:
            # Only errors go to logs
            #logger.error(
            #    f"Failed to ingest recipe '{rec.title}' (ID={rec.external_id}): {exc}",
            #    exc_info=True,
            #)
            # New Log structure
            logger.error(
                "Failed to ingest recipe '%s' (external_id=%s): %s",
                rec.title,
                rec.external_id,
                exc,
                extra={
                    "invoking_func": "ingest_indian_kaggle",
                    "invoking_purpose": "Batch ingest legacy Indian Kaggle CSV via MealETL",
                    "next_step": "Skip this recipe and continue with next one",
                    "resolution": "Inspect this recipe row and Supabase constraints; fix data or schema and rerun if needed",
                },
                exc_info=True,
            )

    #logger.warning("Ingestion completed successfully.")
    # New logs structure
    logger.info(
        "Ingestion completed for %d recipes from '%s'",
        len(recipes),
        path,
        extra={
            "invoking_func": "ingest_indian_kaggle",
            "invoking_purpose": "Batch ingest legacy Indian Kaggle CSV via MealETL",
            "next_step": "Exit script",
            "resolution": "",
        },
    )


if __name__ == "__main__":
    ingest_indian_kaggle("data/indian_food.csv")
