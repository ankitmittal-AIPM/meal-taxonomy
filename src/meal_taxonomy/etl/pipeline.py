from __future__ import annotations
"""
ETL pipeline for meals: Main function called to perform below operations is ingest_recipe
1. Upsert meal
2. Upsert ingredients + meal_ingredients
3. Build/Extract tags from:
   a. dataset metadata
   b. NLP (NER + time buckets)
   c. ontology mappings
4. Attach tags via meal_tags

Tag Flow in pipeline:
a. generate_dataset_tag_candidates creates tags from metadata (diet, region, course, flavor_profile + Indian contexts).
b. generate_tag_candidates_for_recipe adds NLP-based tags.
c. merge_tag_candidates dedupes by (tag, source) with max confidence and then attach_tags_to_recipe writes into recipe_tags.

Logging in loops:
a. Info-level logging is only per-100 recipes in ETL.Pipeline.ingest_dataset, which is good for large runs.
b. Per-recipe and per-ingredient logging is at DEBUG level, so it won’t flood logs in normal production. Good.
"""

import logging
from typing import Dict, List, Optional
from supabase import Client

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.taxonomy.taxonomy_seed import ensure_tag_type, ensure_tag
from src.meal_taxonomy.datasets.base import RecipeRecord
from src.meal_taxonomy.datasets.indian_kaggle import load_indian_kaggle_csv
from src.meal_taxonomy.nlp_tagging import RecipeNLP, TagCandidate
from src.meal_taxonomy.logging_utils import get_logger

# Newly Added: A helper that converts a RecipeRecord into RawMeal, then runs enrichment + upsert:
from src.meal_taxonomy.brain.schema import RawMeal
from src.meal_taxonomy.enrichment.enrichment_pipeline import MealEnrichmentPipeline
from src.meal_taxonomy.enrichment.cleaning import normalize_title
from src.meal_taxonomy.brain.upsert_meal import upsert_meal as upsert_canonical_meal

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

# Class MealETL Started --->
class MealETL:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.nlp = RecipeNLP()
        self.enrichment = MealEnrichmentPipeline(use_llm=False)  # toggle later
        # Cache tag types and tags by (name) and (tag_type_id, value)
        self.tag_type_cache: Dict[str, int] = {}
        self.tag_cache: Dict[tuple[int, str], str] = {}
        
        # Cache ingredients by lower(name)
        self.ingredient_cache: Dict[str, str] = {}
        
    # -----------------------------------------------------
    # Safe bulk upsert with fallback
    # Tries bulk .upsert(rows)
    # If the exception message mentions “parallel”, “multiple”, or “bulk”, 
    # falls back to inserting one row at a time (no schema changes, just safer behavior)
    # -----------------------------------------------------
    def _safe_bulk_upsert(self, table: str, rows: list[dict]) -> None:
        """
        Try bulk upsert, but fall back to row-by-row upsert if the Supabase
        instance doesn't support multi-row / parallel inserts.

        This avoids repeated noisy errors like "multi parallel insert not
        supported" and keeps logs manageable.
        """
        if not rows:
            return

        try:
            # Preferred: single bulk upsert
            self.client.table(table).upsert(rows).execute()
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if "parallel" in msg or "multiple" in msg or "bulk" in msg:
                # Many Supabase setups allow big inserts but can fail if huge.
                # We'll chunk it just in case.
                try:
                    chunk_size = 500    # To Do : iterate to see the best chunk size
                    for i in range(0, len(rows), chunk_size):
                        chunk = rows[i : i + chunk_size]
                        self.client.table(table).upsert(chunk).execute()
                # Exception inside exception for bulk upload
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc).lower()
                    if "parallel" in msg or "multiple" in msg or "bulk" in msg:
                    # Conservative fallback: one row at a time
                        for row in rows:
                            self.client.table(table).upsert(row).execute()
            else:
                # Not the known bulk-insert limitation -> bubble up
                raise
    
    # -----------------------------------------------------
    # Tag helpers
    # -----------------------------------------------------
    def get_tag_type_id(self, name: str) -> str:
        if name in self.tag_type_cache:
            return self.tag_type_cache[name]

        tt_id = ensure_tag_type(
            self.client,
            name=name,
            label_en=name.replace("_", " ").title(),
        )
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
        # Preparing payload to be upserted in Supabase
        # TO DO: check if this insert the data or prepare payload at record level or batch level
        payload = {
            "title": rec.title,
            "title_normalized": normalize_title(rec.title),
            "description": rec.description,
            "instructions": rec.instructions,
            "source": "dataset",
            "external_source": rec.source,
            "external_id": rec.external_id,
            "language_code": rec.language_code,
            "cook_time_minutes": rec.cook_time_minutes,
            "prep_time_minutes": rec.prep_time_minutes,
            "total_time_minutes": (rec.cook_time_minutes or 0) + (rec.prep_time_minutes or 0),
            "servings": rec.meta.get("servings"),
            # ✅ Store full dataset metadata for the Recipe Record for later ontology / debugging use
            "meta": rec.meta or {},
        }

        # Perform actual DB upsert operation to enter data in Meals DB in Supabase
        res = self.client.table("meals").upsert(
            payload, on_conflict="external_source,external_id"
        ).execute()

        if res.data:
            return res.data[0]["id"]

        # Fallback lookup for upsert in Meal DB
        res = (
            self.client.table("meals")
            .select("id")
            .eq("external_source", rec.source)
            .eq("external_id", rec.external_id)
            .execute()
        )
        if not res.data:
            raise RuntimeError(f"Upsert meal failed for {rec.source}:{rec.external_id}")

        return res.data[0]["id"]

    # Invoked Address : From ingest_recipe post meal id is generated against (new or existing) single meal
    # Attaches the ingredient to the meal in Meal Db in Supabase
    # A meal can have list of ingredient sperated by comma or other ways
    def attach_ingredients(self, meal_id: str, ingredients: List[str]) -> None:
        rows = []
        for line in ingredients:
            clean = line.strip()
            if not clean:
                continue

            ing_id = self.get_or_create_ingredient(clean)
            # Preparing rows of ingredient to be added (i.e. to be attached) to a meal
            rows.append(
                {
                    "meal_id": meal_id,
                    "ingredient_id": ing_id,
                    "raw_text": clean,
                }
            )

        # Use the generic safe bulk upsert helper
        # Attaching ingredient means Adding ingredients to the meal in the meal_ingredient table
        if rows:
            self._safe_bulk_upsert("meal_ingredients", rows)

    # Invoked Address : From attach_ingredients
    # This basically checks any ingredient already available in the ingredient table otherwise add one
    # Ingredient table populates through meals addition where ingredient is listed, 
    # searched in ingredient table and if not found then added as new record
    def get_or_create_ingredient(self, name_en: str) -> str:
        name_en = (name_en or "").strip()
        if not name_en:
            raise ValueError("Ingredient name empty")
        if name_en in self.ingredient_cache:
            return self.ingredient_cache[name_en]
        
        # Search if ingredient already exists if so gets ingredient id and return back to calling function
        res = (
            self.client.table("ingredients")
            .select("id")
            .eq("name_en", name_en)
            .execute()
        )
        if res.data:
            return res.data[0]["id"]        
        
        # Fallback. Another way to search ingredient in the table. Just to double sure using "like"
        res = (
            self.client.table("ingredients")
            .select("id")
            .ilike("name_en", name_en)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0]["id"]

        # Add new ingredient if not found in previous step and returns id for new ingredient just added
        res = self.client.table("ingredients").insert(
            {"name_en": name_en}
        ).execute()
        
        ing_id = res.data[0]["id"]
        self.ingredient_cache[name_en] = ing_id
        return ing_id

    # -----------------------------------------------------
    # Tag extraction - Extracting/Building tags to a recipe record object
    # Invoked Address : called from ingest_recipe post ingredient and 
    # meals are added. Next step is to attach tags to meals but before that 
    # relevant tags are fetched which is to be associated
    # -----------------------------------------------------
    def dataset_tags(self, rec: RecipeRecord) -> List[TagCandidate]:
        meta = rec.meta or {}
        tags: List[TagCandidate] = []

        # Region
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

        # Cuisine
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

        # Diet
        diet = (meta.get("diet") or "").strip()
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

        # Flavor profile
        flavor = (meta.get("flavor") or "").strip()
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

        # Meal type or Course
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

    def nlp_tags(self, rec: RecipeRecord) -> List[TagCandidate]:
        extra_parts = [
            rec.title or "",
            rec.description or "",
            rec.instructions or "",
        ]
        extra_text = "\n".join(p for p in extra_parts if p.strip())
        return self.nlp.nlp_tags_for_recipe(rec.ingredients, extra_text=extra_text)

    # -----------------------------------------------------
    # Enrichment-derived tags (Meal Enrichment outputs)
    # These tags should NOT duplicate dataset_tags() or nlp_tags().
    # They are intended for:
    #   - difficulty
    #   - spice_level
    #   - kids_friendly
    #   - health tags / occasions / equipment (when you later turn on LLM/ML)
    # -----------------------------------------------------
    def enrichment_tags(self, enriched) -> List[TagCandidate]:
        tags: List[TagCandidate] = []

        def _val(x: str) -> str:
            return normalize_title(x).replace(" ", "_")

        # Spice level (1-5)
        if getattr(enriched, "spice_level", None) is not None:
            lvl = int(enriched.spice_level)
            tags.append(
                TagCandidate(
                    tag_type="spice_level",
                    value=f"level_{lvl}",
                    label_en=f"Spice level {lvl}",
                    confidence=0.7,
                )
            )

        # Difficulty
        if getattr(enriched, "difficulty", None):
            diff = str(enriched.difficulty)
            tags.append(
                TagCandidate(
                    tag_type="difficulty",
                    value=_val(diff),
                    label_en=diff.title(),
                    confidence=0.7,
                )
            )

        # Kids friendly
        if getattr(enriched, "kids_friendly", None) is not None:
            val = "kids_friendly" if bool(enriched.kids_friendly) else "not_kids_friendly"
            tags.append(
                TagCandidate(
                    tag_type="kids_friendly",
                    value=val,
                    label_en=val.replace("_", " ").title(),
                    confidence=0.7,
                )
            )

        # Occasion tags
        for occ in getattr(enriched, "occasion_tags", []) or []:
            tags.append(
                TagCandidate(
                    tag_type="occasion",
                    value=_val(str(occ)),
                    label_en=str(occ).title(),
                    confidence=0.6,
                )
            )

        # Health tags
        for ht in getattr(enriched, "health_tags", []) or []:
            tags.append(
                TagCandidate(
                    tag_type="health_tag",
                    value=_val(str(ht)),
                    label_en=str(ht).title(),
                    confidence=0.6,
                )
            )

        # Equipment / utensil tags
        for eq in getattr(enriched, "utensil_tags", []) or []:
            tags.append(
                TagCandidate(
                    tag_type="equipment",
                    value=_val(str(eq)),
                    label_en=str(eq).title(),
                    confidence=0.6,
                )
            )

        return tags

    # -----------------------------------------------------
    # Tag persistence - Attaching Tags to the Meals in Meal_Tag Db in Supabase
    # Invoked Address : Called from ingest_recipe post tags to a recipe in the recipe record objects are identified
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
                    "source": source
                }
            )
        if rows:
            # Use the generic safe bulk upsert helper
            self._safe_bulk_upsert("meal_tags", rows)

    # -----------------------------------------------------
    # Main recipe ingestion in Supabase DB
    # Invoke Address: Invoked from ingest_kaggle_all which is code to 
    # read all kaggle based datasource from data/kaggle folder and add 
    # records in those files in Meal DBs in Supabase
    # -----------------------------------------------------
    def ingest_recipe(self, rec: RecipeRecord, index: int | None = None, *, use_brain: bool = True) -> str:
        
        # Step 1 - Decide canonical meal id.
        # Default path uses Meal Brain (canonicalization + variants) so we can:
        #   - dedupe across sources,
        #   - keep provenance (variants),
        #   - attach tags/ingredients/ontology once on canonical meals.
        enriched = None
        variant_id = ""
        status = "legacy"
        if use_brain:
            raw = RawMeal(
                source_type=rec.source,
                source_id=str(rec.external_id),
                name=rec.title,
                description=rec.description,
                ingredients_text="\n".join(rec.ingredients or []),
                instructions_text=rec.instructions or "",
                cuisine=(rec.meta or {}).get("cuisine"),
                course=(rec.meta or {}).get("course"),
                diet=(rec.meta or {}).get("diet"),
                prep_time_mins=rec.prep_time_minutes,
                cook_time_mins=rec.cook_time_minutes,
                total_time_mins=(rec.cook_time_minutes or 0) + (rec.prep_time_minutes or 0),
                servings=(rec.meta or {}).get("servings"),
                extra=(rec.meta or {}),
            )
            enriched = self.enrichment.enrich(raw)
            meal_id, variant_id, status = upsert_canonical_meal(enriched, self.client)
        else:
            # Legacy behavior: each dataset row becomes a row in meals (no canonicalization).
            # Step 1 - Gets meal id for the upserted record in the Supabase 
            # after upsert_meal successfuly upsert the record in Meals Db in supabase
            meal_id = self.upsert_meal(rec)
        
        # Step 2 - Attaches ingredient to the meal in the Supabase DB
        self.attach_ingredients(meal_id, rec.ingredients)

        # Step 3.1 - Extract Tags for the data record in Reciperecord object passed (Hardcoded Tags)
        ds_tags = self.dataset_tags(rec)
        
        # Step 3.2 - Extract Tags for the data record in Reciperecord object passed (NLP Based Tags)
        nlp_tags = self.nlp_tags(rec)

        # Step 3.3 - Additional enrichment-derived tags (difficulty, spice, equipment, etc.)
        enrich_tags: List[TagCandidate] = []
        if enriched is not None:
            enrich_tags = self.enrichment_tags(enriched)

        # Step 4 - Attach Tags to the meal Id in Supabase - First hardcoded based and then NLP based
        self.attach_tags(meal_id, ds_tags, source="dataset")
        self.attach_tags(meal_id, nlp_tags, source="nlp")
        self.attach_tags(meal_id, enrich_tags, source="enrichment")

        # Log brain status for debugging (does not spam because per-recipe INFO is acceptable for ETL)
        if use_brain:
            logger.debug(
                "Meal Brain result for '%s': status=%s variant_id=%s",
                rec.title,
                status,
                variant_id,
                extra={
                    "invoking_func": "MealETL.ingest_recipe",
                    "invoking_purpose": MODULE_PURPOSE,
                    "next_step": "Continue tag + ingredient attachment",
                    "resolution": status,
                },
            )

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

    # Newly Added : New helper method on MealETL
    def ingest_recipe_record(self, record: RecipeRecord) -> None:
        """
        Convert RecipeRecord -> RawMeal -> EnrichedMealVariant -> upsert_meal.
        """
        raw = RawMeal(
            source_type=record.source,
            source_id=str(record.external_id or record.id),
            name=record.title,
            description=record.description or None,
            ingredients_text="\n".join(record.ingredients_text or []),
            instructions_text=record.instructions_text or "",
            cuisine=(record.meta or {}).get("cuisine"),
            course=(record.meta or {}).get("course"),
            diet=(record.meta or {}).get("diet"),
            prep_time_mins=record.prep_time_minutes,
            cook_time_mins=record.cook_time_minutes,
            total_time_mins=(record.cook_time_minutes or 0) + (record.prep_time_minutes or 0),
            servings=(record.meta or {}).get("servings"),
            extra={"dataset_name": record.meta.get("dataset_name")},
        )
        enriched = self.enrichment.enrich(raw)
        meal_id, variant_id, status = upsert_canonical_meal(enriched, self.client)

        logger.info(
            "MealETL.ingest_recipe_record completed; recipe='%s', meal_id=%s, variant_id=%s, status=%s",
            record.title,
            meal_id,
            variant_id,
            status,
            extra={
                "invoking_func": "MealETL.ingest_recipe_record",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "",
                "resolution": "",
            },
        )

# Class MealETL Ends --->

# ---------------------------------------------------------
# Batch ingestion
# TO DO : Duplicate Code. Similar ingestion existings in ingest_kaggle_all 
# just that over there it first reads csv passes through load_kaggle_csv for normalization
# ---------------------------------------------------------
def ingest_indian_kaggle(path: str) -> None:
    """
    Convenience function for CLI: load Kaggle file + ingest.
    """
    client = get_supabase_client()
    etl = MealETL(client)
    # TO DO: Check do we need this any more. Special function to convert 
    # weird manual recipes csv file to one readable by code before entering data in meal db
    recipes = load_indian_kaggle_csv(path)
    
    # Ready to ingest data in Meal DB
    logger.info(
        "Starting ingestion of %d recipes from %s",
        len(recipes),
        path,
        extra={
            "invoking_func": "ingest_indian_kaggle",
            "invoking_purpose": "Ingest Kaggle Indian recipes CSV",
            "next_step": "Iterate recipes and upsert into meals / ingredients / meal_ingredients / meal_tags",
            "resolution": "",
        },
    )

    max_consecutive_failures = 5
    consecutive_failures = 0
    # Invokes pipeline.py function ingest_recipe to upsert data in Meal DBs in Supabase
    # TO DO: This is similar to ingest_kaggle_all, see if we can purge this one
    for idx, rec in enumerate(recipes):
        try:
            # TO DO: If this calls ingest_recipe to upsert data at record level or batch level. 
            # TO DO: Record level is too slow look for method to insert at batch level
            etl.ingest_recipe(rec, index=idx)
            consecutive_failures = 0
        # Long code to silence consecutive errors logs in CLI
        except Exception as exc:  # noqa: BLE001
            consecutive_failures += 1

            extra = {
                "invoking_func": "ingest_indian_kaggle",
                "invoking_purpose": "Ingest Kaggle Indian recipes CSV",
                "next_step": "Inspect first failing record and Supabase response; fix schema or mapping; rerun ETL",
                "resolution": "",
            }

            if consecutive_failures == 1:
                # First failure: keep full traceback for debugging
                logger.error(
                    "Error ingesting recipe '%s' (external_id=%s)",
                    rec.title,
                    rec.external_id,
                    exc_info=True,
                    extra=extra,
                )
            else:
                # Subsequent failures: shorter log line, no stack trace
                logger.error(
                    "Error ingesting recipe '%s' (external_id=%s) "
                    "[consecutive failure %d]: %s",
                    rec.title,
                    rec.external_id,
                    consecutive_failures,
                    exc,
                    extra=extra,
                )

            if consecutive_failures >= max_consecutive_failures:
                logger.error(
                    "Aborting ingestion after %d consecutive failures. "
                    "This usually indicates a systemic issue (e.g. Supabase "
                    "connectivity or schema mismatch).",
                    max_consecutive_failures,
                    extra=extra,
                )
                break

    logger.info(
        "Finished ingestion of %d recipes from %s",
        len(recipes),
        path,
        extra={
            "invoking_func": "ingest_indian_kaggle",
            "invoking_purpose": "Ingest Kaggle Indian recipes CSV",
            "next_step": "Exit",
            "resolution": "",
        },
    )

if __name__ == "__main__":
    # If pipeline.py is directly ran then batch ingestion of data from indian_food.csv is done.
    # This is only place where batch ingestion is called for only one file
    # For rest single record by record ingestion done through pipeline.py called from different code files
    ingest_indian_kaggle("data/indian_food.csv")

