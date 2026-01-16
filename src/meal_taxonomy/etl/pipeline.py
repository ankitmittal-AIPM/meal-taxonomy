from __future__ import annotations
"""
pipeline.py

Purpose:
    High-level orchestration that:
      - loads source recipe records
      - generates tags via dataset metadata + NLP heuristics
      - upserts meals + ingredients into Supabase
      - writes meal_tags + meal_ingredients join rows

Key responsibilities:
    - MealETL class:
        * ingest_recipe(record): full ingest for one record
        * ingest_indian_kaggle(path): end-to-end ingest for Indian Food dataset

Tag flow:
    1) dataset metadata tags from record.meta:
       - meal_type, diet, region
    2) NLP tag candidates from name + ingredients + instructions
    3) merge_tag_candidates to dedupe and prefer high-confidence candidates
    4) create tags/tag_types if missing
    5) insert into meal_tags for each meal
"""

import re
import time
from dataclasses import asdict, is_dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from supabase import Client

from src.meal_taxonomy.brain.schema import RawMeal
from src.meal_taxonomy.brain.upsert_meal import upsert_meal
from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.datasets.base import RecipeRecord
from src.meal_taxonomy.datasets.indian_kaggle import load_indian_kaggle_csv
from src.meal_taxonomy.enrichment.cleaning import normalize_title, split_ingredient_lines
from src.meal_taxonomy.enrichment.enrichment_pipeline import MealEnrichmentConfig, MealEnrichmentPipeline
from src.meal_taxonomy.logging_utils import get_logger
from src.meal_taxonomy.nlp_tagging import TagCandidate
from src.meal_taxonomy.taxonomy.taxonomy_seed import TAG_TYPES, ensure_tag, ensure_tag_type

# --------------------------------------------------------------------------
# Legacy comments preserved verbatim (do not delete; helpful context/intent)
# --------------------------------------------------------------------------
# Newly Added: A helper that converts a RecipeRecord into RawMeal, then runs enrichment + upsert:
# ---------------------------------------------------------
# LOGGING: use structured logger from logging_utils
# ---------------------------------------------------------
# Class MealETL Started --->
# Cache tag types and tags by (name) and (tag_type_id, value)
# Cache ingredients by lower(name)
# -----------------------------------------------------
# Safe bulk upsert with fallback
# Tries bulk .upsert(rows)
# If the exception message mentions “parallel”, “multiple”, or “bulk”, 
# falls back to inserting one row at a time (no schema changes, just safer behavior)
# -----------------------------------------------------
# Preferred: single bulk upsert
# Many Supabase setups allow big inserts but can fail if huge.
# We'll chunk it just in case.
# Exception inside exception for bulk upload
# Conservative fallback: one row at a time
# Not the known bulk-insert limitation -> bubble up
# -----------------------------------------------------
# Tag helpers
# -----------------------------------------------------
# -----------------------------------------------------
# Persistence: meals, ingredients
# -----------------------------------------------------
# Preparing payload to be upserted in Supabase
# TO DO: check if this insert the data or prepare payload at record level or batch level
# ✅ Store full dataset metadata for the Recipe Record for later ontology / debugging use
# Perform actual DB upsert operation to enter data in Meals DB in Supabase
# Fallback lookup for upsert in Meal DB

logger = get_logger(__name__)

def merge_tag_candidates(candidates: List[TagCandidate]) -> List[TagCandidate]:
    """
    Deduplicate TagCandidate list by (tag_type, value) while keeping the best score.

    Policy:
      - keep the max confidence
      - if any candidate is_primary=True for the key, keep is_primary=True
      - prefer non-empty labels where available
      - preserve the *first* non-empty source (but does not affect ranking)

    Returns:
        List[TagCandidate] (deduped)
    """
    best: Dict[Tuple[str, str], TagCandidate] = {}
    for c in candidates or []:
        key = (str(c.tag_type).strip().lower(), str(c.value).strip().lower())
        if not key[0] or not key[1]:
            continue

        prev = best.get(key)
        if prev is None:
            best[key] = c
            continue

        # Choose higher confidence; if tie, keep the one marked primary.
        if (c.confidence or 0) > (prev.confidence or 0):
            best[key] = c
            continue
        if (c.confidence or 0) == (prev.confidence or 0) and c.is_primary and not prev.is_primary:
            best[key] = c
            continue

        # Merge labels / primary flag into existing winner
        if not prev.label_en and c.label_en:
            prev.label_en = c.label_en
        if not prev.label_hi and c.label_hi:
            prev.label_hi = c.label_hi
        if not prev.label_hinglish and c.label_hinglish:
            prev.label_hinglish = c.label_hinglish
        if c.is_primary:
            prev.is_primary = True
        if not prev.source and c.source:
            prev.source = c.source

    return list(best.values())


class MealETL:
    """
    Orchestrates end-to-end ingestion into Supabase.

    Main entrypoints:
      - ingest_recipe(record): dataset record -> RawMeal -> EnrichedMealVariant -> canonical/variant upsert
      - ingest_records(records): batch helper
    """

    def __init__(
        self,
        client: Client,
        *,
        use_llm: bool = False,
        use_embeddings: bool = False,
        use_ml: bool = True,
    ) -> None:
        self.client = client

        self.enricher = MealEnrichmentPipeline(
            config=MealEnrichmentConfig(
                enable_layer1_ml=use_ml,
                enable_layer2_llm=use_llm,
                enable_embeddings=use_embeddings,
            )
        )

        # Cache tag types and tags by (name) and (tag_type_id, value)
        self.tag_type_cache: Dict[str, int] = {}
        self.tag_cache: Dict[Tuple[int, str], str] = {}

        # Cache ingredients by lower(name)
        self.ingredient_cache: Dict[str, str] = {}

    # -----------------------------------------------------
    # Safe bulk upsert with fallback
    # Tries bulk .upsert(rows)
    # If the exception message mentions “parallel”, “multiple”, or “bulk”, 
    # falls back to inserting one row at a time (no schema changes, just safer behavior)
    # -----------------------------------------------------
    def _safe_bulk_upsert(self, table: str, rows: List[dict], *, on_conflict: Optional[str] = None) -> None:
        if not rows:
            return

        # Many Supabase setups allow big inserts but can fail if huge.
        # We'll chunk it just in case.
        chunk_size = 500

        def _do_chunk(chunk: List[dict]) -> None:
            if on_conflict:
                self.client.table(table).upsert(chunk, on_conflict=on_conflict).execute()
            else:
                self.client.table(table).upsert(chunk).execute()

        try:
            # Preferred: single bulk upsert (chunked)
            for i in range(0, len(rows), chunk_size):
                _do_chunk(rows[i : i + chunk_size])
            return
        except Exception as e:  # pragma: no cover
            msg = str(e).lower()
            if any(k in msg for k in ["parallel", "multiple", "bulk"]):
                # Conservative fallback: one row at a time
                for row in rows:
                    _do_chunk([row])
                return
            # Not the known bulk-insert limitation -> bubble up
            raise

    # -----------------------------------------------------
    # Tag helpers
    # -----------------------------------------------------
    def get_tag_type_id(self, tag_type_name: str) -> int:
        key = (tag_type_name or "").strip().lower()
        if not key:
            raise ValueError("tag_type_name cannot be empty")

        if key in self.tag_type_cache:
            return self.tag_type_cache[key]

        description = TAG_TYPES.get(key, f"Auto-created tag type: {key}")
        tag_type_id = ensure_tag_type(self.client, key, description)
        self.tag_type_cache[key] = tag_type_id
        return tag_type_id

    def get_or_create_tag(self, c: TagCandidate) -> str:
        tag_type_id = self.get_tag_type_id(c.tag_type)
        key = (tag_type_id, (c.value or "").strip().lower())
        if key in self.tag_cache:
            return self.tag_cache[key]

        tag_id = ensure_tag(
            self.client,
            tag_type_id=tag_type_id,
            value=(c.value or "").strip(),
            label_en=(c.label_en or (c.value or "").strip()),
            label_hi=c.label_hi,
            label_hinglish=c.label_hinglish,
            parent_id=None,
        )
        self.tag_cache[key] = tag_id
        return tag_id

    # -----------------------------------------------------
    # Ingredient helpers
    # -----------------------------------------------------
    def get_or_create_ingredient(self, ingredient_name: str, *, language_code: str = "en") -> Optional[str]:
        name = (ingredient_name or "").strip()
        if not name:
            return None

        cache_key = name.lower()
        if cache_key in self.ingredient_cache:
            return self.ingredient_cache[cache_key]

        # Fast path (exact match). If the DB has a unique index on lower(name_en),
        # this is efficient and stable.
        try:
            res = (
                self.client.table("ingredients")
                .select("id")
                .eq("name_en", name)
                .limit(1)
                .execute()
            )
            if res.data:
                ing_id = res.data[0]["id"]
                self.ingredient_cache[cache_key] = ing_id
                return ing_id
        except Exception:
            # ignore and fallback to ilike
            pass

        res2 = (
            self.client.table("ingredients")
            .select("id")
            .ilike("name_en", name) # Removed wild card i.e.  f"%{name}%" for name in like clause
            .limit(1)
            .execute()
        )
        if res2.data:
            ing_id = res2.data[0]["id"]
            self.ingredient_cache[cache_key] = ing_id
            return ing_id

        # Building payload to insert in Ingredient Table of Supabase
        # To Do : Language to be made dyanmic See this link for fix - https://chatgpt.com/s/t_694ccc90cdb4819183785faffc3cd36c
        payload = {
            "name_en": name,
            "metadata": {
                "name_normalized": normalize_title(name),
                "language_code": language_code,   # keep as metadata if you still want it
            },
        }
        inserted = self.client.table("ingredients").insert(payload).execute()
        ing_id = inserted.data[0]["id"]
        self.ingredient_cache[cache_key] = ing_id
        return ing_id

    # -----------------------------------------------------
    # Persistence helpers
    # -----------------------------------------------------
    def attach_ingredients(self, meal_id: str, ingredients_text: str) -> None:
        lines = split_ingredient_lines(ingredients_text or "")
        rows: List[dict] = []
        # Get ingredient id from Ingredient table in supabase. Get means "fetch if there or create one in the table"
        for raw_line in lines:
            ing_id = self.get_or_create_ingredient(raw_line)
            if not ing_id:
                continue
            rows.append(
                {
                    "meal_id": meal_id,
                    "ingredient_id": ing_id,
                    "raw_text": raw_line,
                    "quantity": None,
                    "unit": None,
                    "metadata": {},
                }
            )
        self._safe_bulk_upsert("meal_ingredients", rows, on_conflict="meal_id,ingredient_id")

    def attach_tags(self, meal_id: str, candidates: List[TagCandidate]) -> None:
        merged = merge_tag_candidates(candidates)

        rows: List[dict] = []
        for c in merged:
            try:
                tag_id = self.get_or_create_tag(c)
            except Exception as e:
                logger.warning("tag_create_failed", extra={"tag_type": c.tag_type, "value": c.value, "err": str(e)})
                continue

            rows.append(
                {
                    "meal_id": meal_id,
                    "tag_id": tag_id,
                    "confidence": float(c.confidence or 0),
                    "is_primary": bool(c.is_primary),
                    "source": c.source or "etl",
                }
            )

        self._safe_bulk_upsert("meal_tags", rows, on_conflict="meal_id,tag_id")

    def refresh_search_doc(self, meal_id: str) -> None:
        """
        Rebuild meals.search_text (and therefore meals.search_tsv via trigger) for a given meal.

        This depends on the SQL function `public.refresh_meal_search_doc(target_meal_id uuid)`.
        If the RPC is not present yet, this becomes a no-op with a warning.
        """
        try:
            self.client.rpc("refresh_meal_search_doc", {"target_meal_id": meal_id}).execute()
        except Exception as e:  # pragma: no cover
            logger.warning("refresh_search_doc_failed", extra={"meal_id": meal_id, "err": str(e)})

    # -----------------------------------------------------
    # End-to-end ingest
    # Invoked Address : ingest_records within this file, once records are read from csv/external files on meals
    # Converts each record in datatable of recipe record to Rawmeal
    # 
    # -----------------------------------------------------
    def ingest_recipe(self, record: RecipeRecord, *, refresh_search: bool = True) -> dict:
        """
        Full ingest for one record:
          - RecipeRecord -> RawMeal
          - enrichment -> EnrichedMealVariant
          - upsert_meal (canonical + variant) via Meal Brain
          - attach tags + ingredients to canonical meal
          - refresh search doc

        Returns:
            dict with meal_id, variant_id, status
        """
        # Convert RecipeRecord to RawMeal
        total_time = None
        if record.prep_time_minutes is not None or record.cook_time_minutes is not None:
            total_time = (record.prep_time_minutes or 0) + (record.cook_time_minutes or 0)

        # Convert RecipeRecord taken from external data source to RawMeal
        # Create RawMeal from the record i.e. data retrived from CSV file
        raw = RawMeal(
            source_type=record.source,
            source_id=record.external_id,
            name=record.title,
            description=record.description,
            ingredients_text="\n".join(record.ingredients or []),
            instructions_text=record.instructions or "",
            cuisine=(record.meta or {}).get("cuisine"),
            course=(record.meta or {}).get("course"),
            diet=(record.meta or {}).get("diet"),
            prep_time_mins=float(record.prep_time_minutes) if record.prep_time_minutes is not None else None,
            cook_time_mins=float(record.cook_time_minutes) if record.cook_time_minutes is not None else None,
            total_time_mins=float(total_time) if total_time is not None else None,
            servings=None,
            extra=dict(record.meta or {}),
        )

        # Enrich RawMeal and include more details to the meal
        enriched = self.enricher.enrich(raw)

        # Upsert canonical + variant
        meal_id, variant_id, status = upsert_meal(enriched, client=self.client)

        # Attach ingredients/tags to canonical meal row
        self.attach_ingredients(meal_id, enriched.ingredients_norm)
        self.attach_tags(meal_id, enriched.tag_candidates)

        if refresh_search:
            self.refresh_search_doc(meal_id)

        logger.info(
            "ingest_recipe_ok",
            extra={
                "meal_id": meal_id,
                "variant_id": variant_id,
                "status": status,
                "source": record.source,
                "external_id": record.external_id,
            },
        )

        return {"meal_id": meal_id, "variant_id": variant_id, "status": status}
    
    # -----------------------------------------------------
    # Invoked Address : ingest_indian_kaggle within this code file pipeline.py
    # Invokes ingestion of complete Record recipe in DB. This calls the row wise ingestion in loop
    # -----------------------------------------------------
    def ingest_records(self, records: Iterable[RecipeRecord], *, refresh_search: bool = True) -> None:
        for idx, rec in enumerate(records, start=1):
            t0 = time.time()
            try:
                # 
                self.ingest_recipe(rec, refresh_search=refresh_search)
            except Exception as e:  # pragma: no cover
                logger.exception(
                    "ingest_recipe_failed",
                    extra={"idx": idx, "source": rec.source, "external_id": rec.external_id, "err": str(e)},
                )
            finally:
                logger.info("ingest_progress", extra={"idx": idx, "elapsed_s": round(time.time() - t0, 3)})


# Invoked Address : From etl_run.py script to load the indian dataset
def ingest_indian_kaggle(
    csv_path: str,
    *,
    limit: Optional[int] = None,
    use_llm: bool = False,
    use_embeddings: bool = False,
    use_ml: bool = True,
) -> None:
    """
    Convenience wrapper used by scripts/etl_run.py.
    """
    client = get_supabase_client()
    # To Do: Check for Hugging Face Warning here in MealETL class object initialization
    etl = MealETL(client, use_llm=use_llm, use_embeddings=use_embeddings, use_ml=use_ml)
    # Build data table of records to be ingested in Meals Table of Supabase DB
    # initiates the ETL object of the Meal and loads data from CSV file. CSV --> DT --> Data Cleaning in DT --> Creating Recipe Record Object --> Assign it to ETL
    records = load_indian_kaggle_csv(csv_path, limit=limit)
    # Invokes ingestion of complete Record recipe in DB
    etl.ingest_records(records)


def ingest_kaggle_all(csv_path: str, *, limit: Optional[int] = None) -> None:
    """
    Backwards-compatible alias. The repo historically used "ingest_kaggle_all" for the
    Indian Kaggle CSV; keeping this prevents script breakage.
    """
    ingest_indian_kaggle(csv_path, limit=limit)
