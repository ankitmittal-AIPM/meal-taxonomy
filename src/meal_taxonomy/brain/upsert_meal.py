# src/meal_taxonomy/brain/upsert_meal.py
from __future__ import annotations

"""
    upsert_meal.py

    Purpose:
        Given an EnrichedMealVariant, decide whether it should become:
        - a new canonical meal, or
        - a variant of an existing canonical meal.

        Then:
        - upsert into meals (canonical),
        - insert into meal_variants,
        - upsert synonyms into meal_synonyms,
        - attach tags via existing Meal Taxonomy tagging/ontology pipeline.

        This is the "Meal Brain" entrypoint, called by ETL and user flows.
        
    Purpose:
        Meal Brain = canonicalization + de-duplication layer.

        Given an EnrichedMealVariant, decide whether it should become:
        - a new canonical meal (row in `meals`), OR
        - a variant of an existing canonical meal (row in `meal_variants` referencing an existing `meals.id`).

        This module intentionally does *NOT* attach:
        - tags (meal_tags) or
        - ingredients (meal_ingredients)
        because the ETL pipeline already has robust tagging + ontology logic and we do not want to duplicate it.

        Think of Meal Brain as:
        - "which canonical dish is this?"  (cluster / identity)
        - "store the source-specific variant" (provenance, dedupe, audit)

    Notes:
        - Candidate retrieval is designed to be fast:
            * Prefer a Supabase RPC (search_meals_v2) backed by DB indexes (FTS + trigram).
            * Fall back to a simple ILIKE scan if the RPC does not exist.
        - Similarity scoring is intentionally simple and explainable right now (name-based).
        You can later mix in:
            * embedding similarity (pgvector),
            * ingredient overlap,
            * tag overlap,
            * time similarity.
    Updates in the "intelligent" version:
    - supports canonical vs variant rows (meals.is_canonical + meals.canonical_meal_id)
    - supports embeddings + vector candidate lookup via match_canonical_meals RPC (optional)
    - supports search doc refresh via refresh_meal_search_doc(uuid) (optional)
            
    Return:
        (meal_id, variant_id, status)
        where status in {"new_canonical", "attached_as_variant", "needs_review", "existing_variant"}
"""

import difflib
import uuid
from typing import Any, Dict, List, Optional, Tuple
from supabase import Client

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.logging_utils import get_logger, RUN_ID
from src.meal_taxonomy.enrichment.cleaning import normalize_title
from src.meal_taxonomy.brain.schema import EnrichedMealVariant
from src.meal_taxonomy.taxonomy.taxonomy_seed import ensure_tag_type, ensure_tag

MODULE_PURPOSE = (
    "Meal Brain upsert pipeline: dedupe + canonical/variant upsert "
    "into Supabase (meals, meal_variants, meal_synonyms, tags)."
)

logger = get_logger("brain_upsert")

# ----------------------------------------------------------------------
# Thresholds
# ----------------------------------------------------------------------
# If you later add embeddings, you can keep the same thresholds and adjust
# weighting in _score_candidate().
# Thresholds – tune empirically
T_SAME = 0.80       # confidently same canonical dish
T_MAYBE = 0.65      # plausible match; attach variant but mark needs_review


def upsert_meal(
    enriched: EnrichedMealVariant,
    client: Optional[Client] = None,
) -> Tuple[str, str, str]:
    """
    Main entry --> Upsert a canonical meal + variant given an enriched meal variant

    Returns:
        (meal_id, variant_id, status)
        status in {"new_canonical", "attached_as_variant", "needs_review"}
    """
    if client is None:
        client = get_supabase_client()

    # 0) Idempotency: if this source_type+source_id variant already exists, return quickly.
    existing = _get_existing_variant(enriched, client)
    if existing is not None:
        logger.info(
            "Variant already exists; returning existing meal_id=%s variant_id=%s (source_type=%s source_id=%s)",
            existing["meal_id"],
            existing["id"],
            enriched.raw.source_type,
            enriched.raw.source_id,
            extra={
                "invoking_func": "upsert_meal",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "Skip dedupe and return",
                "resolution": "existing_variant",
                "run_id": RUN_ID,
            },
        )
        return existing["meal_id"], existing["id"], "existing_variant"

    # raw = enriched.raw

    # 1) Find candidates (fast DB-side search where possible)
    candidates = _find_candidate_meals(enriched, client, k=20)

    # 2) Pick best match by explainable scoring
    best, best_score = _pick_best_candidate(enriched, candidates)

    # 3) Decide canonical meal id
    if best is not None and best_score >= T_SAME:
        meal_id = best["id"]
        status = "attached_as_variant"
        needs_review = False
    elif best is not None and best_score >= T_MAYBE:
        meal_id = best["id"]
        status = "needs_review"
        needs_review = True
    else:
        meal_id = _insert_new_canonical(enriched, client)
        status = "new_canonical"
        needs_review = True

    # 4) Insert/upsert the variant row
    variant_id = _upsert_variant(meal_id, enriched, client, needs_review=needs_review)

    # 5) Upsert synonyms (optional table; safe no-op if table missing)
    _attach_synonyms(meal_id, enriched, client)
    _attach_tags(meal_id, enriched, client)

    # Successful insertion of new meal with dedupe and variant's check
    logger.info(
        "Meal Brain upsert done; meal_id=%s variant_id=%s status=%s best_score=%.3f",
        meal_id,
        variant_id,
        status,
        float(best_score),
        extra={
            "invoking_func": "upsert_meal",
            "invoking_purpose": MODULE_PURPOSE,
            "next_step": "Return to ETL (ingredients + tags + ontology)",
            "resolution": status,
            "run_id": RUN_ID,
        },
    )

    return meal_id, variant_id, status


# ----------------------------------------------------------------------
# Existing variant lookup
# ----------------------------------------------------------------------
def _get_existing_variant(enriched: EnrichedMealVariant, client: Client) -> Optional[Dict[str, Any]]:
    """Return existing meal_variants row for this source if present."""
    try:
        resp = (
            client.table("meal_variants")
            .select("id, meal_id")
            .eq("source_type", enriched.raw.source_type)
            .eq("source_id", enriched.raw.source_id)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception as exc:  # noqa: BLE001
        # This usually means the table does not exist yet (migrations not applied).
        logger.warning(
            "Could not query meal_variants for idempotency check: %s",
            exc,
            extra={
                "invoking_func": "_get_existing_variant",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "Continue without idempotency shortcut",
                "resolution": "",
                "run_id": RUN_ID,
            },
        )
        return None


# ----------------------------------------------------------------------
# Candidate lookup & scoring
# ----------------------------------------------------------------------
def _find_candidate_meals(
    enriched: EnrichedMealVariant,
    client: Client,
    k: int = 20,
) -> List[Dict[str, Any]]:
    """Find candidate canonical meals.

    Preferred path:
      - Supabase RPC "search_meals_v2" (FTS + trigram indexes)
      If embedding is available and RPC exists:
         call public.match_canonical_meals(query_embedding, match_count=k)
    Fallback path:
      - simple ILIKE on meals.title_normalized / meals.title

    Returns:
      list of dicts with at least {id, title, title_normalized}
    """
    query = normalize_title(enriched.canonical_name or enriched.raw.name)

    # Try RPC first (fast DB-side search)
    try:
        resp = client.rpc(
            "search_meals_v2",
            {
                "query_text": query,
                "limit": k,
                # Keep these optional filters unset for candidate retrieval.
                "diet_value": None,
                "meal_type_value": None,
                "region_value": None,
            },
        ).execute()
        rows = resp.data or []
        # Expected keys: id, title, total_time_minutes, score (depending on SQL)
        # Normalize keys for scoring.
        candidates: List[Dict[str, Any]] = []
        for r in rows:
            candidates.append(
                {
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "title_normalized": r.get("title_normalized") or normalize_title(r.get("title") or ""),
                }
            )
        return [c for c in candidates if c.get("id")]
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "search_meals_v2 RPC unavailable or failed (will fall back): %s",
            exc,
            extra={
                "invoking_func": "_find_candidate_meals",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "Fallback to ILIKE candidate search",
                "resolution": "",
                "run_id": RUN_ID,
            },
        )

    # Fallback: ILIKE on title_normalized / title
    # Use the first token to keep the query selective.
    tokens = query.split()
    token = tokens[0] if tokens else query
    if not token:
        return []

    try:
        resp = (
            client.table("meals")
            .select("id, title, title_normalized")
            .ilike("title", f"%{token}%")
            .limit(k)
            .execute()
        )
        rows = resp.data or []
        for r in rows:
            r["title_normalized"] = r.get("title_normalized") or normalize_title(r.get("title") or "")
        return rows
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Fallback candidate lookup failed: %s",
            exc,
            extra={
                "invoking_func": "_find_candidate_meals",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "Return no candidates; will create new canonical",
                "resolution": "",
                "run_id": RUN_ID,
            },
        )
        return []


def _pick_best_candidate(
    enriched: EnrichedMealVariant,
    candidates: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], float]:
    best: Optional[Dict[str, Any]] = None
    best_score = -1.0
    for cand in candidates:
        score = _score_candidate(enriched, cand)
        if score > best_score:
            best_score = score
            best = cand
    return best, float(best_score)


def _score_candidate(enriched: EnrichedMealVariant, cand: Dict[str, Any]) -> float:
    """Explainable similarity score (name-based).

    score in [0, 1].
    """
    q = normalize_title(enriched.canonical_name or enriched.raw.name)
    c = normalize_title(cand.get("title_normalized") or cand.get("title") or "")

    if not q or not c:
        return 0.0

    # Token Jaccard (robust to word order)
    q_tokens = set(q.split())
    c_tokens = set(c.split())
    inter = len(q_tokens & c_tokens)
    union = len(q_tokens | c_tokens) or 1
    jacc = inter / union

    # Sequence ratio (captures near-spellings)
    seq = difflib.SequenceMatcher(a=q, b=c).ratio()

    # Weighted blend (simple; tune later)
    return float(0.55 * jacc + 0.45 * seq)


# ----------------------------------------------------------------------
# Persistence: meals (canonical) + meal_variants (variants)
# ----------------------------------------------------------------------
def _insert_new_canonical(enriched: EnrichedMealVariant, client: Client) -> str:
    """Insert a new canonical meal row into `meals` and return meal_id."""
    canonical_title = enriched.canonical_name or enriched.raw.name
    payload: Dict[str, Any] = {
        "title": canonical_title,
        "title_normalized": normalize_title(canonical_title),
        "description": enriched.raw.description,
        "instructions": enriched.instructions_norm or enriched.raw.instructions_text,
        # Keep existing schema expectations: 'source' and external_* fields.
        "source": "canonical",
        "external_source": "canonical",
        "external_id": str(uuid.uuid4()),
        "language_code": "en",
        "cook_time_minutes": int(enriched.cook_time_mins) if enriched.cook_time_mins is not None else None,
        "prep_time_minutes": int(enriched.prep_time_mins) if enriched.prep_time_mins is not None else None,
        "total_time_minutes": int(enriched.total_time_mins) if enriched.total_time_mins is not None else None,
        "servings": enriched.servings,
        "meta": {
            # Minimal canonical metadata (safe for ontology scripts that read meals.meta)
            "canonical": True,
            "created_from": {
                "source_type": enriched.raw.source_type,
                "source_id": enriched.raw.source_id,
            },
            "cuisine": enriched.raw.cuisine,
            "course": enriched.predicted_course or enriched.raw.course,
            "diet": enriched.predicted_diet or enriched.raw.diet,
            "region_tags": enriched.region_tags,
            "spice_level": enriched.spice_level,
            "difficulty": enriched.difficulty,
            "kids_friendly": enriched.kids_friendly,
            "occasion_tags": enriched.occasion_tags,
            "health_tags": enriched.health_tags,
            "utensil_tags": enriched.utensil_tags,
            "extra": enriched.extra or {},
        },
    }

    # Optional: store embedding if the DB schema has it.
    if enriched.embedding:
        payload["embedding"] = enriched.embedding

    # Insert with a safe fallback (in case embedding column is missing)
    try:
        resp = client.table("meals").insert(payload).execute()
        row = (resp.data or [])[0]
        return row["id"]
    except Exception as exc:  # noqa: BLE001
        if "embedding" in payload:
            payload.pop("embedding", None)
            resp = client.table("meals").insert(payload).execute()
            row = (resp.data or [])[0]
            logger.warning(
                "Inserted canonical meal without embedding column (schema missing embedding?): %s",
                exc,
                extra={
                    "invoking_func": "_insert_new_canonical",
                    "invoking_purpose": MODULE_PURPOSE,
                    "next_step": "Continue without embeddings",
                    "resolution": "",
                    "run_id": RUN_ID,
                },
            )
            return row["id"]
        raise


def _upsert_variant(
    meal_id: str,
    enriched: EnrichedMealVariant,
    client: Client,
    *,
    needs_review: bool = False,
) -> str:
    """Upsert a meal_variants row (idempotent by source_type+source_id)."""
    payload: Dict[str, Any] = {
        "meal_id": meal_id,
        "source_type": enriched.raw.source_type,
        "source_id": enriched.raw.source_id,
        "title_original": enriched.raw.name,
        "title_normalized": normalize_title(enriched.raw.name),
        "ingredients_raw": enriched.raw.ingredients_text,
        "ingredients_norm": enriched.ingredients_norm,
        "instructions_raw": enriched.raw.instructions_text,
        "instructions_norm": enriched.instructions_norm,
        "cuisine": enriched.raw.cuisine,
        "course": enriched.predicted_course or enriched.raw.course,
        "diet": enriched.predicted_diet or enriched.raw.diet,
        "prep_time_minutes": enriched.prep_time_mins,
        "cook_time_minutes": enriched.cook_time_mins,
        "total_time_minutes": enriched.total_time_mins,
        "servings": enriched.servings,
        "needs_review": needs_review,
        "meta": {
            "tag_candidates": enriched.tag_candidates,
            "region_tags": enriched.region_tags,
            "spice_level": enriched.spice_level,
            "difficulty": enriched.difficulty,
            "kids_friendly": enriched.kids_friendly,
            "occasion_tags": enriched.occasion_tags,
            "health_tags": enriched.health_tags,
            "utensil_tags": enriched.utensil_tags,
            "extra": enriched.extra or {},
        },
    }

    if enriched.embedding:
        payload["embedding"] = enriched.embedding

    try:
        resp = client.table("meal_variants").upsert(
            payload, on_conflict="source_type,source_id"
        ).execute()
        row = (resp.data or [])[0]
        return row["id"]
    except Exception as exc:  # noqa: BLE001
        # If meal_variants doesn't exist, give a clear log and return a placeholder.
        logger.error(
            "Failed to upsert meal_variants (did you run migrations?): %s",
            exc,
            extra={
                "invoking_func": "_upsert_variant",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "Create meal_variants table and rerun",
                "resolution": "",
                "run_id": RUN_ID,
            },
        )
        return ""


# ----------------------------------------------------------------------
# Synonyms (optional)
# ----------------------------------------------------------------------
def _attach_synonyms(meal_id: str, enriched: EnrichedMealVariant, client: Client) -> None:
    """Upsert alt names into meal_synonyms table (optional)."""
    alt_names = enriched.alt_names or []
    if not alt_names:
        return

    rows = []
    for name in alt_names:
        if not name or not str(name).strip():
            continue
        rows.append(
            {
                "meal_id": meal_id,
                "synonym": str(name).strip(),
                "synonym_normalized": normalize_title(str(name)),
                "language_code": "en",
                "source": "enrichment",
            }
        )

    if not rows:
        return

    try:
        client.table("meal_synonyms").upsert(
            rows, on_conflict="meal_id,synonym_normalized"
        ).execute()
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "Skipping meal_synonyms upsert (table missing?): %s",
            exc,
            extra={
                "invoking_func": "_attach_synonyms",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "(Optional) create meal_synonyms table",
                "resolution": "",
                "run_id": RUN_ID,
            },
        )
        return