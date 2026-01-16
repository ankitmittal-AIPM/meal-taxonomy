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

from dataclasses import asdict, is_dataclass
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

# Purpose: Convert tag candidates to JSON-serializable dicts for Supabase JSON columns. 
# This further helps in storing tag candidates in a structured format in meta structure and makes it easy to query and analyze later.
def _serialize_tag_candidates(tag_candidates: Any) -> List[Dict[str, Any]]:
    """Convert tag candidates to JSON-serializable dicts for Supabase JSON columns."""
    out: List[Dict[str, Any]] = []
    if not tag_candidates:
        return out

    # Common case: list[TagCandidate] (dataclass)
    if isinstance(tag_candidates, list):
        for c in tag_candidates:
            if c is None:
                continue
            if is_dataclass(c):
                out.append(asdict(c))
            elif isinstance(c, dict):
                out.append(c)
            elif hasattr(c, "__dict__"):
                out.append(dict(c.__dict__))
            else:
                out.append({"value": str(c)})
        return out

    # Fallback: dict-based structures
    if isinstance(tag_candidates, dict):
        # Convert mapping {tag_type: [values]} into a uniform list of dicts
        for tag_type, vals in tag_candidates.items():
            if not vals:
                continue
            if isinstance(vals, (list, tuple)):
                for v in vals:
                    out.append({"tag_type": str(tag_type), "value": str(v)})
            else:
                out.append({"tag_type": str(tag_type), "value": str(vals)})
        return out

    # Last resort
    out.append({"value": str(tag_candidates)})
    return out


# ----------------------------------------------------------------------
# Thresholds
# ----------------------------------------------------------------------
# If you later add embeddings, you can keep the same thresholds and adjust
# weighting in _score_candidate().
# Thresholds – tune empirically
T_SAME = 0.85       # confidently same canonical dish
T_MAYBE = 0.70      # plausible match; attach variant but mark needs_review

# Invoked Address: Main entry --> Upsert a canonical meal + variant given an enriched meal variant
def upsert_meal( enriched: EnrichedMealVariant, client: Optional[Client] = None,) -> Tuple[str, str, str]:
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

    # 1) Find candidates (fast DB-side search where possible)
    candidates = _find_candidate_meals(enriched, client, k=20)

    # 2) Pick best match by explainable scoring
    best, best_score = _pick_best_candidate(enriched, candidates)

    # 3) Decide new or get existing canonical meal id
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

    # 6) Create new tags in Tags DB from canonical meal using existing taxonomy seed helpers
    _create_tags(meal_id, enriched, client)

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
# Purpose: Check if a meal_variant row already exists for this source_type+source_id to ensure idempotency.
# To Do: How to handle cases where multiple records of same meal present in the same file
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
# Purpose: Find candidate canonical meals using fast DB-side search (RPC or ILIKE). Gives best 20 results that has ben set as value of "k"
# ----------------------------------------------------------------------
def _find_candidate_meals(enriched: EnrichedMealVariant, client: Client, k: int = 20,) -> List[Dict[str, Any]]:
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
    diet_value = _normalize(enriched.predicted_diet or enriched.raw.diet) if _normalize(enriched.predicted_diet or enriched.raw.diet) else None
    meal_type_value = _normalize(enriched.predicted_course or enriched.raw.course) if _normalize(enriched.predicted_course or enriched.raw.course) else None
    region_value = _normalize(", ".join(enriched.region_tags or [])) if enriched.region_tags else None

    # Method 1 - Try RPC first (fast DB-side search)
    try:
        resp = client.rpc(
            "search_meals_v2",
            {
                "query_text": query,
                "limit_n": k,
                # To Do: Keep these optional filters unset for candidate retrieval.
                "diet_value": diet_value,
                "meal_type_value": meal_type_value,
                "region_value": region_value,
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
                    "score": r.get("score", 0.0),
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

    # Method 2 - Fallback: ILIKE on title_normalized / title
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
    
# Find the candidate that is most similar to enriched meal
def _pick_best_candidate(enriched: EnrichedMealVariant, candidates: List[Dict[str, Any]],) -> Tuple[Optional[Dict[str, Any]], float]:
    cands = list(candidates)
    if not cands:
        return None, 0.0
    
    rpc_scores = [float(c.get("score") or 0.0) for c in cands]
    min_rpc_score = min(rpc_scores)
    max_rpc_score = max(rpc_scores)

    best: Optional[Dict[str, Any]] = None
    best_score = -1.0
    # Get the best candidate based on score iteratively
    for cand in candidates:
        score = _score_candidate(enriched, cand, min_rpc_score=min_rpc_score, max_rpc_score=max_rpc_score)
        if score > best_score:
            best_score = score
            best = cand
    return best, float(best_score)

# To Do: Later, you can mix in embedding similarity, ingredient overlap, tag overlap, etc.
# Purpose: Compute explainable similarity score between enriched meal and candidate meal.
# Invoked Address: Called from _pick_best_candidate function to compute similarity score between enriched meal and candidate meal.
def _score_candidate(enriched: EnrichedMealVariant, cand: Dict[str, Any], *, min_rpc_score: float, max_rpc_score: float) -> float:
    """
    Food-aware similarity score using ONLY:
      - title / title_normalized (via normalize_title)
      - rpc score from DB

    Returns score in [0, 1] (approximately; penalties clamp it down).
    """
    q = normalize_title(enriched.canonical_name or enriched.raw.name)
    c = normalize_title(cand.get("title_normalized") or cand.get("title") or "")

    # Handle case where both title in query and candidate are empty
    if not q or not c:
        return 0.0

    #------Score A - Original name similarity Score (robust to typos / word order)------
    # Token Jaccard (robust to word order)
    q_tokens = set(q.split())
    c_tokens = set(c.split())
    inter = len(q_tokens & c_tokens)
    union = len(q_tokens | c_tokens) or 1
    jacc = inter / union

    # Sequence ratio (captures near-spellings)
    seq = difflib.SequenceMatcher(a=q, b=c).ratio()
    name_score = float(0.55 * jacc + 0.45 * seq)

    #-----Score B - RPC Score normalized per candidate set----
    cand_rpc = float(cand.get("score") or 0.0)
    # Per-query normalization across returned candidates.
    denom = (max_rpc_score - min_rpc_score)
    if denom <= 1e-9:
        return 0.0
    x = (cand_rpc - min_rpc_score) / denom
    # Clamp defensively.
    rpc_norm =  float(max(0.0, min(1.0, x)))

    # Final blend (tune later)
    final = 0.55 * name_score + 0.45 * rpc_norm

    # To Do: Recheck the score computation Weighted blend (simple; tune later)
    return float(max(0.0, min(1.0, final)))


# ----------------------------------------------------------------------
# Persistence: meals (canonical) + meal_variants (variants)
# ----------------------------------------------------------------------
def _insert_new_canonical(enriched: EnrichedMealVariant, client: Client) -> str:
    """Insert a new canonical meal row into `meals` and return meal_id."""
    canonical_title = enriched.canonical_name or enriched.raw.name
    payload: Dict[str, Any] = {
        "title": canonical_title,
        # "title_normalized": normalize_title(canonical_title),
        "description": enriched.raw.description,
        "instructions": enriched.instructions_norm or enriched.raw.instructions_text,
        # Keep existing schema expectations: 'source' and external_* fields.
        #"source": "canonical",
        "source": "dataset",
        # "external_source": "canonical",
        "external_source": enriched.raw.source_type,
        # "external_id": str(uuid.uuid4()),
        "external_id": enriched.raw.source_id,
        "language_code": enriched.raw.language_code if enriched.raw.language_code else "en",
        "cook_time_minutes": int(enriched.cook_time_mins) if enriched.cook_time_mins is not None else None,
        "prep_time_minutes": int(enriched.prep_time_mins) if enriched.prep_time_mins is not None else None,
        # "total_time_minutes": int(enriched.total_time_mins) if enriched.total_time_mins is not None else None,
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
            "difficulty": enriched.difficulty,
            "debug": enriched.debug,
        },
        "is_canonical": True,
        "canonical_meal_id": None,  
        "embedding": enriched.embedding,
        "search_text": None,  # filled by refresh_meal_search_doc() after tags/synonyms attach
    }

    # Insert with a safe fallback (in case embedding column is missing)
    try:
        resp = client.table("meals").insert(payload).execute()
        row = (resp.data or [])[0]
        return row["id"]
    except Exception as exc:  # noqa: BLE001
        # If meal doesn't exist, give a clear log and return a placeholder.
        logger.error(
            "Failed to upsert canonical meal: %s",
            exc,
            extra={
                "invoking_func": "_insert_new_canonical",
                "invoking_purpose": MODULE_PURPOSE,
                "next_step": "Create canonical meal in meal table and rerun",
                "resolution": "check meal payload",
                "run_id": RUN_ID,
            },
        )
        return ""
    
def _upsert_variant( meal_id: str, enriched: EnrichedMealVariant, client: Client, *, needs_review: bool = False,) -> str:
    """Upsert a meal_variants row (idempotent by source_type+source_id)."""
    payload: Dict[str, Any] = {
        "meal_id": meal_id,
        "source_type": enriched.raw.source_type,
        "source_id": enriched.raw.source_id,
        "title_original": enriched.raw.name,
        # "title_normalized": normalize_title(enriched.raw.name),
        "ingredients_raw": enriched.raw.ingredients_text,
        "ingredients_norm": enriched.ingredients_norm,
        "instructions_raw": enriched.raw.instructions_text,
        "instructions_norm": enriched.instructions_norm,
        "cuisine": enriched.raw.cuisine,
        "course": enriched.predicted_course or enriched.raw.course,
        "diet": enriched.predicted_diet or enriched.raw.diet,
        "prep_time_minutes": enriched.prep_time_mins,
        "cook_time_minutes": enriched.cook_time_mins,
        #"total_time_minutes": enriched.total_time_mins,
        "servings": enriched.servings,
        "needs_review": needs_review,
        "embedding": enriched.embedding,
        "meta": {
            "tag_candidates": _serialize_tag_candidates(enriched.tag_candidates),
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

# To Do: Where to call this
def _maybe_update_canonical(
    canonical_meal_id: str,
    enriched: EnrichedMealVariant,
    client: Client,
) -> None:
    """
    Optional: update canonical meal meta aggregates.

    For now we keep this minimal to avoid overwriting curated values.
    """
    # You can extend this later:
    #  - widen time ranges
    #  - union tags
    #  - attach missing embeddings
    _ = canonical_meal_id, enriched, client
    return

# Purpose: Insert new tags in Tags DB from canonical meal using existing taxonomy seed helpers.
# It DOESN'T link them to meal_tags table; that is done elsewhere in the pipeline.
def _create_tags(canonical_meal_id: str, enriched: EnrichedMealVariant, client: Client,) -> None:
    """
    Create new tags in Tags DB from canonical meal using existing taxonomy seed helpers.
    """

    tag_type_Ids: Dict[str, str] = {}
    # Ensure tag types exist i.e. Tag Type ID is there for Tag Type fetched from new canonical meal
    for tag_type in [
        "diet",
        "meal_type",
        "cuisine_region",
        "course",
        "equipment",
        "technique",
        "occasion",
        "difficulty",
        "time_bucket",
        "taste_profile",
        "health",
        "ingredient_category",
        "spice_level",
        "kids_friendly",
    ]:
        try:
            temp_tag_type_id = ensure_tag_type(client, tag_type, f"Auto-created tag_type: {tag_type}")
            tag_type_Ids[tag_type] = temp_tag_type_id
        except Exception:
            pass

    # Insert specific tags (minimal: use ensure_tag which upserts tags)
    # NOTE: ACTUAL LINKING of Tags into meal_tags is done elsewhere in your pipeline.
    # Here we just ensure tags exist.
    if enriched.predicted_diet:
        tag_type_id = tag_type_Ids.get("diet")
        ensure_tag(client, tag_type_id=tag_type_id, value=enriched.predicted_diet, label_en=enriched.predicted_diet.replace("_", " ").title())

    if enriched.predicted_course:
        tag_type_id = tag_type_Ids.get("course")
        ensure_tag(client, tag_type_id=tag_type_id, value=enriched.predicted_course, label_en=str(enriched.predicted_course).title())
    for r in (enriched.region_tags or []):
        tag_type_id = tag_type_Ids.get("cuisine_region")
        ensure_tag(client, tag_type_id=tag_type_id, value=r, label_en=r)

    if enriched.spice_level is not None:
        tag_type_id = tag_type_Ids.get("spice_level")
        ensure_tag(client, tag_type_id=tag_type_id, value=str(enriched.spice_level), label_en=f"Spice {enriched.spice_level}")

    if enriched.kids_friendly is not None:
        tag_type_id = tag_type_Ids.get("kids_friendly")
        ensure_tag(client, tag_type_id=tag_type_id, value="kids_friendly" if enriched.kids_friendly else "not_kids_friendly",
                   label_en="Kids-friendly" if enriched.kids_friendly else "Not kids-friendly")
    
    for h in (enriched.health_tags or []):
        tag_type_id = tag_type_Ids.get("health")
        ensure_tag(client, tag_type_id=tag_type_id, value=h, label_en=h.replace("_", " ").title())
    
    for o in (enriched.occasion_tags or []):
        tag_type_id = tag_type_Ids.get("occasion")
        ensure_tag(client, tag_type_id=tag_type_id, value=o, label_en=o.replace("_", " ").title())

    for u in (enriched.utensil_tags or []):
        tag_type_id = tag_type_Ids.get("equipment")
        ensure_tag(client, tag_type_id=tag_type_id, value=u, label_en=u.replace("_", " ").title())

# To Do: Where to call this
def _refresh_search_doc(meal_id: str, client: Client) -> None:
    """Call refresh_meal_search_doc(uuid) if present."""
    try:
        client.rpc("refresh_meal_search_doc", {"target_meal_id": meal_id}).execute()
    except Exception:  # noqa: BLE001
        return


def _normalize(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

# ----------------------------------------------------------------------
# Synonyms (optional)
# Purpose: Upsert alt names into meal_synonyms table (if present). 
# Alt names are stored in enriched.alt_names which gets data majorly from LLM enrichment.
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
