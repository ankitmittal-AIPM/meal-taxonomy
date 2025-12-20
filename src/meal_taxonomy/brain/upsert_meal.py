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
"""

from typing import Any, Dict, List, Optional, Tuple

from supabase import Client

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.logging_utils import get_logger, RUN_ID
from src.meal_taxonomy.brain.schema import EnrichedMealVariant
from src.meal_taxonomy.taxonomy.taxonomy_seed import ensure_tag_type, ensure_tag

MODULE_PURPOSE = (
    "Meal Brain upsert pipeline: dedupe + canonical/variant upsert "
    "into Supabase (meals, meal_variants, meal_synonyms, tags)."
)

logger = get_logger("brain_upsert")

# Thresholds – tune empirically
T_SAME = 0.80
T_MAYBE = 0.65


def upsert_meal(
    enriched: EnrichedMealVariant,
    client: Optional[Client] = None,
) -> Tuple[str, str, str]:
    """
    Main entry.

    Returns:
        (meal_id, variant_id, status)
        status in {"new_canonical", "attached_as_variant", "needs_review"}
    """
    if client is None:
        client = get_supabase_client()

    raw = enriched.raw

    logger.info(
        "Upserting RawMeal '%s' (source_type=%s, source_id=%s, run_id=%s)",
        raw.name,
        raw.source_type,
        raw.source_id,
        RUN_ID,
        extra={
            "invoking_func": "upsert_meal",
            "invoking_purpose": MODULE_PURPOSE,
            "next_step": "find candidate canonical meals",
            "resolution": "",
        },
    )

    candidates = _find_candidate_meals(enriched, client)
    best, score = _pick_best_candidate(enriched, candidates)

    if best and score is not None and score >= T_SAME:
        meal_id = best["id"]
        variant_id = _insert_variant(meal_id, enriched, client, needs_review=False)
        _maybe_update_canonical(meal_id, enriched, client)
        status = "attached_as_variant"
    elif best and score is not None and score >= T_MAYBE:
        meal_id = best["id"]
        variant_id = _insert_variant(meal_id, enriched, client, needs_review=True)
        status = "needs_review"
    else:
        meal_id = _insert_new_canonical(enriched, client)
        variant_id = _insert_variant(meal_id, enriched, client, is_primary=True)
        status = "new_canonical"

    logger.info(
        "Upsert completed for RawMeal '%s' -> meal_id=%s, variant_id=%s, status=%s",
        raw.name,
        meal_id,
        variant_id,
        status,
        extra={
            "invoking_func": "upsert_meal",
            "invoking_purpose": MODULE_PURPOSE,
            "next_step": "attach tags & synonyms",
            "resolution": "",
        },
    )

    _attach_synonyms(meal_id, enriched, client)
    _attach_tags(meal_id, enriched, client)

    return meal_id, variant_id, status


# ----------------------------------------------------------------------
# Candidate lookup & scoring (simplified, extensible)
# ----------------------------------------------------------------------
def _find_candidate_meals(
    enriched: EnrichedMealVariant,
    client: Client,
    k: int = 20,
) -> List[Dict[str, Any]]:
    """
    Placeholder candidate lookup.

    For now:
      - naive search on canonical_name trigram,
      - later: add vector similarity via pgvector search_meals RPC.
    """
    name = enriched.canonical_name
    if not name:
        return []

    # NOTE: This assumes you add a search RPC or trigram index on meals.canonical_name.
    # Replace with your search_meals RPC if you already have one.
    resp = (
        client.table("meals")
        .select("id, canonical_name, primary_cuisine, primary_course, primary_diet")
        .ilike("canonical_name", f"%{name.split()[0]}%")
        .limit(k)
        .execute()
    )
    return resp.data or []


def _pick_best_candidate(
    enriched: EnrichedMealVariant,
    candidates: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
    """
    Compute a simple similarity score using name overlap for now.

    Extend later with:
      - Jaccard over ingredients,
      - vector cosine similarity,
      - cuisine/course matches, etc.
    """
    if not candidates:
        return None, None

    name_tokens = set(enriched.canonical_name.lower().split())

    best = None
    best_score = -1.0
    for cand in candidates:
        cand_name = (cand.get("canonical_name") or "").lower()
        cand_tokens = set(cand_name.split())
        inter = len(name_tokens & cand_tokens)
        union = len(name_tokens | cand_tokens) or 1
        sim_name = inter / union
        score = sim_name  # later: mix in embedding + tags

        if score > best_score:
            best_score = score
            best = cand

    return best, best_score


# ----------------------------------------------------------------------
# Inserts / updates
# ----------------------------------------------------------------------
def _insert_new_canonical(
    enriched: EnrichedMealVariant,
    client: Client,
) -> str:
    """
    Create a new canonical meal row in meals.
    """
    payload = {
        "canonical_name": enriched.canonical_name,
        "primary_cuisine": enriched.raw.cuisine or (enriched.region_tags[0] if enriched.region_tags else None),
        "primary_course": enriched.predicted_course or enriched.raw.course,
        "primary_diet": enriched.predicted_diet or enriched.raw.diet,
        "region_tags": enriched.region_tags or None,
        "default_spice_level": enriched.spice_level,
        "default_servings": int(enriched.servings or 4),
        "typical_prep_time_min": enriched.prep_time_mins,
        "typical_cook_time_min": enriched.cook_time_mins,
        "health_tags": enriched.health_tags or None,
        "occasion_tags": enriched.occasion_tags or None,
        "utensil_tags": enriched.utensil_tags or None,
        "embedding": enriched.embedding or None,
    }

    resp = client.table("meals").insert(payload).execute()
    row = (resp.data or [])[0]
    return row["id"]


def _insert_variant(
    meal_id: str,
    enriched: EnrichedMealVariant,
    client: Client,
    *,
    needs_review: bool = False,
    is_primary: bool = False,
) -> str:
    payload = {
        "meal_id": meal_id,
        "source_type": enriched.raw.source_type,
        "source_id": enriched.raw.source_id,
        "title_original": enriched.raw.name,
        "title_clean": enriched.canonical_name,
        "ingredients_raw": enriched.raw.ingredients_text,
        "ingredients_norm": enriched.ingredients_norm,
        "instructions_raw": enriched.raw.instructions_text,
        "instructions_norm": enriched.instructions_norm,
        "cuisine": enriched.raw.cuisine,
        "course": enriched.predicted_course or enriched.raw.course,
        "diet": enriched.predicted_diet or enriched.raw.diet,
        "prep_time_min": enriched.prep_time_mins,
        "cook_time_min": enriched.cook_time_mins,
        "total_time_min": enriched.total_time_mins,
        "servings": enriched.servings,
        "spice_level": enriched.spice_level,
        "image_url": enriched.raw.extra.get("image_url") if enriched.raw.extra else None,
        "needs_review": needs_review,
        "is_primary": is_primary,
        "embedding": enriched.embedding or None,
    }
    resp = client.table("meal_variants").insert(payload).execute()
    row = (resp.data or [])[0]
    return row["id"]


def _maybe_update_canonical(
    meal_id: str,
    enriched: EnrichedMealVariant,
    client: Client,
) -> None:
    """
    Conservative canonical updater – only fills in NULLs / widens ranges.
    """
    # TODO: Implement range widening for time, merging tags, etc.
    # For now, we can leave canonical as-is once created.
    return


def _attach_synonyms(
    meal_id: str,
    enriched: EnrichedMealVariant,
    client: Client,
) -> None:
    alt_names = enriched.alt_names or []
    if not alt_names:
        return

    rows = [
        {
            "meal_id": meal_id,
            "name": name,
            "language": "en",
            "source": "llm",
        }
        for name in alt_names
    ]
    client.table("meal_synonyms").upsert(rows).execute()


def _attach_tags(
    meal_id: str,
    enriched: EnrichedMealVariant,
    client: Client,
) -> None:
    """
    Turn enrichment tag_candidates into tag + meal_tag rows using your
    existing taxonomy convention.
    """
    if not enriched.tag_candidates:
        return

    for tag_type_name, values in enriched.tag_candidates.items():
        tag_type_id = ensure_tag_type(client, tag_type_name, description="")
        for value in values:
            tag_id = ensure_tag(client, tag_type_id, value)
            client.table("meal_tags").upsert(
                {
                    "meal_id": meal_id,
                    "tag_id": tag_id,
                }
            ).execute()
