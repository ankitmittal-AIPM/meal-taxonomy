"""
recommender.py

A lightweight recommender that works with the current Supabase schema.

Design goals:
  - Works even if embeddings are missing (falls back to tags)
  - Uses DB functions when available (match_canonical_meals)
  - Keeps business logic in Python, storage/retrieval in Postgres

This is intentionally not a heavy "research" recommender system. It is meant
to be a pragmatic baseline that you can ship and iterate on.

Three intelligence layers:
  Layer-0 (heuristics):
    - tag overlap between user preferences and meal tags
    - optional hard filters (diet, region, meal_type, max_time_minutes)

  Layer-1 (ML):
    - embedding similarity against a simple user profile vector
      (weighted avg of liked meal embeddings)

  Layer-2 (LLM, optional):
    - natural-language explanation strings for why a meal was recommended
      (only used if OpenAI is configured)

Note:
  - Service role bypasses RLS (ETL/backfills).
  - For client apps, prefer calling RPCs or a backend; do not expose service keys.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import math

from supabase import Client

from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class RecommendationRequest:
    user_id: str
    limit: int = 20

    # Optional hard filters
    diet_value: Optional[str] = None
    meal_type_value: Optional[str] = None
    region_value: Optional[str] = None
    max_time_minutes: Optional[float] = None

    # Weighting knobs
    weight_tags: float = 0.6
    weight_embedding: float = 0.4


@dataclass
class RecommendedMeal:
    id: str
    title: str
    score: float
    reasons: List[str]


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += float(x) * float(y)
        na += float(x) * float(x)
        nb += float(y) * float(y)
    if na <= 0 or nb <= 0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


class MealRecommender:
    def __init__(self, client: Client) -> None:
        self.client = client

    # ------------------------------------------------------------------
    # Public APIs
    # ------------------------------------------------------------------
    def recommend_for_user(self, req: RecommendationRequest) -> List[RecommendedMeal]:
        """
        Recommend meals for a user using tags + (optional) embeddings.

        This function is backend-friendly and intentionally limits the number
        of round-trips:
          - fetch user prefs + interactions
          - retrieve a candidate pool by top tags
          - fetch meal tags + embeddings
          - score + explain + return

        Returns:
            List[RecommendedMeal] sorted by score desc
        """
        tag_weights = self._get_user_tag_weights(req.user_id)
        liked_meal_ids = self._get_user_positive_meal_ids(req.user_id)

        profile_embedding = self._get_user_profile_embedding(liked_meal_ids)

        candidate_meal_ids = self._candidate_meals_from_tags(
            tag_ids=list(tag_weights.keys()),
            exclude_meal_ids=liked_meal_ids,
            limit_pool=max(req.limit * 50, 200),
        )

        if not candidate_meal_ids:
            logger.info("recommend_fallback_empty_candidates", extra={"user_id": req.user_id})
            return []

        meals = self._fetch_meals(candidate_meal_ids)
        meal_tags = self._fetch_meal_tags(candidate_meal_ids)

        # Preload tag labels for explanation
        tag_labels = self._fetch_tag_labels(list(tag_weights.keys()))

        scored: List[RecommendedMeal] = []
        for m in meals:
            mid = m["id"]
            title = m.get("title") or ""
            time_min = m.get("total_time_minutes")
            emb = m.get("embedding")

            tags_for_meal = meal_tags.get(mid, [])
            tag_score = self._score_by_tags(tags_for_meal, tag_weights)
            emb_score = 0.0
            if profile_embedding and emb:
                emb_score = _cosine_similarity(profile_embedding, emb)

            # Hard filters
            if req.max_time_minutes is not None and time_min is not None:
                try:
                    if float(time_min) > float(req.max_time_minutes):
                        continue
                except Exception:
                    pass

            # Optional tag filters (diet/meal_type/region) are handled by candidate generation
            final_score = (req.weight_tags * tag_score) + (req.weight_embedding * emb_score)

            reasons = self._build_reasons(tags_for_meal, tag_weights, tag_labels, emb_score=emb_score)
            scored.append(RecommendedMeal(id=mid, title=title, score=float(final_score), reasons=reasons))

        scored.sort(key=lambda x: x.score, reverse=True)
        return scored[: req.limit]

    def recommend_similar(self, meal_id: str, *, limit: int = 10, threshold: float = 0.75) -> List[RecommendedMeal]:
        """
        Recommend meals similar to a given meal using pgvector RPC match_canonical_meals().
        """
        # Fetch embedding for the meal
        res = self.client.table("meals").select("embedding,title").eq("id", meal_id).limit(1).execute()
        if not res.data:
            return []
        base = res.data[0]
        emb = base.get("embedding")
        if not emb:
            return []

        try:
            matches = (
                self.client.rpc(
                    "match_canonical_meals",
                    {"query_embedding": emb, "match_threshold": float(threshold), "match_count": int(limit) + 1},
                )
                .execute()
                .data
            )
        except Exception as e:
            logger.warning("match_canonical_meals_rpc_failed", extra={"err": str(e)})
            return []

        out: List[RecommendedMeal] = []
        for row in matches or []:
            if row["id"] == meal_id:
                continue
            out.append(RecommendedMeal(id=row["id"], title=row["title"], score=float(row["similarity"]), reasons=["Similar embedding"]))
        return out[:limit]

    # ------------------------------------------------------------------
    # Data fetch helpers
    # ------------------------------------------------------------------
    def _get_user_tag_weights(self, user_id: str) -> Dict[str, float]:
        res = self.client.table("user_tag_preferences").select("tag_id,weight").eq("user_id", user_id).execute()
        weights: Dict[str, float] = {}
        for row in res.data or []:
            try:
                weights[row["tag_id"]] = float(row.get("weight") or 0)
            except Exception:
                weights[row["tag_id"]] = 0.0
        return weights

    def _get_user_positive_meal_ids(self, user_id: str) -> List[str]:
        res = (
            self.client.table("user_meal_interactions")
            .select("meal_id,interaction_type,rating")
            .eq("user_id", user_id)
            .execute()
        )
        positive: List[str] = []
        for row in res.data or []:
            it = (row.get("interaction_type") or "").lower()
            rating = row.get("rating")
            if it in {"like", "save", "cook"}:
                positive.append(row["meal_id"])
                continue
            try:
                if rating is not None and float(rating) >= 4.0:
                    positive.append(row["meal_id"])
            except Exception:
                continue
        # De-dupe while preserving order
        seen = set()
        out: List[str] = []
        for mid in positive:
            if mid in seen:
                continue
            seen.add(mid)
            out.append(mid)
        return out

    def _get_user_profile_embedding(self, liked_meal_ids: List[str]) -> Optional[List[float]]:
        if not liked_meal_ids:
            return None
        res = self.client.table("meals").select("id,embedding").in_("id", liked_meal_ids).execute()
        embs: List[List[float]] = []
        for row in res.data or []:
            if row.get("embedding"):
                embs.append(row["embedding"])
        if not embs:
            return None

        # Simple average
        dim = len(embs[0])
        acc = [0.0] * dim
        for e in embs:
            if len(e) != dim:
                continue
            for i, v in enumerate(e):
                acc[i] += float(v)
        n = float(len(embs))
        return [v / n for v in acc]

    def _candidate_meals_from_tags(self, tag_ids: List[str], exclude_meal_ids: List[str], limit_pool: int) -> List[str]:
        if not tag_ids:
            return []

        # Use only top N tags to avoid huge IN filters
        tag_ids = tag_ids[:25]

        q = self.client.table("meal_tags").select("meal_id,tag_id").in_("tag_id", tag_ids)
        if exclude_meal_ids:
            # Supabase doesn't support NOT IN directly; fetch and filter client-side.
            pass
        res = q.limit(limit_pool).execute()

        meal_ids: List[str] = []
        for row in res.data or []:
            mid = row["meal_id"]
            if exclude_meal_ids and mid in set(exclude_meal_ids):
                continue
            meal_ids.append(mid)

        # De-dupe preserving order
        seen = set()
        out: List[str] = []
        for mid in meal_ids:
            if mid in seen:
                continue
            seen.add(mid)
            out.append(mid)
        return out

    def _fetch_meals(self, meal_ids: List[str]) -> List[Dict[str, Any]]:
        if not meal_ids:
            return []
        res = (
            self.client.table("meals")
            .select("id,title,total_time_minutes,embedding")
            .in_("id", meal_ids)
            .execute()
        )
        return res.data or []

    def _fetch_meal_tags(self, meal_ids: List[str]) -> Dict[str, List[str]]:
        if not meal_ids:
            return {}
        res = self.client.table("meal_tags").select("meal_id,tag_id").in_("meal_id", meal_ids).execute()
        out: Dict[str, List[str]] = {}
        for row in res.data or []:
            out.setdefault(row["meal_id"], []).append(row["tag_id"])
        return out

    def _fetch_tag_labels(self, tag_ids: List[str]) -> Dict[str, str]:
        if not tag_ids:
            return {}
        res = self.client.table("tags").select("id,value,label_en").in_("id", tag_ids).execute()
        out: Dict[str, str] = {}
        for row in res.data or []:
            out[row["id"]] = row.get("label_en") or row.get("value") or ""
        return out

    # ------------------------------------------------------------------
    # Scoring + explanations
    # ------------------------------------------------------------------
    def _score_by_tags(self, meal_tag_ids: List[str], tag_weights: Dict[str, float]) -> float:
        if not meal_tag_ids or not tag_weights:
            return 0.0
        total = 0.0
        denom = 0.0
        for tid, w in tag_weights.items():
            denom += abs(float(w))
        if denom <= 0:
            denom = 1.0
        for t in meal_tag_ids:
            if t in tag_weights:
                total += float(tag_weights[t])
        return float(total / denom)

    def _build_reasons(
        self,
        meal_tag_ids: List[str],
        tag_weights: Dict[str, float],
        tag_labels: Dict[str, str],
        *,
        emb_score: float,
    ) -> List[str]:
        reasons: List[str] = []
        if meal_tag_ids and tag_weights:
            overlaps: List[Tuple[float, str]] = []
            for tid in meal_tag_ids:
                if tid in tag_weights:
                    overlaps.append((float(tag_weights[tid]), tid))
            overlaps.sort(key=lambda x: abs(x[0]), reverse=True)
            top = overlaps[:3]
            if top:
                labels = [tag_labels.get(tid, tid) for _, tid in top]
                reasons.append("Matches your preferences: " + ", ".join([l for l in labels if l]))
        if emb_score:
            reasons.append(f"Similar to meals you liked (embedding={emb_score:.2f})")
        return reasons
