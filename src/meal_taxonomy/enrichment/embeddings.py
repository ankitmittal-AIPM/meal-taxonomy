# src/meal_taxonomy/enrichment/embeddings.py
from __future__ import annotations

"""
embeddings.py

Purpose:
    Provide a single function get_meal_embedding(text) -> list[float] that can be:
      - stored in Supabase (pgvector) as a JSON array
      - used for vector search (match_canonical_meals RPC)
      - used for dedupe / clustering

Design:
  - This module should be "best effort":
      * If sentence-transformers is available, use it locally.
      * Else return None and let the rest of pipeline continue.
  - The embedding dimension is assumed 384 by default (MiniLM).
"""

from typing import List, Optional

from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("embeddings")

_MODEL = None


def _safe_import_sentence_transformers():
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except Exception:  # noqa: BLE001
        return None
    return SentenceTransformer


def _get_model():
    global _MODEL
    if _MODEL is not None:
        return _MODEL

    SentenceTransformer = _safe_import_sentence_transformers()
    if SentenceTransformer is None:
        return None

    try:
        # Multilingual MiniLM (384 dim) is a solid default for Indian language romanization too.
        _MODEL = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
        return _MODEL
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to load SentenceTransformer model: %s",
            exc,
            extra={
                "invoking_func": "_get_model",
                "invoking_purpose": "Load local embedding model",
                "next_step": "Install sentence-transformers + ensure model download works",
                "resolution": "",
            },
        )
        return None


def get_meal_embedding(text: str) -> Optional[List[float]]:
    """
    Returns:
        embedding as list[float] or None if embeddings are disabled/unavailable.
    """
    if not text or not text.strip():
        return None

    model = _get_model()
    if model is None:
        return None

    try:
        vec = model.encode([text], normalize_embeddings=True)[0]
        return [float(x) for x in vec]
    except Exception:  # noqa: BLE001
        return None
