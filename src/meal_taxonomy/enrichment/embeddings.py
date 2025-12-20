# src/meal_taxonomy/enrichment/embeddings.py
from __future__ import annotations

"""
embeddings.py

Purpose:
    Provide a single function to compute embeddings for meals.

    For now this is a thin wrapper around any model you choose:
      - OpenAI embeddings,
      - sentence-transformers,
      - etc.

    The important part: always return a Python list[float] that
    can be stored in Supabase (pgvector) or used for FAISS.
"""

from typing import List


def get_meal_embedding(text: str) -> List[float]:
    # TODO: Replace this stub with your actual embedding provider.
    # Example (OpenAI):
    #
    #   from openai import OpenAI
    #   client = OpenAI()
    #   resp = client.embeddings.create(
    #       model="text-embedding-3-small",
    #       input=text,
    #   )
    #   return resp.data[0].embedding
    #
    # For now return an empty list to avoid crashes.
    return []
