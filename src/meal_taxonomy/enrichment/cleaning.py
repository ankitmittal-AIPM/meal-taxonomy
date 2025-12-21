# src/meal_taxonomy/enrichment/cleaning.py
from __future__ import annotations

"""
cleaning.py

Purpose:
    Deterministic cleaning utilities for meal names, ingredients,
    and instructions.

    This is the "Layer 0" of Meal Enrichment before ML/LLM.
"""

import re
from typing import Optional


_COMMON_REPLACEMENTS = [
    (r"\brecipe\b", ""),
    (r"\bidly\b", "idli"),
    (r"\bdahi\b", "curd"),
    (r"\bmirchi\b", "chilli"),
    (r"\bchilli powder\b", "red chilli powder"),
]

def clean_meal_name(name: Optional[str]) -> Optional[str]:
    if not isinstance(name, str):
        return None
    t = name.strip()
    if not t:
        return None
    # Remove common junk
    t = re.sub(r"\b(recipe|authentic|best|easy|quick)\b", "", t, flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()
    
    # Remove any parentheses that contain the word "recipe"
    t = re.sub(r"\([^)]*recipe[^)]*\)", "", t, flags=re.IGNORECASE)

    # Remove standalone word "Recipe"
    t = re.sub(r"\brecipe\b", "", t, flags=re.IGNORECASE)

    # Collapse multiple spaces
    t = re.sub(r"\s+", " ", t)

    # Remove hyphens/dashes at start or end
    t = t.strip(" -\u2013\u2014")

    # Apply common replacements
    for pat, rep in _COMMON_REPLACEMENTS:
        t = re.sub(pat, rep, t, flags=re.IGNORECASE)
    
    return t


def normalize_ingredients(text: Optional[str]) -> str:
    if not isinstance(text, str):
        return ""
    t = text.replace("\r", " ")
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    t = " ".join(lines)
    t = re.sub(r"\s+", " ", t)
    return t.strip().lower()


def normalize_instructions(text: Optional[str]) -> str:
    if not isinstance(text, str):
        return ""
    t = text.replace("\r", " ")
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    t = " ".join(lines)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def normalize_title(title: Optional[str]) -> str:
    """Normalize a meal title for search / dedupe.

    Why:
        - `meals.title` is for display.
        - `meals.title_normalized` is for indexing (trigram / FTS fallback) and
          deterministic equality checks.

    Behavior:
        - cleans obvious junk words (via clean_meal_name),
        - lowercases,
        - strips most punctuation,
        - collapses whitespace.

    Notes:
        - We intentionally keep digits (e.g., "2-minute noodles").
        - This is a *light* normalizer. Do not over-stem; keep it readable.
    """
    if not isinstance(title, str):
        return ""
    cleaned = clean_meal_name(title) or title
    t = cleaned.lower()
    # Replace non-alphanumeric (incl. underscores) with spaces.
    t = re.sub(r"[^a-z0-9]+", " ", t)
    # Collapse multiple spaces.
    t = re.sub(r"\s+", " ", t)
    return t.strip()

def split_ingredient_lines(txt: str) -> list[str]:
    """Best-effort split of ingredient list into lines/tokens."""
    if not txt:
        return []
    # Split common separators
    parts = re.split(r"[,\n;]+", txt)
    out = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        out.append(p)
    return out