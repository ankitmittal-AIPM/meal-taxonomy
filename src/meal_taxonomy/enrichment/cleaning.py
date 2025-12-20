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


def clean_meal_name(name: Optional[str]) -> Optional[str]:
    if not isinstance(name, str):
        return name

    n = name.strip()

    # Remove "(something recipe)" noise
    n = re.sub(r"\([^)]*recipe[^)]*\)", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\brecipe\b", "", n, flags=re.IGNORECASE)

    # Collapse spaces and trim dashes
    n = re.sub(r"\s+", " ", n)
    n = n.strip(" -\u2013\u2014")

    # Common standardizations (extend as you discover)
    n = re.sub(r"\bIdly\b", "Idli", n)
    n = re.sub(r"\bchat\b", "Chaat", n, flags=re.IGNORECASE)

    return n.strip() or None


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
