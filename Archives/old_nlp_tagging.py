
# To keep things beginner-friendly, let’s start with a stub NLP that only does time bucketing and later you can plug the full TASTEset model.
# Once the base pipeline works, we can upgrade this to the full transformers-based version.

# Below now converted to full transformers-based NLP tagging.

"""
What this gives you now:
If the model sees “vegan mayonnaise, finely chopped garlic, spicy sauce” in ingredients, it can auto-tag:
a. diet / vegan
b. taste_profile / spicy
c. technique / chopped
Even if your dataset CSV never had explicit diet or taste columns.
"""

# nlp_tagging.py
# nlp_tagging.py

# nlp_tagging.py

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

# Hugging Face imports – we handle absence gracefully
try:
    from transformers import (
        AutoTokenizer,
        AutoModelForTokenClassification,
        pipeline,
    )
except ImportError:  # transformers not installed
    AutoTokenizer = None
    AutoModelForTokenClassification = None
    pipeline = None


@dataclass
class TagCandidate:
    tag_type: str
    value: str
    label_en: str
    confidence: float = 1.0
    is_primary: bool = False
    label_hi: Optional[str] = None
    label_hinglish: Optional[str] = None


class RecipeNLP:
    """
    NLP helper using a HuggingFace NER model trained on recipe text.
    It extracts entities like DIET, TASTE, PROCESS from ingredient text and
    maps them into your tag taxonomy.
    """

    # You can change this to another TASTEset-style NER model if you want.
    MODEL_NAME = "dmargutierrez/distilbert-base-uncased-TASTESet-ner"

    def __init__(self, model_name: str | None = None) -> None:
        self.model_name = model_name or self.MODEL_NAME
        self._ner = None

        if pipeline is None:
            print(
                "[RecipeNLP] transformers is not installed; "
                "NLP tagging will be disabled."
            )
            return

        try:
            tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            model = AutoModelForTokenClassification.from_pretrained(self.model_name)
            self._ner = pipeline(
                "token-classification",
                model=model,
                tokenizer=tokenizer,
                aggregation_strategy="simple",  # merge sub-tokens into spans
            )
            print(f"[RecipeNLP] Loaded NER model: {self.model_name}")
        except Exception as exc:  # noqa: BLE001
            print(f"[RecipeNLP] Failed to load HF model {self.model_name}: {exc}")
            self._ner = None

    # ------------------------------------------------------------------
    # Time bucketing – already used in pipeline.dataset_tags()
    # ------------------------------------------------------------------
    def bucket_time(self, total_minutes: int) -> Optional[TagCandidate]:
        """
        Turn numeric total_time_minutes into a time_bucket tag.
        """
        if total_minutes is None:
            return None

        if total_minutes <= 15:
            value = "under_15_min"
            label = "Under 15 min"
        elif total_minutes <= 30:
            value = "under_30_min"
            label = "Under 30 min"
        elif total_minutes <= 60:
            value = "under_60_min"
            label = "Under 60 min"
        else:
            value = "over_60_min"
            label = "Over 60 min"

        return TagCandidate(
            tag_type="time_bucket",
            value=value,
            label_en=label,
            confidence=1.0,
            is_primary=False,
        )

    # ------------------------------------------------------------------
    # NER → TagCandidate mapping
    # ------------------------------------------------------------------
    def _ner_available(self) -> bool:
        return self._ner is not None

    def _map_entity_to_tag(
        self,
        label_raw: str,
        text: str,
        score: float,
    ) -> Optional[TagCandidate]:
        """
        Map a NER label (e.g. DIET, TASTE, PROCESS) to your tag taxonomy.
        """
        label_norm = (label_raw or "").upper().replace(" ", "_")
        value = text.lower().strip().replace(" ", "_")
        if not value:
            return None

        # DIET → diet (vegan, vegetarian, etc.)
        if label_norm == "DIET":
            return TagCandidate(
                tag_type="diet",
                value=value,
                label_en=text.strip(),
                confidence=score,
                is_primary=True,
            )

        # TASTE → taste_profile (spicy, sweet, tangy...)
        if label_norm == "TASTE":
            return TagCandidate(
                tag_type="taste_profile",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        # PROCESS → technique (fried, baked, grilled...)
        if label_norm == "PROCESS":
            return TagCandidate(
                tag_type="technique",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        # PHYSICAL_QUALITY → ingredient_quality (fresh, frozen...)
        if label_norm in {"PHYSICAL_QUALITY", "PHYSICALQUALITY"}:
            return TagCandidate(
                tag_type="ingredient_quality",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        # COLOR → color
        if label_norm == "COLOR":
            return TagCandidate(
                tag_type="color",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        # Other labels (FOOD, UNIT, QUANTITY, etc.) we ignore for now
        return None

    def nlp_tags_for_recipe(self, ingredients: Sequence[str]) -> List[TagCandidate]:
        """
        Run NER on the list of ingredient strings and produce TagCandidate objects
        for DIET, TASTE, PROCESS, etc.
        """
        if not self._ner_available():
            return []

        # Join ingredients with newlines – works well for list-style ingredient input
        text = "\n".join(
            i for i in ingredients
            if i and str(i).strip()
        )
        if not text:
            return []

        results = self._ner(text)
        tags: list[TagCandidate] = []

        for ent in results:
            # For HF aggregation_strategy="simple", entity_group holds the merged label
            label = (
                ent.get("entity_group")
                or ent.get("entity")
                or ""
            )
            word = ent.get("word") or ent.get("entity") or ""
            score = float(ent.get("score") or 0.0)

            cand = self._map_entity_to_tag(label, word, score)
            if cand is not None:
                tags.append(cand)

        return tags

