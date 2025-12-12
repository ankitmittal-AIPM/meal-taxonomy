
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

VErsion 2 -->
What changed vs. before:

Looks at title + instructions as well (once we wire it in).
Adds rule-based tagging for:
a. diet (vegan, vegetarian, gluten_free, keto)
b. taste_profile (spicy, sweet, tangy, savory)
c. technique (fried, baked, steamed, grilled, pressure_cooked)
d. dish_type (curry, salad, soup, rice_dish, bread, snack)
e. nutrition_profile (high_protein, low_carb, high_fiber)

Still uses NER where possible, but treats it as an extra layer.
"""

# nlp_tagging.py
# nlp_tagging.py

# nlp_tagging.py

# nlp_tagging.py

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

# Hugging Face imports – optional
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
    NLP for recipes combining:
      1) A HuggingFace NER model (TASTEset-style) when available
      2) Rule-based keyword tagging for Indian + global recipes

    It outputs TagCandidate objects that pipeline.py turns into tags + meal_tags.
    """

    MODEL_NAME = "dmargutierrez/distilbert-base-uncased-TASTESet-ner"

    def __init__(self, model_name: str | None = None) -> None:
        self.model_name = model_name or self.MODEL_NAME
        self._ner = None

        if pipeline is None:
            print("[RecipeNLP] transformers not installed; only rule-based tags will be used.")
        else:
            try:
                tok = AutoTokenizer.from_pretrained(self.model_name)
                model = AutoModelForTokenClassification.from_pretrained(self.model_name)
                self._ner = pipeline(
                    "token-classification",
                    model=model,
                    tokenizer=tok,
                    aggregation_strategy="simple",
                )
                print(f"[RecipeNLP] Loaded HF NER model: {self.model_name}")
            except Exception as exc:  # noqa: BLE001
                print(f"[RecipeNLP] Failed to load HF model {self.model_name}: {exc}")
                self._ner = None

    # ------------------------------------------------------------------
    # Time bucketing (used in pipeline.dataset_tags)
    # ------------------------------------------------------------------
    def bucket_time(self, total_minutes: int | None) -> Optional[TagCandidate]:
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
    # Rule-based keyword tagging
    # ------------------------------------------------------------------
    # Diet keywords
    DIET_KEYWORDS = {
        "vegan": [
            "vegan",
            "plant-based",
            "plant based",
            "dairy-free",
            "dairy free",
        ],
        "vegetarian": [
            "vegetarian",
            "veg ",
            "veg.",
            "paneer ",
            "paneer,",
            "paneer-",
        ],
        "gluten_free": [
            "gluten-free",
            "gluten free",
            "no gluten",
        ],
        "keto": [
            "keto",
            "low carb",
            "low-carb",
        ],
    }
    DIET_LABELS = {
        "vegan": "Vegan",
        "vegetarian": "Vegetarian",
        "gluten_free": "Gluten free",
        "keto": "Keto / Low carb",
    }

    # Taste profile keywords
    TASTE_KEYWORDS = {
        "spicy": [
            "spicy",
            "extra spicy",
            "very spicy",
            "fiery",
            "hot and spicy",
            "red chilli",
            "red chili",
            "green chilli",
            "green chili",
            "chilli powder",
            "chili powder",
            "mirchi",
        ],
        "sweet": [
            "sweet",
            "sugar",
            "jaggery",
            "gud",
            "honey",
            "condensed milk",
        ],
        "tangy": [
            "tangy",
            "sour",
            "chatpata",
            "amchur",
            "lemon juice",
            "lime juice",
            "imli",
            "tamarind",
        ],
        "savory": [
            "savory",
            "umami",
            "rich gravy",
        ],
    }
    TASTE_LABELS = {
        "spicy": "Spicy",
        "sweet": "Sweet",
        "tangy": "Tangy / Chatpata",
        "savory": "Savory / Umami",
    }

    # Technique / cooking method
    TECHNIQUE_KEYWORDS = {
        "fried": [
            "deep fry",
            "deep-fry",
            "shallow fry",
            "shallow-fry",
            "stir fry",
            "stir-fry",
            "fried",
            "fry until",
            "fry till",
            "bhuna",
            "bhunao",
        ],
        "baked": [
            "bake",
            "baked",
            "oven-baked",
            "preheated oven",
        ],
        "steamed": [
            "steam",
            "steamed",
            "idli moulds",
            "idli molds",
            "steamer",
        ],
        "grilled": [
            "grill",
            "grilled",
            "tandoori",
            "tandoor",
            "barbecue",
            "bbq",
        ],
        "pressure_cooked": [
            "pressure cook",
            "pressure-cook",
            "whistles",
            "1 whistle",
            "2 whistles",
        ],
    }
    TECHNIQUE_LABELS = {
        "fried": "Fried / stir-fried",
        "baked": "Baked",
        "steamed": "Steamed",
        "grilled": "Grilled / tandoori",
        "pressure_cooked": "Pressure cooked",
    }

    # Dish type
    DISH_TYPE_KEYWORDS = {
        "curry": [
            "curry",
            "masala curry",
            "dal tadka",
            "dal fry",
            "sabzi",
            "gravy",
        ],
        "salad": [
            "salad",
        ],
        "soup": [
            "soup",
            "shorba",
        ],
        "rice_dish": [
            "biryani",
            "pulao",
            "fried rice",
            "jeera rice",
            "lemon rice",
            "curd rice",
        ],
        "bread": [
            "roti",
            "chapati",
            "paratha",
            "naan",
            "kulcha",
            "poori",
            "puri",
            "sandwich",
            "wrap",
        ],
        "snack": [
            "tikki",
            "cutlet",
            "kabab",
            "kebab",
            "pakora",
            "bhajiya",
            "fritter",
        ],
    }
    DISH_TYPE_LABELS = {
        "curry": "Curry / Sabzi",
        "salad": "Salad",
        "soup": "Soup",
        "rice_dish": "Rice dish",
        "bread": "Bread / flatbread / sandwich",
        "snack": "Snack / starter",
    }

    # Nutrition-ish labels
    NUTRITION_KEYWORDS = {
        "high_protein": [
            "high protein",
            "protein-rich",
            "protein rich",
        ],
        "low_carb": [
            "low carb",
            "keto",
        ],
        "high_fiber": [
            "high fiber",
            "high fibre",
            "fibre-rich",
            "fiber-rich",
        ],
    }
    NUTRITION_LABELS = {
        "high_protein": "High protein",
        "low_carb": "Low carb / keto",
        "high_fiber": "High fibre",
    }

    # ------------ Rule-based taggers ------------

    @staticmethod
    def _text_contains_any(text: str, keywords: list[str]) -> bool:
        return any(kw in text for kw in keywords)

    def rule_based_tags(self, text: str) -> List[TagCandidate]:
        """
        Heuristic tags from plain-text keywords (English / Hinglish).
        """
        text_l = text.lower()
        tags: list[TagCandidate] = []

        # Diet
        for value, kws in self.DIET_KEYWORDS.items():
            if self._text_contains_any(text_l, kws):
                tags.append(
                    TagCandidate(
                        tag_type="diet",
                        value=value,
                        label_en=self.DIET_LABELS[value],
                        confidence=0.9,
                        is_primary=value in ("vegan", "vegetarian"),
                    )
                )

        # Taste profile
        for value, kws in self.TASTE_KEYWORDS.items():
            if self._text_contains_any(text_l, kws):
                tags.append(
                    TagCandidate(
                        tag_type="taste_profile",
                        value=value,
                        label_en=self.TASTE_LABELS[value],
                        confidence=0.85,
                    )
                )

        # Techniques
        for value, kws in self.TECHNIQUE_KEYWORDS.items():
            if self._text_contains_any(text_l, kws):
                tags.append(
                    TagCandidate(
                        tag_type="technique",
                        value=value,
                        label_en=self.TECHNIQUE_LABELS[value],
                        confidence=0.85,
                    )
                )

        # Dish type (mostly from title / instructions)
        for value, kws in self.DISH_TYPE_KEYWORDS.items():
            if self._text_contains_any(text_l, kws):
                tags.append(
                    TagCandidate(
                        tag_type="dish_type",
                        value=value,
                        label_en=self.DISH_TYPE_LABELS[value],
                        confidence=0.8,
                        is_primary=(value in ("curry", "rice_dish", "snack")),
                    )
                )

        # Nutrition profile-ish
        for value, kws in self.NUTRITION_KEYWORDS.items():
            if self._text_contains_any(text_l, kws):
                tags.append(
                    TagCandidate(
                        tag_type="nutrition_profile",
                        value=value,
                        label_en=self.NUTRITION_LABELS[value],
                        confidence=0.8,
                    )
                )

        return tags

    # ------------------------------------------------------------------
    # NER-based tags
    # ------------------------------------------------------------------
    def _ner_available(self) -> bool:
        return self._ner is not None

    def _map_entity_to_tag(
        self,
        label_raw: str,
        text: str,
        score: float,
    ) -> Optional[TagCandidate]:
        label_norm = (label_raw or "").upper().replace(" ", "_")
        value = text.lower().strip().replace(" ", "_")
        if not value:
            return None

        if label_norm == "DIET":
            return TagCandidate(
                tag_type="diet",
                value=value,
                label_en=text.strip(),
                confidence=score,
                is_primary=True,
            )

        if label_norm == "TASTE":
            return TagCandidate(
                tag_type="taste_profile",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        if label_norm == "PROCESS":
            return TagCandidate(
                tag_type="technique",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        if label_norm in {"PHYSICAL_QUALITY", "PHYSICALQUALITY"}:
            return TagCandidate(
                tag_type="ingredient_quality",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        if label_norm == "COLOR":
            return TagCandidate(
                tag_type="color",
                value=value,
                label_en=text.strip(),
                confidence=score,
            )

        return None

    def ner_tags(self, text: str) -> List[TagCandidate]:
        if not self._ner_available():
            return []

        if not text.strip():
            return []

        results = self._ner(text)
        tags: list[TagCandidate] = []

        for ent in results:
            label = ent.get("entity_group") or ent.get("entity") or ""
            word = ent.get("word") or ent.get("entity") or ""
            score = float(ent.get("score") or 0.0)

            cand = self._map_entity_to_tag(label, word, score)
            if cand is not None:
                tags.append(cand)

        return tags

    # ------------------------------------------------------------------
    # Entry point used by pipeline.py
    # ------------------------------------------------------------------
    def nlp_tags_for_recipe(
        self,
        ingredients: Sequence[str],
        extra_text: Optional[str] = None,
    ) -> List[TagCandidate]:
        """
        Combine ingredients + extra recipe text (title, instructions) and
        return a richer set of TagCandidates.
        """
        ingredients_text = "\n".join(
            i for i in ingredients if i and str(i).strip()
        )
        full_text_parts = [ingredients_text]
        if extra_text:
            full_text_parts.append(extra_text)
        full_text = "\n".join(p for p in full_text_parts if p)

        if not full_text.strip():
            return []

        # 1) Rule-based tags (high recall for Indian-ish phrases)
        rule_tags = self.rule_based_tags(full_text)

        # 2) NER-based tags (if model available)
        ner_tags = self.ner_tags(full_text) if self._ner_available() else []

        # 3) Merge & deduplicate (keep highest confidence for each tag_type+value)
        merged: dict[tuple[str, str], TagCandidate] = {}

        for cand in rule_tags + ner_tags:
            key = (cand.tag_type, cand.value)
            existing = merged.get(key)
            if existing is None or cand.confidence > existing.confidence:
                merged[key] = cand

        return list(merged.values())


