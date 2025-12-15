
from __future__ import annotations
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

from dataclasses import dataclass
from typing import List, Optional, Sequence
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("nlp_tagging")

# Hugging Face imports – optional
# Import HF only if transformers is installed
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

# ----------------------------------------------------------------------
# TagCandidate dataclass that pipeline.py can consume
@dataclass
class TagCandidate:
    tag_type: str
    value: str
    label_en: str
    confidence: float = 1.0
    is_primary: bool = False
    label_hi: Optional[str] = None
    label_hinglish: Optional[str] = None

    # NEW FIELD – optional, default None, so all existing usages still work
    source: Optional[str] = None

# ----------------------------------------------------------------------
# RecipeNLP class that combines rule-based + NER-based tagging
class RecipeNLP:
    """
    NLP for recipes combining:
      1) A HuggingFace NER model (TASTEset-style) when available
      2) Rule-based keyword tagging for Indian + global recipes

    It outputs TagCandidate objects that pipeline.py turns into tags + meal_tags.
    """
    # Popular model trained on TASTEset for food NER. This has diet, taste, process, etc. trained on more than 100K recipe sentences.
    MODEL_NAME = "dmargutierrez/distilbert-base-uncased-TASTESet-ner"
    
    # Intialize RecipeNLP, optionally loading HF model. This will log and proceed if transformers not installed.
    # Purpose: Uses HuggingFace transformers to load a NER (Named Entity Recognition) model for recipe tagging.
    def __init__(self, model_name: str | None = None) -> None:
        self.model_name = model_name or self.MODEL_NAME
        self._ner = None

        if pipeline is None:
            # transformers not installed – log and proceed with rule-based only
            logger.warning(
                "transformers library not installed; NLP will use rule-based tags only",
                extra={
                    "invoking_func": "__init__",
                    "invoking_purpose": "Initialize RecipeNLP and optionally load HF NER model",
                    "next_step": "Proceed without loading HuggingFace model",
                    "resolution": (
                        "Install 'transformers' and 'torch' in the environment if NER is desired"
                    ),
                },
            )
            return

        # Try to load HuggingFace model
        try:
            logger.info(
                "Loading HuggingFace NER model '%s' for RecipeNLP",
                self.model_name,
                extra={
                    "invoking_func": "__init__",
                    "invoking_purpose": "Initialize RecipeNLP and optionally load HF NER model",
                    "next_step": "Download/load tokenizer and model, build pipeline()",
                    "resolution": "",
                },
            )

            tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            model = AutoModelForTokenClassification.from_pretrained(self.model_name)
            self._ner = pipeline(
                "token-classification",
                model=model,
                tokenizer=tokenizer,
                aggregation_strategy="simple",
            )

            logger.info(
                "Successfully loaded HuggingFace NER model '%s'",
                self.model_name,
                extra={
                    "invoking_func": "__init__",
                    "invoking_purpose": "Initialize RecipeNLP and optionally load HF NER model",
                    "next_step": "Use NER alongside rule-based tagging in nlp_tags_for_recipe",
                    "resolution": "",
                },
            )

        except Exception as exc:  # noqa: BLE001
            # Log failure but keep rule-based tagging available
            logger.error(
                "Failed to load HuggingFace NER model '%s': %s",
                self.model_name,
                exc,
                extra={
                    "invoking_func": "__init__",
                    "invoking_purpose": "Initialize RecipeNLP and optionally load HF NER model",
                    "next_step": "Disable NER and use only rule-based tags",
                    "resolution": (
                        "Check internet / HF model availability; verify model name; "
                        "fix environment and retry if NER is needed"
                    ),
                },
                exc_info=True,
            )
            self._ner = None

    # ------------------------------------------------------------------
    # Time bucketing (used in pipeline.dataset_tags).
    # Purpose: Simple bucketing of total time into under_15_min, under_30_min, etc.
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
    # Purpose: Heuristic tags from plain-text keywords (English / Hinglish).
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
    # This is a helper to check if any keyword is in text and accordingly add tags.
    @staticmethod
    def _text_contains_any(text: str, keywords: list[str]) -> bool:
        return any(kw in text for kw in keywords)

    # Purpose: This function scans the text for keywords defined above and generates TagCandidate objects.
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
    # Purpose: Use HuggingFace NER model to extract tags from text and map to TagCandidate.
    # ------------------------------------------------------------------
    def _ner_available(self) -> bool:
        return self._ner is not None

    # Purpose: Map NER entity labels to TagCandidate objects and also assign tag_type.
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

    # Purpose: Run NER on text and return list of TagCandidate objects.
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
    # Purpose: Combine ingredients + extra recipe text (title, instructions) and return a richer set of TagCandidates.
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

        final_tags = list(merged.values())

        logger.debug(
            "Generated %d NLP tags (rule-based=%d, ner=%d)",
            len(final_tags),
            len(rule_tags),
            len(ner_tags),
            extra={
                "invoking_func": "nlp_tags_for_recipe",
                "invoking_purpose": "Derive TagCandidate objects from ingredients + text",
                "next_step": "Return tags to caller (MealETL.nlp_tags)",
                "resolution": "",
            },
        )
        return final_tags


