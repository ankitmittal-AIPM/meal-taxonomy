# src/meal_taxonomy/enrichment/ml_models.py
from __future__ import annotations

"""
ml_models.py

Purpose:
    Layer-1 supervised enrichment models.

These models are intentionally designed to be:
  - small,
  - fast,
  - deployable without GPUs (scikit-learn).

They should be trained on Indian recipe datasets (HuggingFace / Kaggle / your CSVs)
and exported as joblib pipelines.

Expected artifacts (defaults, configurable):
  - course_clf.joblib          (multi-class)
  - diet_clf.joblib            (multi-class)
  - region_clf.joblib          (multi-class; labels can be region paths like "South Indian|Karnataka|Udupi")
  - spice_clf.joblib           (classification or regression; mapped to 1..5)
  - health_multilabel.joblib   (multi-label probabilities)
  - prep_time_reg.joblib       (regressor)
  - cook_time_reg.joblib       (regressor)

If a file is missing, we log once and gracefully skip.
"""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("enrichment_ml_models")


def _safe_import_joblib():
    try:
        import joblib  # type: ignore
    except Exception:  # noqa: BLE001
        return None
    return joblib


@dataclass
class MLLabel:
    value: str
    confidence: float


@dataclass
class MLTimes:
    prep_time_mins: Optional[float]
    cook_time_mins: Optional[float]


class IndianMLModels:
    """Loads and serves Layer-1 ML models (sklearn joblib pipelines)."""

    def __init__(self, models_dir: str = "models_store") -> None:
        self.models_dir = models_dir
        self._joblib = _safe_import_joblib()

        self.course_clf = self._load("course_clf.joblib")
        self.diet_clf = self._load("diet_clf.joblib")
        self.region_clf = self._load("region_clf.joblib")
        self.spice_model = self._load("spice_clf.joblib")
        self.health_model = self._load("health_multilabel.joblib")
        self.prep_reg = self._load("prep_time_reg.joblib")
        self.cook_reg = self._load("cook_time_reg.joblib")

        # If your multi-label model needs class names, store them here.
        self.health_labels: List[str] = self._load_json_labels("health_labels.json")

    def enabled(self) -> bool:
        return self._joblib is not None

    # ------------------------------------------------------------------
    # Public predictions
    # ------------------------------------------------------------------
    def predict_course(self, text: str) -> Optional[MLLabel]:
        return self._predict_multiclass(self.course_clf, text)

    def predict_diet(self, text: str) -> Optional[MLLabel]:
        return self._predict_multiclass(self.diet_clf, text)

    def predict_region(self, text: str) -> Optional[MLLabel]:
        return self._predict_multiclass(self.region_clf, text)

    def predict_spice_level_1_to_5(self, text: str) -> Optional[MLLabel]:
        """Returns spice_level as '1'..'5' (string) + confidence."""
        if self.spice_model is None:
            return None

        # Classification pipeline with predict_proba
        if hasattr(self.spice_model, "predict_proba"):
            try:
                proba = self.spice_model.predict_proba([text])[0]
                classes = getattr(self.spice_model, "classes_", None)
                if classes is None:
                    # Some sklearn pipelines expose classes_ on last step only
                    classes = getattr(self.spice_model[-1], "classes_", None)  # type: ignore[index]
                if classes is None:
                    # Fallback: pick argmax and map to 3
                    best_idx = int(proba.argmax())
                    return MLLabel(value=str(best_idx + 1), confidence=float(proba[best_idx]))
                best_idx = int(proba.argmax())
                best_class = str(classes[best_idx])
                # If model emits Low/Medium/High map to 2/3/5
                mapped = self._map_spice_label(best_class)
                return MLLabel(value=str(mapped), confidence=float(proba[best_idx]))
            except Exception:  # noqa: BLE001
                return None

        # Regression pipeline: output numeric (assume 1..5)
        if hasattr(self.spice_model, "predict"):
            try:
                y = float(self.spice_model.predict([text])[0])
                y = max(1.0, min(5.0, y))
                return MLLabel(value=str(int(round(y))), confidence=0.55)  # regression has no proba
            except Exception:  # noqa: BLE001
                return None

        return None

    def predict_health_tags(self, text: str, threshold: float = 0.45) -> List[MLLabel]:
        """Multi-label prediction. Returns list of tags above threshold."""
        if self.health_model is None:
            return []

        try:
            if hasattr(self.health_model, "predict_proba"):
                proba = self.health_model.predict_proba([text])[0]
                # If labels not provided, we can't map outputs; return empty
                if not self.health_labels or len(self.health_labels) != len(proba):
                    return []
                out: List[MLLabel] = []
                for lbl, p in zip(self.health_labels, proba):
                    if float(p) >= threshold:
                        out.append(MLLabel(value=str(lbl), confidence=float(p)))
                # Sort descending confidence
                out.sort(key=lambda x: x.confidence, reverse=True)
                return out
        except Exception:  # noqa: BLE001
            return []

        return []

    # Invoked Address : Apply_ML and Enrich function from enrichment_pipeline.py
    # Predicts the preparation and Cook time of the meal based on meal information
    def predict_prep_cook_time(self, text: str) -> MLTimes:
        prep = self._predict_regression(self.prep_reg, text)
        cook = self._predict_regression(self.cook_reg, text)
        return MLTimes(prep_time_mins=prep, cook_time_mins=cook)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load(self, filename: str):
        if self._joblib is None:
            logger.warning(
                "joblib not installed; ML enrichment disabled.",
                extra={
                    "invoking_func": "IndianMLModels._load",
                    "invoking_purpose": "Load ML model artifact",
                    "next_step": "Install optional deps (joblib, scikit-learn) if needed",
                    "resolution": "",
                },
            )
            return None

        path = os.path.join(self.models_dir, filename)
        if not os.path.exists(path):
            return None
        try:
            return self._joblib.load(path)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to load ML model '%s': %s",
                path,
                exc,
                extra={
                    "invoking_func": "IndianMLModels._load",
                    "invoking_purpose": "Load ML model artifact",
                    "next_step": "Check joblib file integrity / sklearn version",
                    "resolution": "",
                },
            )
            return None

    def _load_json_labels(self, filename: str) -> List[str]:
        path = os.path.join(self.models_dir, filename)
        if not os.path.exists(path):
            return []
        try:
            import json
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return [str(x) for x in data]
        except Exception:  # noqa: BLE001
            return []
        return []

    def _predict_multiclass(self, model, text: str) -> Optional[MLLabel]:
        if model is None:
            return None
        try:
            if hasattr(model, "predict_proba"):
                proba = model.predict_proba([text])[0]
                classes = getattr(model, "classes_", None)
                if classes is None:
                    classes = getattr(model[-1], "classes_", None)  # type: ignore[index]
                best_idx = int(proba.argmax())
                best_class = str(classes[best_idx]) if classes is not None else str(best_idx)
                return MLLabel(value=best_class, confidence=float(proba[best_idx]))
            # Fallback: no proba
            pred = str(model.predict([text])[0])
            return MLLabel(value=pred, confidence=0.55)
        except Exception:  # noqa: BLE001
            return None

    # Invoked Address : Currently called from predicting preparation and cook time
    # Regression ML pipeline
    def _predict_regression(self, model, text: str) -> Optional[float]:
        if model is None:
            return None
        try:
            y = float(model.predict([text])[0])
            if y < 0:
                return None
            return y
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _map_spice_label(label: str) -> int:
        l = label.strip().lower()
        if l in {"low", "mild"}:
            return 2
        if l in {"medium", "med"}:
            return 3
        if l in {"high", "hot"}:
            return 5
        # If already numeric
        try:
            n = int(float(l))
            return max(1, min(5, n))
        except Exception:  # noqa: BLE001
            return 3
