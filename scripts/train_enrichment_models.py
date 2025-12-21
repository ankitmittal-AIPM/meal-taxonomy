#!/usr/bin/env python3
"""Train Layer-1 enrichment models (scikit-learn) and export to models_store/.

This script is intentionally flexible:
  - train from a local CSV
  - OR train from a HuggingFace dataset (optional)

Examples:

  # Train from CSV with default column names
  python scripts/train_enrichment_models.py \
    --input_csv data/indian_recipe_dataset.csv \
    --models_dir models_store

  # Train from HuggingFace dataset (requires: pip install datasets)
  python scripts/train_enrichment_models.py \
    --hf_dataset_name your_org/indian_recipe_dataset \
    --split train \
    --models_dir models_store

Column defaults expected (override with flags if needed):
  - RecipeName
  - Ingredients
  - Instructions
  - Course
  - Diet
  - Cuisine
  - PrepTimeInMins
  - CookTimeInMins
  - TotalTimeInMins
"""

from __future__ import annotations

import argparse
import json
import os
from typing import List, Optional, Tuple

import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import MultiLabelBinarizer, FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.metrics import classification_report, mean_absolute_error
import joblib


def _build_text(df: pd.DataFrame, col_name: str, col_ing: str, col_inst: str) -> pd.Series:
    return (
        df[col_name].fillna("").astype(str)
        + "\n"
        + df[col_ing].fillna("").astype(str)
        + "\n"
        + df[col_inst].fillna("").astype(str)
    )


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def train_multiclass_text_clf(X: List[str], y: List[str]) -> Pipeline:
    return Pipeline(
        steps=[
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2), min_df=2, max_features=120_000)),
            ("clf", LogisticRegression(max_iter=2000, n_jobs=-1)),
        ]
    )


def train_times_regressor(df: pd.DataFrame) -> Tuple[Pipeline, Pipeline]:
    # Features: text + total_time (numeric)
    preprocessor = ColumnTransformer(
        transformers=[
            ("text", TfidfVectorizer(ngram_range=(1, 2), min_df=2, max_features=120_000), "text"),
            ("total", FunctionTransformer(lambda x: x.fillna(0).astype(float).to_numpy().reshape(-1, 1)), "total_time"),
        ],
        remainder="drop",
    )
    prep_model = Pipeline(steps=[("features", preprocessor), ("reg", Ridge(alpha=1.0))])
    cook_model = Pipeline(steps=[("features", preprocessor), ("reg", Ridge(alpha=1.0))])
    return prep_model, cook_model


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_csv", type=str, default=None)
    ap.add_argument("--hf_dataset_name", type=str, default=None)
    ap.add_argument("--split", type=str, default="train")
    ap.add_argument("--models_dir", type=str, default="models_store")

    # Column overrides
    ap.add_argument("--col_recipe_name", type=str, default="RecipeName")
    ap.add_argument("--col_ingredients", type=str, default="Ingredients")
    ap.add_argument("--col_instructions", type=str, default="Instructions")
    ap.add_argument("--col_course", type=str, default="Course")
    ap.add_argument("--col_diet", type=str, default="Diet")
    ap.add_argument("--col_cuisine", type=str, default="Cuisine")
    ap.add_argument("--col_prep", type=str, default="PrepTimeInMins")
    ap.add_argument("--col_cook", type=str, default="CookTimeInMins")
    ap.add_argument("--col_total", type=str, default="TotalTimeInMins")

    # Optional health tags column (comma-separated)
    ap.add_argument("--col_health_tags", type=str, default=None)

    args = ap.parse_args()

    if not args.input_csv and not args.hf_dataset_name:
        raise SystemExit("Provide either --input_csv or --hf_dataset_name")

    if args.input_csv:
        df = pd.read_csv(args.input_csv)
    else:
        try:
            from datasets import load_dataset  # type: ignore
        except Exception as exc:
            raise SystemExit("datasets not installed. pip install datasets") from exc
        ds = load_dataset(args.hf_dataset_name, split=args.split)
        df = ds.to_pandas()

    # Normalize required columns
    needed = [
        args.col_recipe_name,
        args.col_ingredients,
        args.col_instructions,
        args.col_course,
        args.col_diet,
        args.col_cuisine,
        args.col_prep,
        args.col_cook,
        args.col_total,
    ]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise SystemExit(f"Missing columns: {missing}. Use --col_* overrides.")

    df = df.copy()
    df["text"] = _build_text(df, args.col_recipe_name, args.col_ingredients, args.col_instructions)
    df["total_time"] = pd.to_numeric(df[args.col_total], errors="coerce")

    _ensure_dir(args.models_dir)

    # 1) Course / meal-time classifier
    X = df["text"].fillna("").astype(str).tolist()
    y_course = df[args.col_course].fillna("").astype(str).tolist()
    X_train, X_test, y_train, y_test = train_test_split(X, y_course, test_size=0.2, random_state=42, stratify=y_course)

    course_clf = train_multiclass_text_clf(X_train, y_train)
    course_clf.fit(X_train, y_train)
    preds = course_clf.predict(X_test)
    print("\n=== Course classifier ===")
    print(classification_report(y_test, preds, zero_division=0))
    joblib.dump(course_clf, os.path.join(args.models_dir, "course_clf.joblib"))

    # 2) Diet classifier
    y_diet = df[args.col_diet].fillna("").astype(str).tolist()
    X_train, X_test, y_train, y_test = train_test_split(X, y_diet, test_size=0.2, random_state=42, stratify=y_diet)
    diet_clf = train_multiclass_text_clf(X_train, y_train)
    diet_clf.fit(X_train, y_train)
    preds = diet_clf.predict(X_test)
    print("\n=== Diet classifier ===")
    print(classification_report(y_test, preds, zero_division=0))
    joblib.dump(diet_clf, os.path.join(args.models_dir, "diet_clf.joblib"))

    # 3) Region / cuisine classifier
    y_region = df[args.col_cuisine].fillna("").astype(str).tolist()
    X_train, X_test, y_train, y_test = train_test_split(X, y_region, test_size=0.2, random_state=42, stratify=y_region)
    region_clf = train_multiclass_text_clf(X_train, y_train)
    region_clf.fit(X_train, y_train)
    preds = region_clf.predict(X_test)
    print("\n=== Region classifier ===")
    print(classification_report(y_test, preds, zero_division=0))
    joblib.dump(region_clf, os.path.join(args.models_dir, "region_clf.joblib"))

    # 4) Prep/Cook time regressors
    prep_model, cook_model = train_times_regressor(df[["text", "total_time", args.col_prep, args.col_cook]].rename(columns={args.col_prep: "prep", args.col_cook: "cook"}))
    # Drop rows with missing targets
    df_time = df[["text", "total_time", args.col_prep, args.col_cook]].copy()
    df_time["prep"] = pd.to_numeric(df_time[args.col_prep], errors="coerce")
    df_time["cook"] = pd.to_numeric(df_time[args.col_cook], errors="coerce")
    df_time = df_time.dropna(subset=["prep", "cook"])

    X_time = df_time[["text", "total_time"]]
    y_prep = df_time["prep"]
    y_cook = df_time["cook"]
    X_tr, X_te, y_tr, y_te = train_test_split(X_time, y_prep, test_size=0.2, random_state=42)
    prep_model.fit(X_tr, y_tr)
    pred_p = prep_model.predict(X_te)
    print("\n=== Prep time regressor ===")
    print("MAE:", mean_absolute_error(y_te, pred_p))
    joblib.dump(prep_model, os.path.join(args.models_dir, "prep_time_reg.joblib"))

    X_tr, X_te, y_tr, y_te = train_test_split(X_time, y_cook, test_size=0.2, random_state=42)
    cook_model.fit(X_tr, y_tr)
    pred_c = cook_model.predict(X_te)
    print("\n=== Cook time regressor ===")
    print("MAE:", mean_absolute_error(y_te, pred_c))
    joblib.dump(cook_model, os.path.join(args.models_dir, "cook_time_reg.joblib"))

    # 5) Optional health multi-label
    if args.col_health_tags and args.col_health_tags in df.columns:
        # Parse comma-separated tags
        tags = df[args.col_health_tags].fillna("").astype(str).apply(lambda s: [t.strip() for t in s.split(",") if t.strip()])
        mlb = MultiLabelBinarizer()
        Y = mlb.fit_transform(tags)

        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

        health_clf = Pipeline(
            steps=[
                ("tfidf", TfidfVectorizer(ngram_range=(1, 2), min_df=2, max_features=120_000)),
                ("clf", OneVsRestClassifier(LogisticRegression(max_iter=2000, n_jobs=-1))),
            ]
        )
        health_clf.fit(X_train, Y_train)

        joblib.dump(health_clf, os.path.join(args.models_dir, "health_multilabel.joblib"))
        with open(os.path.join(args.models_dir, "health_labels.json"), "w", encoding="utf-8") as f:
            json.dump(list(mlb.classes_), f, ensure_ascii=False, indent=2)

        print("\nSaved health_multilabel.joblib + health_labels.json")

    print(f"\nSaved models to: {args.models_dir}")


if __name__ == "__main__":
    main()
