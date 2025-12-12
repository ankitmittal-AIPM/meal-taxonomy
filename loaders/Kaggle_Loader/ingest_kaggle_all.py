# ingest_kaggle_all.py

from __future__ import annotations

import glob
import os

from config import get_supabase_client
from pipeline import MealETL
from datasets.kaggle_unified import load_kaggle_csv


def ingest_folder(folder: str = "data/kaggle") -> None:
    client = get_supabase_client()
    etl = MealETL(client)

    pattern = os.path.join(folder, "*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"[Kaggle] No CSV files found in {folder}")
        return

    print(f"[Kaggle] Found {len(files)} CSV file(s):")
    for f in files:
        print("  -", f)

    total_recipes = 0

    for fpath in files:
        dataset_name = os.path.splitext(os.path.basename(fpath))[0]
        print(f"\n[Kaggle] Loading {fpath} as dataset '{dataset_name}'")
        try:
            recipes = load_kaggle_csv(fpath, dataset_name=dataset_name)
        except Exception as exc:  # noqa: BLE001
            print(f"[Kaggle] Failed to load {fpath}: {exc}")
            continue

        print(f"[Kaggle] Ingesting {len(recipes)} recipes from {dataset_name} ...")

        for idx, rec in enumerate(recipes):
            try:
                etl.ingest_recipe(rec, index=idx)
            except Exception as exc:  # noqa: BLE001
                print(f"[Kaggle] Error ingesting recipe '{rec.title}': {exc}")

        total_recipes += len(recipes)

    print(f"\n[Kaggle] Finished ingesting all datasets. Total recipes: {total_recipes}")


if __name__ == "__main__":
    ingest_folder("data/kaggle")
