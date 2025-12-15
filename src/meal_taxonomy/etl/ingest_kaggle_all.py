"""
ingest_kaggle_all.py

Purpose:
    Batch-ingest all Kaggle CSV files under a folder (default: data/kaggle)
    using the unified Kaggle loader and the MealETL pipeline.

Usage:
    from meal_taxonomy.etl.ingest_kaggle_all import ingest_folder
    ingest_folder("data/kaggle")
"""

from __future__ import annotations

import glob
import os

from src.meal_taxonomy.config import get_supabase_client
from etl.pipeline import MealETL
from datasets.kaggle_unified import load_kaggle_csv
from src.meal_taxonomy.logging_utils import get_logger

MODULE_PURPOSE = (
    "Batch ingestion of multiple Kaggle CSV files into Supabase using MealETL."
)

logger = get_logger("ingest_kaggle_all")

def ingest_folder(folder: str = "data/kaggle") -> None:
    client = get_supabase_client()
    etl = MealETL(client)

    pattern = os.path.join(folder, "*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        logger.warning(
            "No CSV files found in folder '%s'",
            folder,
            extra={
                "invoking_func": "ingest_folder",
                "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
                "next_step": "Exit script",
                "resolution": "Place Kaggle CSV files under data/kaggle and rerun",
            },
        )
        return

    logger.info(
        "Found %d Kaggle CSV files under '%s'",
        len(files),
        folder,
        extra={
            "invoking_func": "ingest_folder",
            "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
            "next_step": "Loop over files and load them",
            "resolution": "",
        },
    )

    total_recipes = 0

    for fpath in files:
        dataset_name = os.path.splitext(os.path.basename(fpath))[0]
        logger.info(
            "Loading file '%s' as dataset '%s'",
            fpath,
            dataset_name,
            extra={
                "invoking_func": "ingest_folder",
                "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
                "next_step": "Call load_kaggle_csv and then MealETL.ingest_recipe",
                "resolution": "",
            },
        )

        try:
            recipes = load_kaggle_csv(fpath, dataset_name=dataset_name)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to load Kaggle CSV '%s': %s",
                fpath,
                exc,
                extra={
                    "invoking_func": "ingest_folder",
                    "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
                    "next_step": "Skip this file and continue with next",
                    "resolution": "Inspect the CSV format and fix columns / encoding",
                },
                exc_info=True,
            )
            continue

        logger.info(
            "Ingesting %d recipes from dataset '%s'",
            len(recipes),
            dataset_name,
            extra={
                "invoking_func": "ingest_folder",
                "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
                "next_step": "Loop over RecipeRecord objects and ingest them",
                "resolution": "",
            },
        )

        for idx, rec in enumerate(recipes):
            try:
                etl.ingest_recipe(rec, index=idx)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Error ingesting recipe '%s' from dataset '%s': %s",
                    rec.title,
                    dataset_name,
                    exc,
                    extra={
                        "invoking_func": "ingest_folder",
                        "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
                        "next_step": "Skip this recipe and continue",
                        "resolution": "Inspect this recipe's data / DB constraints",
                    },
                    exc_info=True,
                )

        total_recipes += len(recipes)

    logger.info(
        "Finished ingesting all Kaggle datasets. Total recipes: %d",
        total_recipes,
        extra={
            "invoking_func": "ingest_folder",
            "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
            "next_step": "Exit script",
            "resolution": "",
        },
    )


if __name__ == "__main__":
    ingest_folder("data/kaggle")
