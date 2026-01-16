from __future__ import annotations
"""
ingest_kaggle_all.py

A. Purpose:
    Batch-ingest all Kaggle CSV files under a folder (default: data/kaggle)
    using the unified Kaggle loader and the MealETL pipeline.

B. Usage:
    from meal_taxonomy.etl.ingest_kaggle_all import ingest_folder
    ingest_folder("data/kaggle")

"""
import sys
from pathlib import Path

import glob
import os
from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.etl.pipeline import MealETL
from src.meal_taxonomy.datasets.kaggle_unified import load_kaggle_csv
from src.meal_taxonomy.logging_utils import get_logger

MODULE_PURPOSE = (
    "Batch ingestion of multiple Kaggle CSV files into Supabase using MealETL."
)

logger = get_logger("ingest_kaggle_all")

# ---------------------------------------------------------
# Batch ingestion - Ingest all data files/CSV files with meals in folder --> data/kaggle
# Invoked Address - called directly from main function and runs when python ingest_kaggle_all.py file in CLI
# ---------------------------------------------------------
def ingest_folder(folder: str = "data/kaggle") -> None:
    client = get_supabase_client()
    # Intializes the etl object with class as MealETL doesn't call any function now just makes it object of class MealETL in pipeline
    etl = MealETL(client)

    pattern = os.path.join(folder, "*.csv")
    files = sorted(glob.glob(pattern))

    # No kaggle files in data/kaggle folder
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

    # some kaggle files found in the folder
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

    # intiliaze recipes
    total_recipes = 0

    # parsing through files
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

        # Calls kaggle_unified code where the records in csv files is stores in dataset to be upserted in Supabase
        # load_kaggle_csv does two things - normalizes the csv columns and prepare dataset for DB
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

        # Ready to ingest data in Meal DB
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

        # Milestone logging: Log only 5 error rows per dataset to avoid log flooding
        max_consecutive_failures = 5
        consecutive_failures = 0
        # TO DO : For testing the insertion
        record_count = 0
        
        # Invokes pipeline.py function ingest_recipe to upsert data in Meal DBs in Supabase
        for idx, rec in enumerate(recipes):
            try:
                # TO DO: If this calls ingest_recipe to upsert data at record level. 
                # TO DO: Record level is too slow look for method to insert at batch level
                logger.info(
                    "Ingesting recipe %d/%d from dataset '%s': '%s'",
                    idx + 1,
                    len(recipes),
                    dataset_name,
                    rec.title,
                )
                 # Calls the ingest recipe function in pipeline.py to upsert data in Meal DBs in Supabase
                etl.ingest_recipe(rec)
                consecutive_failures = 0
                # if record_count >= 5:
                #     break            
            # Long code to silence consecutive errors logs in CLI
            except Exception as exc:  # noqa: BLE001
                consecutive_failures += 1
                extra = {
                    "invoking_func": "ingest_folder",
                    "invoking_purpose": "Batch ingest all Kaggle CSV files in a folder",
                    "next_step": "Skip this recipe and continue, unless there are many consecutive failures",
                    "resolution": "Inspect this recipe's data / DB constraints",
                }

                if consecutive_failures == 1:
                    # First failure for this dataset: keep traceback
                    logger.error(
                        "Error ingesting recipe '%s' from dataset '%s': %s",
                        rec.title,
                        dataset_name,
                        exc,
                        extra=extra,
                        exc_info=True,
                    )
                else:
                    logger.error(
                        "Error ingesting recipe '%s' from dataset '%s' "
                        "[consecutive failure %d]: %s",
                        rec.title,
                        dataset_name,
                        consecutive_failures,
                        exc,
                        extra=extra,
                    )
                # Alert on too many consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(
                        "Aborting ingestion for dataset '%s' after %d consecutive "
                        "failures (likely systemic issue such as Supabase outage).",
                        dataset_name,
                        max_consecutive_failures,
                        extra=extra,
                    )
                    break

        total_recipes += len(recipes)

    # Successfully ingested all records from kaggle files in the data/kaggle folder to Supabase Meal DBs
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
