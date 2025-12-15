"""
etl_run.py

Purpose:
    Unified ETL runner for the Meal Taxonomy project.

    This script orchestrates:
      • Kaggle ingestion (multiple datasets)
      • Legacy Indian Kaggle ingestion
      • FoodOn ontology linking (synonyms)
      • Ontology-derived ingredient_category tagging
      • Kaggle ontology import (cuisine/course/diet → ontology_nodes)
      • Any additional ETL modules future (FKG, RecipeDB, embedding generation…)

Design:
    - Uses a shared LOG_RUN_ID (from logging_utils).
    - Emits a "Run Banner" at the start.
    - Emits structured logs for milestone boundaries.
    - Each ETL step can be toggled on/off.
    - Safe to rerun — every component is idempotent.

Usage:
    python scripts/etl_run.py

    Or run only specific stages:
    python scripts/etl_run.py --kaggle --foodon
"""

from __future__ import annotations

import argparse
import datetime
from typing import List

from meal_taxonomy.logging_utils import LOG_RUN_ID, log_info, log_error
from meal_taxonomy.etl.pipeline import ingest_indian_kaggle
from meal_taxonomy.etl.ingest_kaggle_all import ingest_folder as ingest_kaggle_folder
from meal_taxonomy.ontologies.foodon_import import main as foodon_main
from meal_taxonomy.ontologies.build_ingredient_category_tags import main as category_main
from meal_taxonomy.ontologies.kaggle_ontology_import import main as kaggle_onto_main

MODULE_PURPOSE = (
    "Unified ETL runner coordinating ingestion, ontology linking, and tagging "
    "for the Meal Taxonomy / Indian Food platform."
)


def print_run_banner(enabled_steps: List[str]) -> None:
    now = datetime.datetime.utcnow()
    banner = [
        "\n===============================================================",
        "  MEAL-TAXONOMY ETL RUN",
        f"  Run ID       : {LOG_RUN_ID}",
        f"  UTC Time     : {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "  Enabled Steps:",
    ]
    for step in enabled_steps:
        banner.append(f"    • {step}")
    banner.append("===============================================================\n")
    print("\n".join(banner))


def run_etl(args) -> None:
    steps_run: List[str] = []

    # Kaggle
    if args.kaggle:
        steps_run.append("Kaggle ingestion")
        log_info(
            "Starting Kaggle ingestion step",
            module_purpose=MODULE_PURPOSE,
            invoking_function="run_etl",
            invoking_purpose="Unified ETL execution flow",
            next_step="Call ingest_kaggle_folder",
        )
        try:
            ingest_kaggle_folder("data/kaggle")
        except Exception as exc:
            log_error(
                "Kaggle ingestion failed",
                module_purpose=MODULE_PURPOSE,
                invoking_function="run_etl",
                invoking_purpose="Unified ETL execution flow",
                next_step="Fix CSV or loader, then rerun.",
                resolution="Inspect error and Kaggle CSV formats.",
                exc=exc,
            )

    # Legacy Indian
    if args.indian:
        steps_run.append("Indian Kaggle ingestion")
        log_info(
            "Starting legacy Indian Kaggle ingestion",
            module_purpose=MODULE_PURPOSE,
            invoking_function="run_etl",
            invoking_purpose="Unified ETL execution flow",
            next_step="Call ingest_indian_kaggle('data/indian_food.csv')",
        )
        try:
            ingest_indian_kaggle("data/indian_food.csv")
        except Exception as exc:
            log_error(
                "Indian Kaggle ingestion failed",
                module_purpose=MODULE_PURPOSE,
                invoking_function="run_etl",
                invoking_purpose="Unified ETL execution flow",
                next_step="Skip this dataset or fix CSV.",
                resolution="Check CSV format / data issues.",
                exc=exc,
            )

    # FoodOn
    if args.foodon:
        steps_run.append("FoodOn synonyms import")
        log_info(
            "Starting FoodOn ingredient linking",
            module_purpose=MODULE_PURPOSE,
            invoking_function="run_etl",
            invoking_purpose="Unified ETL execution flow",
            next_step="Call foodon_main()",
        )
        try:
            foodon_main()
        except Exception as exc:
            log_error(
                "FoodOn linking failed",
                module_purpose=MODULE_PURPOSE,
                invoking_function="run_etl",
                invoking_purpose="Unified ETL execution flow",
                next_step="Inspect TSV and ontology tables.",
                resolution="Fix TSV or DB schema and rerun.",
                exc=exc,
            )

    # Ingredient categories
    if args.category:
        steps_run.append("Ontology-based ingredient categories")
        log_info(
            "Starting ingredient_category derivation",
            module_purpose=MODULE_PURPOSE,
            invoking_function="run_etl",
            invoking_purpose="Unified ETL execution flow",
            next_step="Call category_main()",
        )
        try:
            category_main()
        except Exception as exc:
            log_error(
                "Ingredient category tagging failed",
                module_purpose=MODULE_PURPOSE,
                invoking_function="run_etl",
                invoking_purpose="Unified ETL execution flow",
                next_step="Check ontology_relations / FoodOn config.",
                resolution="Fix mappings and rerun.",
                exc=exc,
            )

    # Kaggle ontology
    if args.kaggle_onto:
        steps_run.append("Kaggle ontology import")
        log_info(
            "Starting Kaggle ontology import",
            module_purpose=MODULE_PURPOSE,
            invoking_function="run_etl",
            invoking_purpose="Unified ETL execution flow",
            next_step="Call kaggle_onto_main()",
        )
        try:
            kaggle_onto_main()
        except Exception as exc:
            log_error(
                "Kaggle ontology import failed",
                module_purpose=MODULE_PURPOSE,
                invoking_function="run_etl",
                invoking_purpose="Unified ETL execution flow",
                next_step="Check meals.meta and Kaggle datasets.",
                resolution="Fix metadata inconsistencies and rerun.",
                exc=exc,
            )

    log_info(
        f"ETL run completed. Steps executed: {', '.join(steps_run)}",
        module_purpose=MODULE_PURPOSE,
        invoking_function="run_etl",
        invoking_purpose="Unified ETL execution flow",
        next_step="Exit script or start next pipeline stage.",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Unified Meal Taxonomy ETL Runner")
    parser.add_argument("--kaggle", action="store_true", help="Run Kaggle ingestion")
    parser.add_argument("--indian", action="store_true", help="Run legacy Indian ingestion")
    parser.add_argument("--foodon", action="store_true", help="Run FoodOn synonyms linking")
    parser.add_argument("--category", action="store_true", help="Run category tagging")
    parser.add_argument("--kaggle-onto", action="store_true", help="Run Kaggle ontology import")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    enabled_steps = [
        s for s, enabled in [
            ("Kaggle ingestion", args.kaggle),
            ("Indian Kaggle ingestion", args.indian),
            ("FoodOn linking", args.foodon),
            ("Ingredient category tagging", args.category),
            ("Kaggle ontology import", args.kaggle_onto),
        ] if enabled
    ]

    print_run_banner(enabled_steps)
    run_etl(args)
