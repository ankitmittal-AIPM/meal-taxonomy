from __future__ import annotations
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

import argparse
import datetime
import sys
from pathlib import Path
from typing import List

# --- Make project root importable so `src.*` imports work even when this
# --- script is executed from the `scripts/` directory.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
# --- Ignore above for linting/static analysis tools.

from src.meal_taxonomy.logging_utils import RUN_ID, get_logger
from src.meal_taxonomy.etl.pipeline import ingest_indian_kaggle
from src.meal_taxonomy.etl.ingest_kaggle_all import ingest_folder as ingest_kaggle_folder
from src.meal_taxonomy.ontologies.foodon_import import main as foodon_main
from src.meal_taxonomy.ontologies.build_ingredient_category_tags import main as category_main
from src.meal_taxonomy.ontologies.kaggle_ontology_import import main as kaggle_onto_main

# Structured logger for this runner
logger = get_logger("etl_run")

MODULE_PURPOSE = (
    "Unified ETL runner coordinating ingestion, ontology linking, and tagging "
    "for the Meal Taxonomy / Indian Food platform."
)


def print_run_banner(enabled_steps: List[str]) -> None:
    now = datetime.datetime.utcnow()
    banner = [
        "\n===============================================================",
        "  MEAL-TAXONOMY ETL RUN",
        f"  Run ID       : {RUN_ID}",
        f"  UTC Time     : {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "  Enabled Steps:",
    ]
    for step in enabled_steps:
        banner.append(f"    • {step}")

    banner.append("===============================================================\n")
    # Also emit a structured log with enabled steps for observability
    logger.info(
        "Starting ETL run; enabled steps: %s",
        ", ".join(enabled_steps) or "(none)",
        extra={
            "invoking_func": "print_run_banner",
            "invoking_purpose": MODULE_PURPOSE,
            "next_step": "Begin ETL run",
            "resolution": "",
        },
    )

    # Keep pretty banner on stdout for operator visibility
    print("\n".join(banner))


def run_etl(args) -> None:
    steps_run: List[str] = []

    # Kaggle
    if args.kaggle:
        steps_run.append("Kaggle ingestion")
        logger.info(
            "Starting Kaggle ingestion step",
            extra={
                "invoking_func": "run_etl",
                "invoking_purpose": "Unified ETL execution flow",
                "next_step": "Call ingest_kaggle_folder",
                "resolution": "",
            },
        )
        try:
            ingest_kaggle_folder("data/kaggle")
        except Exception as exc:
            logger.error(
                "Kaggle ingestion failed: %s",
                exc,
                extra={
                    "invoking_func": "run_etl",
                    "invoking_purpose": "Unified ETL execution flow",
                    "next_step": "Fix CSV or loader, then rerun.",
                    "resolution": "Inspect error and Kaggle CSV formats.",
                },
                exc_info=True,
            )

    # Legacy Indian
    if args.indian:
        steps_run.append("Indian Kaggle ingestion")
        logger.info(
            "Starting legacy Indian Kaggle ingestion",
            extra={
                "invoking_func": "run_etl",
                "invoking_purpose": "Unified ETL execution flow",
                "next_step": "Call ingest_indian_kaggle('data/indian_food.csv')",
                "resolution": "",
            },
        )
        try:
            ingest_indian_kaggle("data/indian_food.csv")
        except Exception as exc:
            logger.error(
                "Indian Kaggle ingestion failed: %s",
                exc,
                extra={
                    "invoking_func": "run_etl",
                    "invoking_purpose": "Unified ETL execution flow",
                    "next_step": "Skip this dataset or fix CSV.",
                    "resolution": "Check CSV format / data issues.",
                },
                exc_info=True,
            )

    # FoodOn
    if args.foodon:
        steps_run.append("FoodOn synonyms import")
        logger.info(
            "Starting FoodOn ingredient linking",
            extra={
                "invoking_func": "run_etl",
                "invoking_purpose": "Unified ETL execution flow",
                "next_step": "Call foodon_main()",
                "resolution": "",
            },
        )
        try:
            foodon_main()
        except Exception as exc:
            logger.error(
                "FoodOn linking failed: %s",
                exc,
                extra={
                    "invoking_func": "run_etl",
                    "invoking_purpose": "Unified ETL execution flow",
                    "next_step": "Inspect TSV and ontology tables.",
                    "resolution": "Fix TSV or DB schema and rerun.",
                },
                exc_info=True,
            )

    # Ingredient categories
    if args.category:
        steps_run.append("Ontology-based ingredient categories")
        logger.info(
            "Starting ingredient_category derivation",
            extra={
                "invoking_func": "run_etl",
                "invoking_purpose": "Unified ETL execution flow",
                "next_step": "Call category_main()",
                "resolution": "",
            },
        )
        try:
            category_main()
        except Exception as exc:
            logger.error(
                "Ingredient category tagging failed: %s",
                exc,
                extra={
                    "invoking_func": "run_etl",
                    "invoking_purpose": "Unified ETL execution flow",
                    "next_step": "Check ontology_relations / FoodOn config.",
                    "resolution": "Fix mappings and rerun.",
                },
                exc_info=True,
            )

    # Kaggle ontology
    if args.kaggle_onto:
        steps_run.append("Kaggle ontology import")
        logger.info(
            "Starting Kaggle ontology import",
            extra={
                "invoking_func": "run_etl",
                "invoking_purpose": "Unified ETL execution flow",
                "next_step": "Call kaggle_onto_main()",
                "resolution": "",
            },
        )
        try:
            kaggle_onto_main()
        except Exception as exc:
            logger.error(
                "Kaggle ontology import failed: %s",
                exc,
                extra={
                    "invoking_func": "run_etl",
                    "invoking_purpose": "Unified ETL execution flow",
                    "next_step": "Check meals.meta and Kaggle datasets.",
                    "resolution": "Fix metadata inconsistencies and rerun.",
                },
                exc_info=True,
            )

    logger.info(
        "ETL run completed. Steps executed: %s",
        ", ".join(steps_run),
        extra={
            "invoking_func": "run_etl",
            "invoking_purpose": "Unified ETL execution flow",
            "next_step": "Exit script or start next pipeline stage.",
            "resolution": "",
        },
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
