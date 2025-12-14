"""
foodon_import.py

Purpose:
    Utility script to link your ingredients to FoodOn ontology terms,
    using the FoodOn synonyms TSV file.

High-level behaviour:
    - Reads all ingredients from Supabase.
    - Uses ontologies.link_ingredients_via_foodon_synonyms() to:
        * match ingredient names to FoodOn terms
        * create ontology_nodes for those terms
        * create entity_ontology_links: ingredient -> FoodOn node
        * update ingredients.ontology_term_iri / ontology_source
    - Logs progress and errors using the shared structured logging format.

Usage:
    python foodon_import.py

    Make sure:
        - SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are set in your env (.env).
        - data/foodon-synonyms.tsv exists and is the FoodOn synonyms file.
"""

from __future__ import annotations

from pathlib import Path

from config import get_supabase_client
from ontologies import link_ingredients_via_foodon_synonyms
from logging_utils import get_logger


# Module-level logger with structured formatting
logger = get_logger("foodon_import")


def main() -> None:
    """
    Entry point for FoodOn import script.

    Steps:
        1) Resolve path to foodon-synonyms.tsv.
        2) Validate that the file exists.
        3) Create Supabase client.
        4) Delegate work to ontologies.link_ingredients_via_foodon_synonyms().
    """
    tsv_path = Path("data/foodon-synonyms.tsv")

    if not tsv_path.exists():
        logger.error(
            "FoodOn synonyms TSV not found at '%s'",
            tsv_path,
            extra={
                "invoking_func": "main",
                "invoking_purpose": "Link ingredients to FoodOn via synonyms file",
                "next_step": "Abort script",
                "resolution": (
                    "Download foodon-synonyms.tsv from FoodOn GitHub and "
                    "place it under data/foodon-synonyms.tsv"
                ),
            },
        )
        return

    logger.info(
        "Starting FoodOn ingredient linking using TSV at '%s'",
        tsv_path,
        extra={
            "invoking_func": "main",
            "invoking_purpose": "Link ingredients to FoodOn via synonyms file",
            "next_step": "Create Supabase client and call link_ingredients_via_foodon_synonyms()",
            "resolution": "",
        },
    )

    client = get_supabase_client()

    try:
        link_ingredients_via_foodon_synonyms(client, str(tsv_path))
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Unexpected error while linking ingredients to FoodOn: %s",
            exc,
            extra={
                "invoking_func": "main",
                "invoking_purpose": "Link ingredients to FoodOn via synonyms file",
                "next_step": "Abort script",
                "resolution": (
                    "Inspect stack trace, check TSV format and DB constraints; "
                    "fix issues and rerun"
                ),
            },
            exc_info=True,
        )
        return

    logger.info(
        "FoodOn ingredient linking completed successfully",
        extra={
            "invoking_func": "main",
            "invoking_purpose": "Link ingredients to FoodOn via synonyms file",
            "next_step": "Exit script",
            "resolution": "",
        },
    )


if __name__ == "__main__":
    main()
