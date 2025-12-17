from __future__ import annotations
"""
foodon_import.py

A. Purpose:
    Utility script to link your ingredients to FoodOn ontology terms,
    using the FoodOn synonyms TSV file.

B. High-level behaviour:
    - Reads all ingredients from Supabase.
    - Uses ontologies.link_ingredients_via_foodon_synonyms() to:
        * match ingredient names to FoodOn terms
        * create ontology_nodes for those terms
        * create entity_ontology_links: ingredient -> FoodOn node
        * update ingredients.ontology_term_iri / ontology_source
    - Logs progress and errors using the shared structured logging format.

C. Usage:
    python foodon_import.py

D. Assumptions:
    - SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are set in your env (.env).
    - data/foodon-synonyms.tsv exists and is the FoodOn synonyms file.

E. FoodOn synonyms TSV:
    Column 1: FoodOn Term ID (e.g., FOODON:00001234).
    Column 2: Parent terms (hierarchy).
    Column 3: Preferred label (the common name) and synonyms.
    
    Purpose: Allows quick searching and mapping of real-world food names (like "Fuji apple") to standardized FoodOn IDs
"""

from pathlib import Path
from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.ontologies.ontologies import link_ingredients_via_foodon_synonyms
from src.meal_taxonomy.logging_utils import get_logger

# Module-level logger with structured formatting
logger = get_logger("foodon_import")

# Main entry point. This basically inputs full FoodOn synonyms TSV path and calls the linking Ingredient to FoodOnTSV function
def main() -> None:
    """
    Entry point for FoodOn import script.
    Steps:
        1) Resolve path to foodon-synonyms.tsv.
        2) Validate that the file exists.
        3) Create Supabase client.
        4) Delegate work to ontologies.link_ingredients_via_foodon_synonyms() to link ingredients to FoodOn.
    """
    # Foodon synonyms TSV path have FoodOn TSV file that have detailed synonyms for ingredients
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
