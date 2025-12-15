# ontologies.py
""""
Module to map ingredients to FoodOn ontology terms.
Full, automated FoodOn integration is a big topic (FoodOn has ~40k classes).
For now, you want something pragmatic:
--> “Link ingredients to ontology categories (e.g. paneer → dairy, chole → pulses).”

You already have columns for this in ingredients:
a. ontology_term_iri text -- IRI stands for Internationalized Resource Identifier. In the RDF space IRIs are used as “names”, or an equivalent of “IDs”, for graph nodes.
b. ontology_source text

An RDF ontology uses the Resource Description Framework (RDF) to model knowledge about a domain by defining its concepts (classes) and the relationships between them (properties)
So we can start with manual + extendable mapping, and (optionally later) evolve to a proper OWL/RDF-based import.

To Do: 
🔧 Later upgrade:
Once you’re comfortable, you can:
Download foodon.owl or foodon-synonyms.tsv from the FoodOn GitHub repo
Use rdflib to search by label, and build this mapping automatically instead of manually.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple

import csv
from pathlib import Path

from supabase import Client
from src.meal_taxonomy.logging_utils import get_logger


logger = get_logger("ontologies")

# This module starts with a simple hard-coded mapping.
# Later you can generate this from FoodOn OWL / TSV files.

@dataclass
class OntologyLink:

    """
    Represents a link from a local ingredient name to an ontology term.

    iri:
      - In a real setup you would use a proper FoodOn IRI like:
        http://purl.obolibrary.org/obo/FOODON_0000xxxx
      - For now we use simple IDs like 'FOODON:chickpea' as placeholders.
    """
    iri: str
    label: str
    category_value: str          # e.g. "legume"
    category_label: str          # e.g. "Legume"
    source: str = "FoodOn"


# Example subset mapping – you will grow this over time.
# IRIs below are placeholders: look up real FoodOn IRIs via Ontobee / OLS. 
FOODON_INGREDIENT_MAPPING: Dict[str, OntologyLink] = {
    # "local ingredient name" : OntologyLink("FOODON_IRI", "Human readable label")
    "chickpeas": OntologyLink(
        iri="http://purl.obolibrary.org/obo/FOODON_XXXXXXX",  # replace with real
        label="chickpea (foodon)",
        category_value="legume",
        category_label="Legume",
    ),
    "chana": OntologyLink(
        iri="http://purl.obolibrary.org/obo/FOODON_XXXXXXX",
        label="chickpea (foodon)",
        category_value="legume",
        category_label="Legume",
    ),
    "paneer": OntologyLink(
        iri="http://purl.obolibrary.org/obo/FOODON_YYYYYYY",  # dairy product
        label="paneer",
        category_value="dairy",
        category_label="Dairy",
    ),
    "toor dal": OntologyLink(
        iri="http://purl.obolibrary.org/obo/FOODON_ZZZZZZZ",  # pigeon pea
        label="pigeon pea",
        category_value="legume",
        category_label="Legume",
    ),
    # add more as needed...
}


def normalize_ingredient_name(name: str) -> str:
    return name.strip().lower()


def find_foodon_link(name: str) -> Optional[OntologyLink]:
    key = normalize_ingredient_name(name)
    return FOODON_INGREDIENT_MAPPING.get(key)


def link_all_ingredients(client: Client) -> None:
    """
    Go through all ingredients in DB and fill ontology_term_iri / ontology_source
    wherever we have a mapping in FOODON_INGREDIENT_MAPPING.

      1) Read all ingredients from DB
    2) For any name that appears in INGREDIENT_ONTOLOGY_MAP,
       set ontology_term_iri + ontology_source.
    """
    # Fetch all ingredients
    res = client.table("ingredients").select("id, name_en, ontology_term_iri").execute()
    rows = res.data or []

    linked_count = 0

    for row in rows:
        ing_id = row["id"]
        name = row.get("name_en") or ""
        current_iri = row.get("ontology_term_iri")

        mapping = find_foodon_link(name)
        if not mapping:
            continue

        # Optional: skip if already linked
        if current_iri:
            continue

        # Safe partial update: only touches the two ontology columns
        client.table("ingredients").update(
            {
                "ontology_term_iri": mapping.iri,
                "ontology_source": mapping.source,
            }
        ).eq("id", ing_id).execute()

        linked_count += 1

    if linked_count:
        logger.info(
            "Linked %d ingredients to ontology terms.",
            linked_count,
            extra={
                "invoking_func": "link_all_ingredients",
                "invoking_purpose": "Link local ingredients to ontology terms",
                "next_step": "Continue processing other ingredients",
                "resolution": "",
            },
        )
    else:
        logger.info(
            "No ingredients matched ontology mapping",
            extra={
                "invoking_func": "link_all_ingredients",
                "invoking_purpose": "Link local ingredients to ontology terms",
                "next_step": "Consider expanding FOODON_INGREDIENT_MAPPING or importing FoodOn TSV",
                "resolution": "",
            },
        )
        
# -----------------------------------------------------------------------------
# FoodOn integration: link ingredients to FoodOn terms using foodon-synonyms.tsv
# -----------------------------------------------------------------------------

def _load_foodon_synonyms(tsv_path: Path) -> List[Tuple[str, str]]:
    """
    Read FoodOn's foodon-synonyms.tsv and keep only:
      - term_id  (col 0)
      - blob of label+synonyms (cols 2..end, lowercased)

    We don't depend on exact column names; we rely on the documented
    structure: first column term id, second parents, last column label+synonyms.
    """
    rows: List[Tuple[str, str]] = []

    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for raw in reader:
            if not raw or len(raw) < 3:
                continue

            term_id = (raw[0] or "").strip()
            if not term_id:
                continue

            # Combine everything from col 2 onwards as one big text field
            text = " ".join(raw[2:]).strip().lower()
            if not text:
                continue

            rows.append((term_id, text))

    return rows


def _upsert_foodon_node(
    client: Client,
    iri: str,
    label: str,
    kind: str = "ingredient_class",
) -> str:
    """
    Ensure we have an ontology_nodes row for this FoodOn term.

    We use a simple label for now (often the ingredient name) –
    you can refine this later by pulling official labels from FoodOn.

    We can't rely on ON CONFLICT here because there is no matching unique
    constraint in Postgres, so we:
      1) Try to SELECT an existing node
      2) If not found, INSERT a new one
    """
    # 1) Try to find existing
    existing = (
        client.table("ontology_nodes")
        .select("id")
        .eq("iri", iri)
        .eq("source", "FoodOn")
        .limit(1)
        .execute()
    )
    if existing.data:
        return existing.data[0]["id"]

    # 2) Insert new
    res = client.table("ontology_nodes").insert(
        {
            "iri": iri,
            "label": label,
            "source": "FoodOn",
            "kind": kind,
        }
    ).execute()

    if not res.data:
        # Very unexpected – but don't crash the whole pipeline
        raise RuntimeError(f"Failed to upsert ontology_nodes row for {iri}")

    return res.data[0]["id"]


def _upsert_entity_link(
    client: Client,
    entity_type: str,
    entity_id: str,
    ontology_node_id: str,
    confidence: float = 0.9,
    source: str = "FoodOn",
) -> None:
    """
    Link an ingredient (or later a meal) to an ontology node.

    Uses a unique index on (entity_type, entity_id, ontology_node_id, source)
    so repeated runs are safe.
    """
    client.table("entity_ontology_links").upsert(
        {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "ontology_node_id": ontology_node_id,
            "confidence": confidence,
            "source": source,
        },
        on_conflict="entity_type,entity_id,ontology_node_id,source",
    ).execute()


def link_ingredients_via_foodon_synonyms(client: Client, tsv_path: str) -> None:
    """
    Match your ingredients.name_en against the label+synonym text in
    foodon-synonyms.tsv and:

      * Set ingredients.ontology_term_iri + ontology_source = 'FoodOn'
      * Create ontology_nodes rows for matched FoodOn terms
      * Create entity_ontology_links rows: ingredient -> FoodOn term

    Matching is simple substring-based for now:
      normalized(ingredient_name) in lowercased(label+synonyms blob).
    """
    path = Path(tsv_path)
    if not path.exists():
        logger.error(
            "FoodOn synonyms TSV not found at '%s'",
            path,
            extra={
                "invoking_func": "link_ingredients_via_foodon_synonyms",
                "invoking_purpose": "Match ingredients to FoodOn via synonyms TSV",
                "next_step": "Abort operation",
                "resolution": "Place data/foodon-synonyms.tsv or pass correct path",
            },
        )
        return

    synonyms_rows = _load_foodon_synonyms(path)
    if not synonyms_rows:
        logger.warning(
            "No usable rows found in FoodOn synonyms file",
            extra={
                "invoking_func": "link_ingredients_via_foodon_synonyms",
                "invoking_purpose": "Match ingredients to FoodOn via synonyms TSV",
                "next_step": "Abort operation",
                "resolution": "Verify TSV format",
            },
        )
        return

    # Load ingredients from DB
    res = client.table("ingredients").select("id, name_en, ontology_term_iri").execute()
    ingredients = res.data or []
    if not ingredients:
        logger.warning(
            "No ingredients found in DB to match against FoodOn synonyms",
            extra={
                "invoking_func": "link_ingredients_via_foodon_synonyms",
                "invoking_purpose": "Match ingredients to FoodOn via synonyms TSV",
                "next_step": "Populate ingredients table before retrying",
                "resolution": "",
            },
        )
        return

    # Build matches: ingredient_id -> FoodOn term_id
    matches: Dict[str, str] = {}

    for ing in ingredients:
        ing_id = ing["id"]
        name_raw = ing.get("name_en") or ""
        name_norm = name_raw.strip().lower()
        if not name_norm:
            continue

        # If already linked to something non-empty, you can choose to skip
        # to avoid overwriting manual mappings. For now, we allow override.
        # if ing.get("ontology_term_iri"):
        #     continue

        for term_id, blob in synonyms_rows:
            if name_norm and name_norm in blob:
                matches[ing_id] = term_id
                break  # take the first match

    if not matches:
        logger.info(
            "No ingredient names matched any FoodOn synonyms",
            extra={
                "invoking_func": "link_ingredients_via_foodon_synonyms",
                "invoking_purpose": "Match ingredients to FoodOn via synonyms TSV",
                "next_step": "Consider using fuzzy matching or expanding TSV",
                "resolution": "",
            },
        )
        return

    linked_count = 0

    for ing in ingredients:
        ing_id = ing["id"]
        term_id = matches.get(ing_id)
        if not term_id:
            continue

        name_raw = ing.get("name_en") or ""
        iri = term_id  # in foodon-synonyms.tsv the id is already a full IRI

        # 1) Ensure ontology_nodes entry
        node_id = _upsert_foodon_node(
            client,
            iri=iri,
            label=name_raw or iri,
            kind="ingredient_class",
        )

        # 2) Update ingredient with FoodOn IRI (partial update, no upsert)
        client.table("ingredients").update(
            {
                "ontology_term_iri": iri,
                "ontology_source": "FoodOn",
            }
        ).eq("id", ing_id).execute()

        # 3) Create / upsert entity_ontology_link
        _upsert_entity_link(
            client,
            entity_type="ingredient",
            entity_id=ing_id,
            ontology_node_id=node_id,
            confidence=0.9,
            source="FoodOn",
        )

        linked_count += 1

    logger.info(
        "Linked %d ingredients to FoodOn terms using synonyms",
        linked_count,
        extra={
            "invoking_func": "link_ingredients_via_foodon_synonyms",
            "invoking_purpose": "Match ingredients to FoodOn via synonyms TSV",
            "next_step": "Exit",
            "resolution": "",
        },
    )
