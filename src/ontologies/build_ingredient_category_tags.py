"""
build_ingredient_category_tags.py

Purpose (High-Level):
    • Automatically assign ingredient_category tags (legume, dairy, etc.) to meals
      based on ontology mappings (manual + FoodOn-derived).
    • Uses ontology_nodes + entity_ontology_links + meal_ingredients to infer
      category membership.
    • Writes into tags and meal_tags for search & recommendations.

When to run:
    After:
      - Ingredients table is populated
      - Ontology (FoodOn or manual) has been linked to ingredients

Logging:
    Structured logs per company logging standard:
    <RunId>|<Date>|<Time>|<Level>|<File:Line>|<Module.Func>|<ModulePurpose>|
    <InvokingFunc>|<InvokingFuncPurpose>|<Detail>|<NextStep>|<Resolution>|<END>
"""

from __future__ import annotations
from typing import Dict, Set, List

from src.meal_taxonomy.config import get_supabase_client
from taxonomy_seed import ensure_tag_type, ensure_tag
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("build_ingredient_category_tags")

# --- CATEGORY ROOT DEFINITIONS ------------------------------------------------
def build_category_roots() -> Dict[str, str]:
    """
    Define root FoodOn IRIs for ingredient categories.
    YOU MUST UPDATE THESE with real FoodOn category IRIs.
    Example:
        legume_root = 'http://purl.obolibrary.org/obo/FOODON_03301500'
        dairy_root  = 'http://purl.obolibrary.org/obo/FOODON_00002453'
    """
    return {
        "legume": "FOODON_LEGUME_ROOT_IRI",  # TODO: replace with real FoodOn term
        "dairy":  "FOODON_DAIRY_ROOT_IRI",   # TODO
    }


# --- MAIN LOGIC ---------------------------------------------------------------
def ensure_category_tags(client) -> Dict[str, str]:
    """
    Ensure tag_type 'ingredient_category' exists and create tags (legume, dairy…).

    Returns:
        dict: category_value -> tag_id
    """
    logger.info(
        "Ensuring ingredient_category tag_type and category tags",
        extra={
            "invoking_func": "ensure_category_tags",
            "invoking_purpose": "Initialize or retrieve ingredient_category tag types",
            "next_step": "Upsert tag_types and category tags",
            "resolution": "",
        },
    )

    tag_type_id = ensure_tag_type(
        client,
        name="ingredient_category",
        description="Ingredient categories from ontology hierarchy",
    )

    roots = build_category_roots()
    mapping: Dict[str, str] = {}

    for value, _iri in roots.items():
        tag_id = ensure_tag(
            client,
            tag_type_id=tag_type_id,
            value=value,
            label_en=value.replace("_", " ").title(),
        )
        mapping[value] = tag_id

    logger.info(
        "Category tags ensured: %s",
        list(mapping.keys()),
        extra={
            "invoking_func": "ensure_category_tags",
            "invoking_purpose": "Initialize ingredient_category tag types",
            "next_step": "Return category tag mapping",
            "resolution": "",
        },
    )
    return mapping


def load_foodon_hierarchy(client):
    """
    Load FoodOn is_a relationships stored in ontology_relations.

    Returns:
        dict: parent_id -> set(child_ids)
    """
    logger.info(
        "Loading FoodOn hierarchy from ontology_relations",
        extra={
            "invoking_func": "load_foodon_hierarchy",
            "invoking_purpose": "Fetch ingredient class hierarchy from FoodOn",
            "next_step": "Query ontology_relations table",
            "resolution": "",
        },
    )

    rel_res = (
        client.table("ontology_relations")
        .select("subject_id, object_id")
        .eq("source", "FoodOn")
        .eq("predicate", "is_a")
        .execute()
    )

    parent_to_children: Dict[str, Set[str]] = {}
    for rec in rel_res.data or []:
        parent_to_children.setdefault(rec["object_id"], set()).add(rec["subject_id"])

    return parent_to_children


def build_descendants(root_id: str, parent_tree: Dict[str, Set[str]]) -> Set[str]:
    """
    DFS over ontology hierarchy to find descendant nodes.
    """
    seen: Set[str] = set()

    def dfs(nid: str):
        if nid in seen:
            return
        seen.add(nid)
        for child in parent_tree.get(nid, []):
            dfs(child)

    dfs(root_id)
    return seen


def map_ingredients_to_categories(client, category_roots, hierarchy):
    """
    Map ingredient_id -> set(category_values)
    based on FoodOn ontology links.
    """
    # Load FoodOn ontology_nodes (iri -> id)
    node_res = (
        client.table("ontology_nodes")
        .select("id, iri")
        .eq("source", "FoodOn")
        .execute()
    )
    iri_to_id = {row["iri"]: row["id"] for row in node_res.data or []}

    cat_to_descendants: Dict[str, Set[str]] = {}
    for cat, root_iri in category_roots.items():
        root_id = iri_to_id.get(root_iri)
        if not root_id:
            logger.warning(
                "Category '%s' root IRI '%s' missing from ontology_nodes",
                cat, root_iri,
                extra={
                    "invoking_func": "map_ingredients_to_categories",
                    "invoking_purpose": "Derive category trees from FoodOn roots",
                    "next_step": "Skip this category",
                    "resolution": (
                        "Import FoodOn hierarchy first using foodon_hierarchy_import.py"
                    ),
                },
            )
            continue
        descendants = build_descendants(root_id, hierarchy)
        cat_to_descendants[cat] = descendants

    # Load ingredient→FoodOn links
    link_res = (
        client.table("entity_ontology_links")
        .select("entity_id, ontology_node_id")
        .eq("entity_type", "ingredient")
        .eq("source", "FoodOn")
        .execute()
    )
    ingredient_to_cats: Dict[str, Set[str]] = {}

    for rec in link_res.data or []:
        ing_id = rec["entity_id"]
        node_id = rec["ontology_node_id"]

        for cat_value, node_ids in cat_to_descendants.items():
            if node_id in node_ids:
                ingredient_to_cats.setdefault(ing_id, set()).add(cat_value)

    logger.info(
        "Ingredient→Category mapping complete. %d ingredients mapped.",
        len(ingredient_to_cats),
        extra={
            "invoking_func": "map_ingredients_to_categories",
            "invoking_purpose": "Compute category membership of ingredients",
            "next_step": "Propagate categories to meals",
            "resolution": "",
        },
    )

    return ingredient_to_cats


def propagate_categories_to_meals(client, ingredient_to_cats, tag_ids_by_value):
    """
    For each meal, apply ingredient_category tags based on ingredient categories.
    """
    mi_res = client.table("meal_ingredients").select("meal_id, ingredient_id").execute()

    meal_to_cats: Dict[str, Set[str]] = {}
    for rec in mi_res.data or []:
        meal_id = rec["meal_id"]
        ing_id = rec["ingredient_id"]

        cats = ingredient_to_cats.get(ing_id)
        if not cats:
            continue

        meal_to_cats.setdefault(meal_id, set()).update(cats)

    if not meal_to_cats:
        logger.warning(
            "No meals received ingredient_category tags",
            extra={
                "invoking_func": "propagate_categories_to_meals",
                "invoking_purpose": "Assign category tags to meals",
                "next_step": "Exit script",
                "resolution": "",
            },
        )
        return

    rows = []
    for meal_id, cats in meal_to_cats.items():
        for cat_value in cats:
            tag_id = tag_ids_by_value.get(cat_value)
            if not tag_id:
                continue
            rows.append(
                {
                    "meal_id": meal_id,
                    "tag_id": tag_id,
                    "confidence": 0.9,
                    "is_primary": False,
                    "source": "ontology",
                }
            )

    client.table("meal_tags").upsert(rows).execute()

    logger.info(
        "Assigned %d ingredient_category tags across meals",
        len(rows),
        extra={
            "invoking_func": "propagate_categories_to_meals",
            "invoking_purpose": "Assign category tags to meals",
            "next_step": "Exit script",
            "resolution": "",
        },
    )


def main():
    """
    Orchestrates category-tag derivation:
        1) Ensure category tags exist.
        2) Load FoodOn hierarchy.
        3) Map ingredients → categories.
        4) Map meals → categories.
    """
    client = get_supabase_client()

    logger.info(
        "Starting ingredient_category derivation from FoodOn hierarchy",
        extra={
            "invoking_func": "main",
            "invoking_purpose": "Top-level script for ontology-based category tagging",
            "next_step": "Ensure ingredient_category tags",
            "resolution": "",
        },
    )

    tag_ids = ensure_category_tags(client)
    roots = build_category_roots()
    hierarchy = load_foodon_hierarchy(client)
    ing_to_cats = map_ingredients_to_categories(client, roots, hierarchy)
    propagate_categories_to_meals(client, ing_to_cats, tag_ids)


if __name__ == "__main__":
    main()
