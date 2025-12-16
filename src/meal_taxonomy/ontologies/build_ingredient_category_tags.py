from __future__ import annotations
"""
build_ingredient_category_tags.py

Purpose (High-Level):

    It takes ingredients you already stored in the database, looks at which FoodOn ontology category 
    each ingredient belongs to, and then adds high-level category tags (like legume, dairy, grain, etc.) to both:
    a. individual ingredients
    b. and the meals that use those ingredients

    End Result: Your meal have much smarter ingredient_category tags, which can be used for 
    search, filtering, and recommendations. Such as 
    i.      ingredient_category:legume, 
    ii.     ingredient_category:dairy, 
    iii.    ingredient_category:grain, 
    iv.     ingredient_category:vegetable, 
    v.      ingredient_category:fruit, etc.

    This tags makes richer search, filtering, and recommendation experiences possible. Such as
    - "Show me all meals with legumes"
    - "Recommend me meals without dairy"
    - "Find meals rich in grains"
    - "Suggest vegetarian meals with high vegetable content"
    - "Discover fruit-based desserts"
    - "Explore meals featuring seasonal vegetables"
    - "Find gluten-free meals with legume ingredients"
    - "Show me high-protein meals with dairy ingredients"

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

from typing import Dict, Set, List

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.taxonomy.taxonomy_seed import ensure_tag_type, ensure_tag
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("build_ingredient_category_tags")

# --- CATEGORY ROOT START ---------------------------------------------------------
# Invoke Address - Called from build_final_category_roots and Main in this file
# Define root FoodOn IRIs for ingredient categories on 14 predefined ingredient categories
def build_category_roots() -> Dict[str, str]:
    """
    Define root FoodOn IRIs for ingredient categories.
    Define which ingredient categories you want to track and their FoodOn root IRIs.
    
    Returns:
        dict: category_value -> FoodOn root IRI
    """

    # list of 14 high-value ingredient categories relevant to Indian meals, mapped to real FoodOn IRIs.
    # These cover >90% of Indian dishes semantically.
    # Supports healthy filtering, veg/non-veg detection, macro classification, allergen detection
    return {
         # Protein / Pulses
        "legume":  "http://purl.obolibrary.org/obo/FOODON_03301500",  # pulses, beans, lentils
        "nut":     "http://purl.obolibrary.org/obo/FOODON_03309936",  # almonds, cashews, pistachios

        # Dairy / Animal Products
        "dairy":   "http://purl.obolibrary.org/obo/FOODON_00002453",
        "egg":     "http://purl.obolibrary.org/obo/FOODON_00002427",

        # Meat Categories
        "meat":    "http://purl.obolibrary.org/obo/FOODON_00001230",
        "poultry": "http://purl.obolibrary.org/obo/FOODON_00001216",
        "fish":    "http://purl.obolibrary.org/obo/FOODON_00001215",
        "seafood": "http://purl.obolibrary.org/obo/FOODON_00001220",

        # Staples
        "cereal_grain": "http://purl.obolibrary.org/obo/FOODON_00001208",  # rice, wheat flour, etc.
        "spice":        "http://purl.obolibrary.org/obo/FOODON_03303101",  # cardamom, cumin, chili
        "herb":         "http://purl.obolibrary.org/obo/FOODON_03302724",
        "vegetable":    "http://purl.obolibrary.org/obo/FOODON_00001205",
        "fruit":        "http://purl.obolibrary.org/obo/FOODON_00001206",

        # Fats / Oils / Sugars
        "oil_fat":   "http://purl.obolibrary.org/obo/FOODON_03302094",  # ghee, oils
        "sweetener": "http://purl.obolibrary.org/obo/FOODON_00001059",  # sugar, jaggery
    }

# Invoke Address - Called from build_final_category_roots in this file
# Auto-discover category roots from FoodOn hierarchy
def auto_discover_category_roots(ontology_nodes, ontology_relations, min_descendants=20):
    """
    Instead of manually specifying categories, the system can:
    Automatically detect good candidate category roots by analyzing your FoodOn ontology tree.
    This is what “smart ontology systems” do

    We treat any node with many descendants as a 'category root'.

    NEW BEHAVIOR >>
    i. The system looks at your existing ontology_nodes (FoodOn).
    ii.Identifies all high-level classes (parents that have many descendants).
    iii.Uses heuristics to pick top categories (e.g., dairy, meat, legume, herb, spice).
    iv. Assigns them as category roots automatically.
    v. Still allows manual overrides.
    """
    # Build parent-child mappings
    child_to_parent = {child: parent for parent, child in ontology_relations}
    parent_to_children = {}
    # Reverse mapping of child_to_parent
    for child, parent in child_to_parent.items():
        parent_to_children.setdefault(parent, []).append(child)

    # Function to count descendants
    def count_descendants(node):
        visited = set()
        stack = [node]
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            stack.extend(parent_to_children.get(n, []))
        return len(visited)

    # Find nodes with many descendants
    category_roots = {}
    for node_id, iri in ontology_nodes.items():
        descendants = count_descendants(node_id)
        if descendants >= min_descendants:
            label = iri.split("/")[-1]  # fallback name
            category_roots[label] = iri

    return category_roots

# Invoke Address - Called from ensure_category_tags and main in this file
# Merge auto-discovered roots + your manual list
def build_final_category_roots(client): 

    """
    Combine auto-discovered category roots with manual definitions.
    Now the engine:
    a. Detects ontology category families automatically
    b. Ensures you never miss categories if FoodOn updates
    c. Still respects your manual curation
    d. Reduces maintenance effort dramatically
    """
    # Load FoodOn graph or hierarchy from ontology_relations table in Supabase DB
    relations = load_foodon_hierarchy(client)
    # Load all FoodOn ontology_nodes (id -> iri) from Supabase DB
    nodes = load_foodon_nodes(client)

    auto = auto_discover_category_roots(nodes, relations)
    manual = build_category_roots()  # your curated list

    # Manual keys overwrite auto-detected ones
    return {**auto, **manual}
# --- CATEGORY ROOT ENDS ------------------------------------------------------------

# --- MAIN LOGIC ---------------------------------------------------------------
# Invoke Address - Called from main in this file
# Ensure ingredient_category tag_type and tags exist in the DB
def ensure_category_tags(client) -> Dict[str, str]:
    """
    Ensure tag_type 'ingredient_category' exists and create tags (legume, dairy…).
    ensure_category_tags() uses the keys ("legume", "dairy", etc.) to create ingredient_category tags in the DB.
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

    roots = build_final_category_roots(client)
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

# Invoke Address - Called from build_final_category_roots in this file
# Load all FoodOn ontology_nodes (id -> iri) from Supabase DB
def load_foodon_nodes(client):
    # Load FoodOn ontology_nodes (id -> iri) from Supabase DB
    res = (
        client.table("ontology_nodes")
        .select("id, iri, label, kind")
        .eq("source", "FoodOn")
        .execute()
    )
    return res.data or []

# Invoke Address - Called from build_final_category_roots and Main in this file
# Returns FoodOn Hierarchy as parent -> children mapping from ontology_relations Table in Supabase DB
def load_foodon_hierarchy(client):
    """
    Load FoodOn "is_a" relationships stored in ontology_relations.

    **Hierarchy**: This module reads `ontology_relations` where `source='FoodOn'` 
    and `predicate='is_a'` and builds a parent → children mapping
    Returns:
        dict: parent_id -> set(child_ids)
    """
    logger.info(
        "Loading FoodOn hierarchy from ontology_relations in the Supabase DB",
        extra={
            "invoking_func": "load_foodon_hierarchy",
            "invoking_purpose": "Fetch ingredient class hierarchy from FoodOn",
            "next_step": "Query ontology_relations table",
            "resolution": "",
        },
    )
    # Load FoodOn hierarchy from ontology_relations in the Supabase DB
    rel_res = (
        client.table("ontology_relations")
        .select("subject_id, object_id")
        .eq("source", "FoodOn")
        .eq("predicate", "is_a")
        .execute()
    )

    parent_to_children: Dict[str, Set[str]] = {}
    # Build parent -> children mapping where Object is parent, Subject is child
    for rec in rel_res.data or []:
        parent_to_children.setdefault(rec["object_id"], set()).add(rec["subject_id"])

    return parent_to_children

# Invoke Address - Called from map_ingredients_to_categories
# Build all descendant nodes for a given root node in the ontology hierarchy
def build_descendants(root_id: str, parent_tree: Dict[str, Set[str]]) -> Set[str]:
    """
    DFS i.e. Depth-First Search over ontology hierarchy to find descendant nodes.
    """
    seen: Set[str] = set()
    # Defining depth-first search function that traverses the tree and collects descendants
    def dfs(nid: str):
        if nid in seen:
            return
        seen.add(nid)
        for child in parent_tree.get(nid, []):
            dfs(child)

    dfs(root_id)
    return seen

# Invoke Address - Called from main in this file
# Map ingredients to categories based on FoodOn hierarchy
def map_ingredients_to_categories(client, category_roots, hierarchy):
    """
    Map ingredient_id -> set(category_values) based on FoodOn ontology links
    - Loads all FoodOn nodes from `ontology_nodes` (`iri` → `id`) Table in Supabase DB
    - For each category root IRI (legume, dairy, etc.), finds its node_id, walks all descendants with `build_descendants`, and builds a set of all child node_ids per category
    - Reads `entity_ontology_links` for ingredients and marks an ingredient as belonging to a category if its `ontology_node_id` sits under that root

    """
    # Load all FoodOn ontology_nodes (iri -> id) from Supabase DB
    node_res = (
        client.table("ontology_nodes")
        .select("id, iri")
        .eq("source", "FoodOn")
        .execute()
    )
    # Build IRI -> id mapping
    iri_to_id = {row["iri"]: row["id"] for row in node_res.data or []}

    cat_to_descendants: Dict[str, Set[str]] = {}
    # For each category root, build its descendant node IDs list. This will be matched against ingredient links next
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

    # Load all ingredient→FoodOn links from entity_ontology_links Table in Supabase DB
    link_res = (
        client.table("entity_ontology_links")
        .select("entity_id, ontology_node_id")
        .eq("entity_type", "ingredient")
        .eq("source", "FoodOn")
        .execute()
    )
    ingredient_to_cats: Dict[str, Set[str]] = {}
    # For each ingredient link, see which category roots it falls under
    for rec in link_res.data or []:
        ing_id = rec["entity_id"]
        node_id = rec["ontology_node_id"]
        # Check each category to see if this node_id is a descendant
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

# Invoke Address - Called from main in this file
# Propagate ingredient categories to meals via meal_ingredients
def propagate_categories_to_meals(client, ingredient_to_cats, tag_ids_by_value):
    """
    For each meal, apply ingredient_category tags based on ingredient categories.
    **Meals → categories**: This module looks at `meal_ingredients` and maps meals to the combined categories of their ingredients, and writes to `meal_tags`
    """
    # Loads all meal_ingredients from Supabase DB
    mi_res = client.table("meal_ingredients").select("meal_id, ingredient_id").execute()

    meal_to_cats: Dict[str, Set[str]] = {}
    # For each meal_ingredient, look up ingredient categories and aggregate it to meal level
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
    # For each meal and its categories, prepare meal_tags upsert rows in Supabase DB
    for meal_id, cats in meal_to_cats.items():
        for cat_value in cats:
            tag_id = tag_ids_by_value.get(cat_value)
            if not tag_id:
                continue
            rows.append(
                {
                    "meal_id": meal_id,
                    "tag_id": tag_id,
                    # TO DO: You can refine confidence later based on ingredient prominence
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

# This is the main function that orchestrates the entire process
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
    roots = build_final_category_roots(client)
    hierarchy = load_foodon_hierarchy(client)
    ing_to_cats = map_ingredients_to_categories(client, roots, hierarchy)
    propagate_categories_to_meals(client, ing_to_cats, tag_ids)


if __name__ == "__main__":
    main()
