
# 🏗️ Architecture – Meal-Taxonomy

## 1. Layers

| Layer | Responsibility |
|--------|----------------|
| Data Ingestion | CSV, Kaggle, RecipeDB |
| Ontology Layer | FoodOn mappings |
| NLP Layer | Entity extraction |
| Tagging Engine | Category + Meal tagging |
| Database Layer | Supabase |
| Search Layer | RPC-based search |
| Recommendation Layer | Tag + embedding recommender (baseline), optional LLM explanations |

---

## 2. Component Diagram

```mermaid
flowchart LR
    A[Dataset CSV] --> B[ETL Loader]
    B --> C[Ingredient Normalizer]
    C --> D[Ontology Mapper]
    D --> E[Category Tag Engine]
    E --> F[Meal Tag Engine]
    F --> G[Supabase DB]
    G --> H[Search / Rec Engine]
    H --> I[User App / Web]
```

---

## 3. Ontology Graph Structure

Tables used:
- ontology_nodes  
- ontology_relations  
- entity_ontology_links  

Relationships:
```
ingredient → foodon_node → category_root → ingredient_category
```

---

## 4. Scalability
- ETL chunking  
- Batch upserts  
- Use pgvector for embeddings (future)  
- Precompute category trees  

---
