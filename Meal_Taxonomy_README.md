# Meal Taxonomy – The Intelligence Layer for Indian Food

## Vision

Meal‑Taxonomy is a backend that converts messy recipe data into a clean, canonical, searchable meal catalog enriched with:
- **Tags** (diet, meal type, cuisine/region, cooking method, equipment, occasion, health)
- **Synonyms** and alternate names (e.g., “Chole”, “Chana Masala”)
- **Embeddings** for semantic search and dedupe
- **Ontology links** (FoodOn) for ingredient/category intelligence
- **User preferences + interactions** to power personalization and recommendations

The initial focus is **Indian food** (regional coverage + vernacular naming), but the schema and pipeline are generic.

---

## What this repo ships

### 1) Canonical meal catalog
A single canonical row per meal in `meals`, plus source-specific `meal_variants` to retain provenance and normalize across datasets.

### 2) Search that understands food
Hybrid search (`search_meals_v2`) that combines:
- Full‑text ranking (`tsvector`)
- Fuzzy string similarity (`pg_trgm`)
- Optional embedding similarity (`match_canonical_meals`)

### 3) A practical enrichment pipeline
- Layer‑0: normalization + rule/NLP tags (fast, deterministic)
- Layer‑1: optional scikit‑learn models (course/diet/time/etc)
- Layer‑2: optional LLM enrichment (OpenAI) for alt names / structured metadata

### 4) Recommendation baseline
A backend-friendly recommender that uses:
- User tag preferences
- Positive interactions (likes/saves/ratings)
- Embedding similarity (if available)

Located at `src/meal_taxonomy/recommendation/`.

---

## Data model (high level)

- `meals` – canonical meals
- `meal_variants` – per-source variants
- `meal_synonyms` – alternate names
- `tags`, `tag_types`, `meal_tags` – tagging system
- `ingredients`, `meal_ingredients` – ingredient joins
- `ontology_*` – FoodOn graph storage + links
- `user_*` – personalization layer

See `Supabase Meal Taxonomy Db Schema.txt` for a human overview and `migrations/` for SQL.

---

## Running the pipeline (local)

1. Set env vars (see `README.md`)
2. Apply migrations in Supabase SQL editor
3. Seed tag types:
   ```bash
   python scripts/seed_taxonomy.py
   ```
4. Ingest a dataset:
   ```bash
   python scripts/etl_run.py --indian-csv data/indian_food.csv --limit 200
   ```

---

## Next steps (suggested)

- Add more dataset adapters (YouTube cooking channels, community submissions)
- Improve ingredient parsing (quantity/unit extraction)
- Use ontology links to normalize ingredients → categories automatically
- Add offline evaluation for dedupe + tag quality
