# Meal Taxonomy

Meal‑Taxonomy is a Supabase + Python backend that builds a searchable, canonical meal catalog from noisy sources (datasets, user submissions, scraped recipes) and enriches it with tags, synonyms, and embeddings.

It contains three main subsystems:

1. **Enrichment** (`src/meal_taxonomy/enrichment/`)
   - Layer‑0: rules / NLP heuristics (tags, normalization, region inference)
   - Layer‑1: optional ML models (classification, derived signals)
   - Layer‑2: optional LLM enrichment (alt names, cuisine guesses, structured extras)

2. **Meal Brain** (`src/meal_taxonomy/brain/`)
   - Dedupes and upserts a **canonical meal** + a **source variant**
   - Writes synonyms + “tag type + tag value” inventory
   - Leaves join-table attachment (`meal_tags`, `meal_ingredients`) to ETL

3. **ETL** (`src/meal_taxonomy/etl/`)
   - Loads datasets, calls enrichment + brain upsert, then attaches join rows
   - Refreshes `meals.search_text` for better search relevance

---

## Quick start

### 1) Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure env

Create a `.env` file:

```bash
SUPABASE_URL="https://<project>.supabase.co"
SUPABASE_SERVICE_ROLE_KEY="<service role key>"
# Optional (LLM enrichment)
OPENAI_API_KEY="<optional>"
```

### 3) Apply migrations

In Supabase SQL editor (or `supabase db push` if you use the CLI), apply:

- `migrations/000_base_schema.sql`
- `migrations/001_meal_brain_and_search.sql`
- `migrations/002_search_doc_and_match.sql`
- `migrations/003_rls_policies.sql`

> `migrations/` is the source of truth for the schema.

### 4) Seed taxonomy tag types

```bash
python scripts/seed_taxonomy.py
```

### 5) Ingest a dataset

Indian Kaggle CSV example:

```bash
python scripts/etl_run.py --indian-csv data/indian_food.csv --limit 200
```

---

## Search

The recommended query path is the RPC:

- `public.search_meals_v2(query_text, diet_value, meal_type_value, region_value, limit_n)`

Example:

```bash
python scripts/search_example.py --q "paneer curry" --limit 10
```

---

## Recommendations

A baseline recommender is available in:

- `src/meal_taxonomy/recommendation/recommender.py`

Example:

```bash
python -m src.meal_taxonomy.recommendation.recommendation_example --user-id <uuid> --limit 10
```

---

## Notes

- Ingestion/backfills should use **service role** (bypasses RLS).
- Client apps should use **anon/authenticated** keys and rely on RLS policies + RPCs.
