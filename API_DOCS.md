# 🔌 API Documentation (Supabase)

This project uses Supabase tables + Postgres RPC functions for search and similarity.

---

## Authentication

- Backend scripts (ETL, backfills): **Service role** key (bypasses RLS)
- Client apps: **anon** / **authenticated** keys (constrained by RLS)

---

## Tables (REST)

Supabase automatically exposes REST endpoints for tables, e.g.

```http
GET /rest/v1/meals?select=*
GET /rest/v1/tags?select=*
GET /rest/v1/meal_tags?select=*
```

---

## RPC: Search Meals

### `search_meals_v2`

Hybrid full‑text + trigram search over **canonical meals**.

```sql
search_meals_v2(
  query_text text,
  diet_value text default null,
  meal_type_value text default null,
  region_value text default null,
  limit_n int default 20
)
```

Returns:

- `id` (uuid)
- `title` (text)
- `total_time_minutes` (numeric)
- `score` (float)
- `title_normalized` (text)

Example call (Supabase client SDK):

```python
client.rpc("search_meals_v2", {"query_text": "paneer curry", "limit_n": 10}).execute()
```

---

## RPC: Vector Similarity (Dedupe / "More like this")

### `match_canonical_meals`

```sql
match_canonical_meals(
  query_embedding vector(384),
  match_threshold float,
  match_count int
)
```

Returns:

- `id` (uuid)
- `title` (text)
- `similarity` (float, cosine similarity)

---

## RPC: Refresh Search Document

### `refresh_meal_search_doc`

Builds `meals.search_text` by aggregating:
- meal synonyms (`meal_synonyms`)
- attached tags (`meal_tags` + `tags`)
- ingredients (`meal_ingredients` + `ingredients`)

```sql
refresh_meal_search_doc(target_meal_id uuid) returns void
```

This is typically called after attaching tags/ingredients in ETL:

```python
client.rpc("refresh_meal_search_doc", {"target_meal_id": meal_id}).execute()
```

---

## Notes on RLS

See `migrations/003_rls_policies.sql`:
- Catalog tables are readable by anon/authenticated.
- `user_*` tables are user‑scoped via `auth.uid()`.
