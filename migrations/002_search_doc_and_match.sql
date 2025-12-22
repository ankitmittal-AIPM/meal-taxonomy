-- migrations/002_search_doc_and_match.sql
-- Adds:
--   - search_text-aware tsvector generation
--   - refresh_meal_search_doc RPC (aggregates synonyms/tags/ingredients into meals.search_text)
--   - match_canonical_meals RPC (pgvector similarity search)
--   - search_meals_v2 updated to return only canonical meals

-- Ensure columns exist (idempotent)
alter table public.meals add column if not exists is_canonical boolean not null default true;
alter table public.meals add column if not exists canonical_meal_id uuid references public.meals(id) on delete set null;
alter table public.meals add column if not exists search_text text;

-- Rebuild the tsvector function to include search_text
create or replace function public.meals_set_search_tsv()
returns trigger
language plpgsql
as $$
begin
  new.search_tsv :=
    setweight(to_tsvector('simple', unaccent(coalesce(new.title, ''))), 'A') ||
    setweight(to_tsvector('simple', unaccent(coalesce(new.search_text, ''))), 'B') ||
    setweight(to_tsvector('simple', unaccent(coalesce(new.description, ''))), 'C') ||
    setweight(to_tsvector('simple', unaccent(coalesce(new.instructions, ''))), 'D');
  return new;
end;
$$;

-- Refresh search doc helper ------------------------------------------------
-- This is called from the ETL after tags/synonyms/ingredients are attached.
create or replace function public.refresh_meal_search_doc(target_meal_id uuid)
returns void
language plpgsql
security definer
as $$
declare
  v_synonyms text;
  v_tags text;
  v_ingredients text;
begin
  select string_agg(ms.synonym, ' ') into v_synonyms
  from public.meal_synonyms ms
  where ms.meal_id = target_meal_id;

  select string_agg(coalesce(t.label_en, t.value) || ' ' || t.value, ' ') into v_tags
  from public.meal_tags mt
  join public.tags t on t.id = mt.tag_id
  where mt.meal_id = target_meal_id;

  select string_agg(i.name_en, ' ') into v_ingredients
  from public.meal_ingredients mi
  join public.ingredients i on i.id = mi.ingredient_id
  where mi.meal_id = target_meal_id;

  update public.meals
  set search_text = concat_ws(' ', coalesce(v_synonyms, ''), coalesce(v_tags, ''), coalesce(v_ingredients, ''))
  where id = target_meal_id;
end;
$$;

-- Vector similarity search -------------------------------------------------
create or replace function public.match_canonical_meals(
  query_embedding vector(384),
  match_threshold float,
  match_count int
)
returns table (
  id uuid,
  title text,
  similarity float
)
language sql
stable
security definer
as $$
  select
    m.id,
    m.title,
    (1 - (m.embedding <=> query_embedding))::float as similarity
  from public.meals m
  where
    m.is_canonical = true
    and m.embedding is not null
    and (1 - (m.embedding <=> query_embedding)) > match_threshold
  order by m.embedding <=> query_embedding
  limit match_count;
$$;

-- Search function patch: only canonical meals ------------------------------
create or replace function public.search_meals_v2(
  query_text text,
  diet_value text default null,
  meal_type_value text default null,
  region_value text default null,
  limit_n int default 20
)
returns table (
  id uuid,
  title text,
  total_time_minutes numeric,
  score float,
  title_normalized text
)
language sql
stable
security definer
as $$
with q as (
  select websearch_to_tsquery('simple', unaccent(query_text)) as tsq
),
base as (
  select
    m.id,
    m.title,
    m.title_normalized,
    m.total_time_minutes,
    -- rank score blends trigram similarity and full-text rank
    (
      0.55 * greatest(similarity(m.title_normalized, unaccent(query_text)), 0)
      +
      0.45 * ts_rank_cd(m.search_tsv, (select tsq from q))
    )::float as score
  from public.meals m
  where
    m.is_canonical = true
    and (
      m.search_tsv @@ (select tsq from q)
      or similarity(m.title_normalized, unaccent(query_text)) > 0.15
    )
)
select
  b.id,
  b.title,
  b.total_time_minutes,
  b.score,
  b.title_normalized
from base b
where
  -- optional filters (diet / meal_type / region)
  (diet_value is null or exists (
    select 1
    from public.meal_tags mt
    join public.tags t on t.id = mt.tag_id
    join public.tag_types tt on tt.id = t.tag_type_id
    where mt.meal_id = b.id and tt.name = 'diet' and t.value = diet_value
  ))
  and (meal_type_value is null or exists (
    select 1
    from public.meal_tags mt
    join public.tags t on t.id = mt.tag_id
    join public.tag_types tt on tt.id = t.tag_type_id
    where mt.meal_id = b.id and tt.name = 'meal_type' and t.value = meal_type_value
  ))
  and (region_value is null or exists (
    select 1
    from public.meal_tags mt
    join public.tags t on t.id = mt.tag_id
    join public.tag_types tt on tt.id = t.tag_type_id
    where mt.meal_id = b.id and tt.name = 'cuisine_region' and t.value = region_value
  ))
order by b.score desc
limit limit_n;
$$;
