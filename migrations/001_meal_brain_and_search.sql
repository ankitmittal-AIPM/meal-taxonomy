-- migrations/001_meal_brain_and_search.sql
-- -------------------------------------------------------------------
-- Meal Brain + Google-like Search upgrades for Meal-Taxonomy
--
-- What this migration adds:
--   1) meal_variants: store per-source variants linked to canonical meals
--   2) meal_synonyms: store alternate names for canonical meals
--   3) meals.search_tsv + indexes: fast full-text + trigram search
--   4) Optional vector columns (pgvector) for embeddings (future-proof)
--
-- Apply this SQL in Supabase SQL editor (or via CLI migrations).
-- -------------------------------------------------------------------

-- Extensions (safe idempotent)
create extension if not exists pgcrypto;
create extension if not exists pg_trgm;
-- pgvector is available on Supabase, but if your project doesn't have it enabled yet,
-- this line may error. You can comment it out if needed.
create extension if not exists vector;

-- -------------------------------------------------------------------
-- Canonical meals improvements
-- -------------------------------------------------------------------
alter table if exists public.meals
    add column if not exists title_normalized text;

alter table if exists public.meals
    add column if not exists search_tsv tsvector;

-- Optional: canonical embedding stored on meals (for future similarity search)
-- Dimension 384 matches common small sentence-transformer models (e.g. all-MiniLM-L6-v2).
alter table if exists public.meals
    add column if not exists embedding vector(384);

-- Keep search_tsv updated automatically
create or replace function public.meals_set_search_tsv()
returns trigger
language plpgsql
as $$
begin
  new.search_tsv :=
    to_tsvector(
      'simple',
      coalesce(new.title,'') || ' ' ||
      coalesce(new.description,'') || ' ' ||
      coalesce(new.instructions,'')
    );
  return new;
end;
$$;

drop trigger if exists trg_meals_search_tsv on public.meals;
create trigger trg_meals_search_tsv
before insert or update on public.meals
for each row execute function public.meals_set_search_tsv();

-- Indexes for speed
create index if not exists idx_meals_search_tsv on public.meals using gin (search_tsv);
create index if not exists idx_meals_title_trgm on public.meals using gin (title_normalized gin_trgm_ops);

-- Optional vector index (only useful once you populate embeddings)
-- NOTE: ivfflat works best after you have a decent amount of vectors.
-- Also, ivfflat requires an operator class; vector_cosine_ops is used below.
do $$
begin
  -- Only attempt if the column exists (it will if vector extension is available)
  if exists (
    select 1
    from information_schema.columns
    where table_schema='public' and table_name='meals' and column_name='embedding'
  ) then
    begin
      execute $sql$
        create index if not exists idx_meals_embedding_ivfflat
        on public.meals using ivfflat (embedding vector_cosine_ops)
        with (lists = 100)
      $sql$;
    exception when others then
      -- If ivfflat isn't supported in this environment or vector isn't enabled, skip.
      null;
    end;
  end if;
end $$;

-- -------------------------------------------------------------------
-- Meal variants table (provenance + dedupe)
-- -------------------------------------------------------------------
create table if not exists public.meal_variants (
  id uuid primary key default gen_random_uuid(),
  meal_id uuid not null references public.meals(id) on delete cascade,

  source_type text not null,
  source_id text not null,

  title_original text,
  title_normalized text,

  ingredients_raw text,
  ingredients_norm text,

  instructions_raw text,
  instructions_norm text,

  cuisine text,
  course text,
  diet text,

  prep_time_minutes numeric,
  cook_time_minutes numeric,
  total_time_minutes numeric,
  servings numeric,

  needs_review boolean not null default false,

  -- Optional embedding for variant (pgvector)
  embedding vector(384),

  meta jsonb not null default '{}'::jsonb,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Make ingestion idempotent
create unique index if not exists uq_meal_variants_source on public.meal_variants (source_type, source_id);

-- Helpful indexes
create index if not exists idx_meal_variants_meal_id on public.meal_variants (meal_id);
create index if not exists idx_meal_variants_title_trgm on public.meal_variants using gin (title_normalized gin_trgm_ops);

-- Keep updated_at fresh
create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_meal_variants_updated_at on public.meal_variants;
create trigger trg_meal_variants_updated_at
before update on public.meal_variants
for each row execute function public.set_updated_at();

-- -------------------------------------------------------------------
-- Synonyms table (alternate spellings, translations, etc.)
-- -------------------------------------------------------------------
create table if not exists public.meal_synonyms (
  id uuid primary key default gen_random_uuid(),
  meal_id uuid not null references public.meals(id) on delete cascade,

  synonym text not null,
  synonym_normalized text not null,

  language_code text default 'en',
  source text default 'enrichment',

  created_at timestamptz not null default now()
);

create unique index if not exists uq_meal_synonyms_meal_norm on public.meal_synonyms (meal_id, synonym_normalized);
create index if not exists idx_meal_synonyms_trgm on public.meal_synonyms using gin (synonym_normalized gin_trgm_ops);

-- -------------------------------------------------------------------
-- Hybrid search RPC (FTS + trigram; tag filters compatible with existing project)
-- -------------------------------------------------------------------
create or replace function public.search_meals_v2(
  query_text text,
  diet_value text default null,
  meal_type_value text default null,
  region_value text default null,
  result_limit integer default 20
)
returns table (
  id uuid,
  title text,
  total_time_minutes integer,
  score double precision,
  title_normalized text
)
language plpgsql
stable
as $$
declare
  q text;
  q_norm text;
begin
  q := coalesce(query_text, '');
  q_norm := regexp_replace(lower(q), '\s+', ' ', 'g');

  return query
  with base as (
    select
      m.id,
      m.title,
      m.total_time_minutes,
      m.title_normalized,
      -- Full-text score (0..1-ish)
      ts_rank(m.search_tsv, plainto_tsquery('simple', q)) as fts_rank,
      -- Trigram similarity (0..1)
      similarity(coalesce(m.title_normalized,''), q_norm) as tri_sim
    from public.meals m
    where (
      m.search_tsv @@ plainto_tsquery('simple', q)
      or coalesce(m.title_normalized,'') % q_norm
    )
    and (
      diet_value is null or exists (
        select 1
        from public.meal_tags mt
        join public.tags t on t.id = mt.tag_id
        join public.tag_types tt on tt.id = t.tag_type_id
        where mt.meal_id = m.id and tt.name = 'diet' and t.value = diet_value
      )
    )
    and (
      meal_type_value is null or exists (
        select 1
        from public.meal_tags mt
        join public.tags t on t.id = mt.tag_id
        join public.tag_types tt on tt.id = t.tag_type_id
        where mt.meal_id = m.id and tt.name = 'meal_type' and t.value = meal_type_value
      )
    )
    and (
      region_value is null or exists (
        select 1
        from public.meal_tags mt
        join public.tags t on t.id = mt.tag_id
        join public.tag_types tt on tt.id = t.tag_type_id
        where mt.meal_id = m.id and tt.name = 'cuisine_region' and t.value = region_value
      )
    )
  )
  select
    id,
    title,
    total_time_minutes,
    (0.70 * fts_rank + 0.30 * tri_sim) as score,
    title_normalized
  from base
  order by score desc
  limit result_limit;

end;
$$;
