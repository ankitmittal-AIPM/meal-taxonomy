-- migrations/003_rls_policies.sql
-- Row-Level Security policies for Meal Taxonomy.
--
-- Assumptions:
--   - ingestion scripts use the Supabase Service Role key (bypasses RLS)
--   - client apps use anon/authenticated keys and should have read-only access
--     to public catalog tables, while user_* tables are user-scoped.

-- Enable RLS on catalog tables (read-only for clients) ---------------------

alter table public.tag_types enable row level security;
create policy "tag_types_read" on public.tag_types
for select using (true);

alter table public.tags enable row level security;
create policy "tags_read" on public.tags
for select using (true);

alter table public.meals enable row level security;
create policy "meals_read" on public.meals
for select using (true);

alter table public.ingredients enable row level security;
create policy "ingredients_read" on public.ingredients
for select using (true);

alter table public.meal_ingredients enable row level security;
create policy "meal_ingredients_read" on public.meal_ingredients
for select using (true);

alter table public.meal_tags enable row level security;
create policy "meal_tags_read" on public.meal_tags
for select using (true);

alter table public.meal_variants enable row level security;
create policy "meal_variants_read" on public.meal_variants
for select using (true);

alter table public.meal_synonyms enable row level security;
create policy "meal_synonyms_read" on public.meal_synonyms
for select using (true);

alter table public.ontology_nodes enable row level security;
create policy "ontology_nodes_read" on public.ontology_nodes
for select using (true);

alter table public.ontology_relations enable row level security;
create policy "ontology_relations_read" on public.ontology_relations
for select using (true);

alter table public.entity_ontology_links enable row level security;
create policy "entity_ontology_links_read" on public.entity_ontology_links
for select using (true);

alter table public.tag_ontology_links enable row level security;
create policy "tag_ontology_links_read" on public.tag_ontology_links
for select using (true);

-- User tables (scoped) -----------------------------------------------------

alter table public.users enable row level security;

create policy "users_select_own" on public.users
for select using (auth.uid() = auth_user_id);

create policy "users_insert_self" on public.users
for insert with check (auth.uid() = auth_user_id);

create policy "users_update_own" on public.users
for update using (auth.uid() = auth_user_id) with check (auth.uid() = auth_user_id);

alter table public.user_tag_preferences enable row level security;

create policy "user_tag_preferences_select_own" on public.user_tag_preferences
for select using (
  exists (
    select 1 from public.users u
    where u.id = user_tag_preferences.user_id and u.auth_user_id = auth.uid()
  )
);

create policy "user_tag_preferences_write_own" on public.user_tag_preferences
for all using (
  exists (
    select 1 from public.users u
    where u.id = user_tag_preferences.user_id and u.auth_user_id = auth.uid()
  )
) with check (
  exists (
    select 1 from public.users u
    where u.id = user_tag_preferences.user_id and u.auth_user_id = auth.uid()
  )
);

alter table public.user_meal_interactions enable row level security;

create policy "user_meal_interactions_select_own" on public.user_meal_interactions
for select using (
  exists (
    select 1 from public.users u
    where u.id = user_meal_interactions.user_id and u.auth_user_id = auth.uid()
  )
);

create policy "user_meal_interactions_write_own" on public.user_meal_interactions
for all using (
  exists (
    select 1 from public.users u
    where u.id = user_meal_interactions.user_id and u.auth_user_id = auth.uid()
  )
) with check (
  exists (
    select 1 from public.users u
    where u.id = user_meal_interactions.user_id and u.auth_user_id = auth.uid()
  )
);

-- Grants ------------------------------------------------------------------
-- Supabase usually grants these automatically, but we keep them explicit to avoid surprises.

grant execute on function public.search_meals_v2(text, text, text, text, int) to anon, authenticated;
grant execute on function public.match_canonical_meals(vector, float, int) to anon, authenticated;
grant execute on function public.refresh_meal_search_doc(uuid) to service_role;

