"""
Recommendation layer (Meal Taxonomy)

This package contains simple, production-friendly recommenders that combine:
  - Layer-0 heuristics (tags, filters)
  - Layer-1 ML signals (embeddings / similarity)
  - Optional Layer-2 LLM explanations (OpenAI, if enabled)

The goal is to keep recommendation logic *decoupled* from ingestion/enrichment,
while still reusing the same canonical tables:
  meals, meal_tags, tags, user_tag_preferences, user_meal_interactions
"""
