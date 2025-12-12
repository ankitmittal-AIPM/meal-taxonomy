This will:
Invokes - kaggle_unified.py to load multiple Kaggle recipe datasets in a uniform way.
    1. Load each CSV with load_kaggle_csv()
    2. Normalize columns and semantics
Invokes - pipeline.MealETL.ingest_recipe() to create meals, ingredients, tags, ontology in Supabase.
    3. Send them into pipeline.MealETL.ingest_recipe()
    4. Create meals, ingredients, tags, ontology, reusing all your existing logic
    5. Mark recipes in Supabase with:
        source = 'dataset' (pipeline)
        external_source = 'Kaggle:<dataset_name>'
        external_id = '<dataset_name>_<row_index>'

How this avoids duplicate tag types or attributes
1. All semantic mapping is done in the loader: we map diet_type, veg_or_nonveg, etc. → diet.
2. pipeline.dataset_tags() uses hard-coded tag_type names: cuisine_region, cuisine_national, diet, taste_profile, meal_type, etc.
3. Tag types are created through ensure_tag_type() which:
    Does an upsert on the same name

So you get one diet tag_type, one meal_type, etc.
So there is no chance of tag_type duplication, as long as:
Meta keys stay consistent (we’ve enforced that in kaggle_unified.py).