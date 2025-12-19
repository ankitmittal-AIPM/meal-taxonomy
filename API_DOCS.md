
# 🔌 API Documentation (Supabase RPC + REST)

## Authentication
Use Supabase anon or service role key.

---

## GET Meals

```http
GET /rest/v1/meals
```

---

## Complex Search via RPC

```sql
rpc.search_meals(
  diet_value text,
  max_total_time int,
  meal_type_value text,
  region_value text
)
```

Params:
| Name | Meaning |
|------|----------|
| diet_value | veg/non-veg/vegan |
| max_total_time | total prep+cook minutes |
| meal_type_value | breakfast/lunch/dinner/snack |
| region_value | punjabi/south_indian etc |

---

## Insert Meal (ETL)

```http
POST /rest/v1/meals
```

Payload:
```json
{
  "title": "...",
  "instructions": "...",
  "source": "dataset",
  "external_id": "1234"
}
```

