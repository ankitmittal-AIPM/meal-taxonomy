# search_example.py. Testing Search
from config import get_supabase_client

def run_search() -> None:
    client = get_supabase_client()

    resp = client.rpc(
        "search_meals",
        {
            "diet_value": "vegan",
            "meal_type_value": "main",
            "region_value": "indian",
            "max_total_time": 40,
        },
    ).execute()

    for row in resp.data or []:
        print(f"{row['title']} - {row['total_time_minutes']} mins")


if __name__ == "__main__":
    run_search()

