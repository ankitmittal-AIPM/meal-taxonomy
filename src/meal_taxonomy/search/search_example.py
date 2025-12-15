"""Example script demonstrating search RPC usage."""
from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("search_example")


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
        logger.info(
            "%s - %s mins",
            row.get("title"),
            row.get("total_time_minutes"),
            extra={
                "invoking_func": "run_search",
                "invoking_purpose": "Demonstrate search_meals RPC",
                "next_step": "",
                "resolution": "",
            },
        )


if __name__ == "__main__":
    run_search()

