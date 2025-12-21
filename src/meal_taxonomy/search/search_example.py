"""Example script demonstrating search RPC usage.

This project supports a "Google-like" hybrid search on meals via Supabase RPC.

Preferred (new) RPC:
    search_meals_v2(query_text, diet_value, meal_type_value, region_value, limit)

Legacy (older) RPC (if you already created it):
    search_meals(diet_value, meal_type_value, region_value, limit)

This script tries search_meals_v2 first and falls back to search_meals.
"""

from src.meal_taxonomy.config import get_supabase_client
from src.meal_taxonomy.logging_utils import get_logger

logger = get_logger("search_example")


def run_search() -> None:
    client = get_supabase_client()

    query_text = "paneer butter masala"

    # Try the new RPC first
    try:
        resp = client.rpc(
            "search_meals_v2",
            {
                "query_text": query_text,
                "diet_value": None,
                "meal_type_value": None,
                "region_value": None,
                "limit": 10,
            },
        ).execute()
        rows = resp.data or []
        logger.info(
            "search_meals_v2 returned %d rows for query='%s'",
            len(rows),
            query_text,
            extra={
                "invoking_func": "run_search",
                "invoking_purpose": "Demonstrate search_meals_v2 RPC",
                "next_step": "Print results",
                "resolution": "",
            },
        )
        for row in rows:
            logger.info(
                "%s (score=%s)",
                row.get("title"),
                row.get("score"),
                extra={
                    "invoking_func": "run_search",
                    "invoking_purpose": "Print search result",
                    "next_step": "Next row",
                    "resolution": "",
                },
            )
        return
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "search_meals_v2 failed (falling back to legacy search_meals): %s",
            exc,
            extra={
                "invoking_func": "run_search",
                "invoking_purpose": "Demonstrate search RPC",
                "next_step": "Call legacy search_meals RPC",
                "resolution": "",
            },
        )

    # Legacy fallback
    resp = client.rpc(
        "search_meals",
        {
            "diet_value": "vegan",
            "meal_type_value": "main",
            "region_value": "indian",
            "limit": 5,
        },
    ).execute()
    rows = resp.data or []
    for row in rows:
        logger.info(
            "%s - %s mins",
            row.get("title"),
            row.get("total_time_minutes"),
            extra={
                "invoking_func": "run_search",
                "invoking_purpose": "Demonstrate legacy search_meals RPC",
                "next_step": "",
                "resolution": "",
            },
        )


if __name__ == "__main__":
    run_search()
