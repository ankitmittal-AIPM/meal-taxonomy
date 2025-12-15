import os
import sys
import httpx
import dotenv
from src.meal_taxonomy.logging_utils import get_logger

dotenv.load_dotenv()

logger = get_logger("test_ssl")

url = os.environ.get("SUPABASE_URL")
if not url:
	logger.error(
		"SUPABASE_URL environment variable not set",
		extra={
			"invoking_func": "__main__",
			"invoking_purpose": "Simple SSL / reachability check for Supabase URL",
			"next_step": "Set SUPABASE_URL in environment or .env",
			"resolution": "",
		},
	)
	sys.exit(1)

try:
	r = httpx.get(url)
	logger.info(
		"SUPABASE_URL status: %d",
		r.status_code,
		extra={
			"invoking_func": "__main__",
			"invoking_purpose": "Simple SSL / reachability check for Supabase URL",
			"next_step": "",
			"resolution": "",
		},
	)
except Exception as exc:
	logger.error(
		"HTTP request to SUPABASE_URL failed: %s",
		exc,
		extra={
			"invoking_func": "__main__",
			"invoking_purpose": "Simple SSL / reachability check for Supabase URL",
			"next_step": "Check network / URL",
			"resolution": "",
		},
		exc_info=True,
	)
