import os
import httpx
import dotenv
dotenv.load_dotenv()

print("SUPABASE_URL =", os.environ.get("SUPABASE_URL"))

url = os.environ["SUPABASE_URL"]
r = httpx.get(url)
print("Status:", r.status_code)
