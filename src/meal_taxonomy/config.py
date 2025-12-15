"""
config.py

Purpose:
    Provide a single function get_supabase_client() that creates and returns
    a Supabase Python client using environment variables.

Usage:
    from meal_taxonomy.config import get_supabase_client
"""
from __future__ import annotations
import os       # os module to read environment variables

# Supabase client setup where env vars are used for configuration. Client connection details are not hardcoded.
from supabase import create_client, Client  # supabase-py v2 :contentReference[oaicite:3]{index=3}

from dotenv import load_dotenv      # Load environment variables from .env file

load_dotenv()  # loads .env

# Function to create and return a Supabase client. This is like building a database connection.
def get_supabase_client() -> Client:
    """Create a Supabase client using env vars."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # use service role for ETL, not anon key
    return create_client(url, key)

