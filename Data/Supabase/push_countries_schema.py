import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CountrySchemaPusher")

# Constants
PROJECT_ROOT = Path(__file__).parent.parent.parent
SQL_FILE = PROJECT_ROOT / "Data" / "Supabase" / "countries_schema.sql"

def push_countries_schema():
    """Reads the local countries_schema.sql and pushes it to Supabase via RPC."""
    load_dotenv(PROJECT_ROOT / ".env")
    
    url = os.environ.get("SUPABASE_URL")
    # Must use service key for DDL execution
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    
    if not url or not key:
        logger.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env")
        return False
        
    try:
        supabase: Client = create_client(url, key)
    except Exception as e:
        logger.error(f"Failed to initialize Supabase client: {e}")
        return False

    if not SQL_FILE.exists():
        logger.error(f"Schema file not found at {SQL_FILE}")
        return False
        
    logger.info(f"Reading schema from {SQL_FILE}")
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    logger.info("Pushing countries schema to Supabase via RPC 'execute_sql'...")
    try:
        # Calls the 'execute_sql' function
        supabase.rpc('execute_sql', {'query': sql_content}).execute()
        
        # Refresh the PostgREST schema cache
        logger.info("Refreshing PostgREST schema cache via RPC 'refresh_schema'...")
        supabase.rpc('refresh_schema').execute()
        
        logger.info("Countries schema push successful.")
        return True
    except Exception as e:
        logger.error(f"Countries schema push failed. Details: {e}")
        return False

if __name__ == "__main__":
    success = push_countries_schema()
    sys.exit(0 if success else 1)
