import os
import sys
from dotenv import load_dotenv
from supabase import create_client

load_dotenv('leobookapp/.env')
url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(url, key)

tables = ['predictions', 'schedules', 'live_scores', 'match_odds', 'teams', 'region_league', 'standings']

sql = """
DO $$
BEGIN
    -- Ensure publication exists
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime') THEN
        CREATE PUBLICATION supabase_realtime;
    END IF;
END $$;
"""
try:
    supabase.rpc('exec_sql', {'query': sql}).execute()
    print("Ensured supabase_realtime publication exists.")
except Exception as e:
    print("Error creating publication:", e)

for t in tables:
    pub_sql = f"""
    DO $$
    BEGIN
        -- Add table to publication if it's not already there
        IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '{t}') THEN
            IF NOT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = 'supabase_realtime' AND tablename = '{t}') THEN
                ALTER PUBLICATION supabase_realtime ADD TABLE public.{t};
            END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END $$;
    """
    try:
        supabase.rpc('exec_sql', {'query': pub_sql}).execute()
        print(f"Added {t} to realtime publication.")
    except Exception as e:
        print(f"Error adding {t} to realtime pub:", e)

print("Realtime configuration complete.")
