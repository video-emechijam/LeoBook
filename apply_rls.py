import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv('leobookapp/.env')
url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_SERVICE_KEY')
supabase = create_client(url, key)

tables = ['predictions', 'schedules', 'live_scores', 'match_odds', 'teams', 'region_league']
for t in tables:
    sql = f"""
    ALTER TABLE public.{t} ENABLE ROW LEVEL SECURITY;
    DROP POLICY IF EXISTS "Public Read Access {t}" ON public.{t};
    CREATE POLICY "Public Read Access {t}" ON public.{t} FOR SELECT USING (true);
    """
    try:
        supabase.rpc('exec_sql', {'query': sql}).execute()
        print(f'RLS enabled for {t}')
    except Exception as e:
        print(f'Missing or err on {t}: {e}')

for t in tables:
    pub_sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = 'supabase_realtime' AND tablename = '{t}') THEN
            ALTER PUBLICATION supabase_realtime ADD TABLE public.{t};
        END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END $$;
    """
    try:
        supabase.rpc('exec_sql', {'query': pub_sql}).execute()
        print(f'Added {t} to realtime pub')
    except Exception as e:
        pass

# Test anon
supabase_anon = create_client(url, os.getenv('SUPABASE_ANON_KEY'))
res = supabase_anon.table('predictions').select('fixture_id').limit(5).execute()
print(f'Predictions length with Anon KEY: {len(res.data)}')
