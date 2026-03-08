-- =============================================================================
-- GLOBAL SUPABASE SCHEMA (LeoBook) v3.0
-- Single source of truth. Columns MUST match db_helpers.py files_and_headers.
-- PostgreSQL naming: only [a-z0-9_] allowed in column names.
-- CSV "over_2.5" maps to Supabase "over_2_5" via sync_manager.batch_upsert().
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- 1. USER MANAGEMENT
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.profiles (
    id UUID REFERENCES auth.users (id) ON DELETE CASCADE PRIMARY KEY,
    email TEXT,
    username TEXT UNIQUE,
    full_name TEXT,
    avatar_url TEXT,
    tier TEXT DEFAULT 'free',
    credits INTEGER DEFAULT 0,
    created_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW (),
        updated_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW (),
        last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Users can view own profile" ON public.profiles;

CREATE POLICY "Users can view own profile" ON public.profiles FOR
SELECT USING (auth.uid () = id);

DROP POLICY IF EXISTS "Users can update own profile" ON public.profiles;

CREATE POLICY "Users can update own profile" ON public.profiles FOR
UPDATE USING (auth.uid () = id);

-- =============================================================================
-- 2. CUSTOM RULE ENGINE
-- =============================================================================

-- =============================================================================
-- 2. CUSTOM RULE ENGINE
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.custom_rules (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID REFERENCES public.profiles(id) ON DELETE CASCADE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    logic JSONB DEFAULT '{}'::jsonb NOT NULL,
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_default BOOLEAN DEFAULT false,
    scope JSONB DEFAULT '{}'::jsonb,
    accuracy JSONB DEFAULT '{}'::jsonb,
    backtest_csv_url TEXT
);

ALTER TABLE public.custom_rules ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Users can fully manage own rules" ON public.custom_rules;

CREATE POLICY "Users can fully manage own rules" ON public.custom_rules FOR ALL USING (auth.uid () = user_id);

CREATE TABLE IF NOT EXISTS public.rule_executions (
    id UUID DEFAULT uuid_generate_v4 () PRIMARY KEY,
    rule_id UUID REFERENCES public.custom_rules (id) ON DELETE CASCADE,
    fixture_id TEXT,
    user_id UUID REFERENCES public.profiles (id),
    result JSONB,
    executed_at TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW (),
        last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);

ALTER TABLE public.rule_executions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Users can view own rule executions" ON public.rule_executions;

CREATE POLICY "Users can view own rule executions" ON public.rule_executions FOR
SELECT USING (auth.uid () = user_id);

-- =============================================================================
-- 3. CORE DATA TABLES (mirrors db_helpers.py files_and_headers exactly)
-- =============================================================================

-- predictions (37 columns) — CSV key: fixture_id
-- Note: CSV "over_2.5" → Supabase "over_2_5" (dots illegal in PostgreSQL identifiers)
CREATE TABLE IF NOT EXISTS public.predictions (
    fixture_id TEXT PRIMARY KEY,
    date TEXT,
    match_time TEXT,
    region_league TEXT,
    home_team TEXT,
    away_team TEXT,
    home_team_id TEXT,
    away_team_id TEXT,
    prediction TEXT,
    confidence TEXT,
    reason TEXT,
    xg_home TEXT,
    xg_away TEXT,
    btts TEXT,
    over_2_5 TEXT,
    best_score TEXT,
    top_scores TEXT,
    home_form_n TEXT,
    away_form_n TEXT,
    home_tags TEXT,
    away_tags TEXT,
    h2h_tags TEXT,
    standings_tags TEXT,
    h2h_count TEXT,
    actual_score TEXT,
    outcome_correct TEXT,
    status TEXT,
    match_link TEXT,
    odds TEXT,
    market_reliability_score TEXT,
    home_crest_url TEXT,
    away_crest_url TEXT,
    recommendation_score TEXT,
    h2h_fixture_ids TEXT,
    form_fixture_ids TEXT,
    standings_snapshot TEXT,
    league_stage TEXT,
    home_score TEXT,
    away_score TEXT,
    last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);

ALTER TABLE public.predictions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access Predictions" ON public.predictions;

CREATE POLICY "Public Read Access Predictions" ON public.predictions FOR
SELECT USING (true);

-- schedules (15 columns) — CSV key: fixture_id
CREATE TABLE IF NOT EXISTS public.schedules (
    fixture_id TEXT PRIMARY KEY,
    date TEXT,
    match_time TEXT,
    region_league TEXT,
    league_id TEXT,
    home_team TEXT,
    away_team TEXT,
    home_team_id TEXT,
    away_team_id TEXT,
    home_score TEXT,
    away_score TEXT,
    match_status TEXT,
    match_link TEXT,
    league_stage TEXT,
    last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);
-- Migration: add league_id if missing
ALTER TABLE public.schedules ADD COLUMN IF NOT EXISTS league_id TEXT;

ALTER TABLE public.schedules ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access Schedules" ON public.schedules;

CREATE POLICY "Public Read Access Schedules" ON public.schedules FOR
SELECT USING (true);

-- teams (6 CSV columns + search enrichment) — CSV key: team_id
CREATE TABLE IF NOT EXISTS public.teams (
    team_id TEXT PRIMARY KEY,
    team_name TEXT,
    league_ids TEXT,
    team_crest TEXT,
    team_url TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    country TEXT,
    city TEXT,
    stadium TEXT,
    other_names JSONB DEFAULT '[]',
    abbreviations JSONB DEFAULT '[]',
    search_terms TEXT[] DEFAULT ARRAY[]::TEXT[]
);
-- Migration: rename rl_ids → league_ids if old column exists
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='teams' AND column_name='rl_ids') THEN
    ALTER TABLE public.teams RENAME COLUMN rl_ids TO league_ids;
  END IF;
END $$;

ALTER TABLE public.teams ADD COLUMN IF NOT EXISTS league_ids TEXT;

ALTER TABLE public.teams ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access Teams" ON public.teams;

CREATE POLICY "Public Read Access Teams" ON public.teams FOR
SELECT USING (true);

-- region_league (9 CSV columns + search enrichment) — CSV key: league_id
CREATE TABLE IF NOT EXISTS public.region_league (
    league_id TEXT PRIMARY KEY,
    region TEXT,
    region_flag TEXT,
    region_url TEXT,
    league TEXT,
    league_crest TEXT,
    league_url TEXT,
    date_updated TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    other_names JSONB DEFAULT '[]',
    abbreviations JSONB DEFAULT '[]',
    search_terms TEXT[] DEFAULT ARRAY[]::TEXT[]
);
-- Migration: rename rl_id → league_id if old column exists
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='region_league' AND column_name='rl_id') THEN
    ALTER TABLE public.region_league RENAME COLUMN rl_id TO league_id;
  END IF;
END $$;
-- Migration: drop deprecated columns
ALTER TABLE public.region_league DROP COLUMN IF EXISTS logo_url;

ALTER TABLE public.region_league DROP COLUMN IF EXISTS country;

ALTER TABLE public.region_league ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access RegionLeague" ON public.region_league;

CREATE POLICY "Public Read Access RegionLeague" ON public.region_league FOR
SELECT USING (true);

-- standings (15 columns) — CSV key: standings_key
CREATE TABLE IF NOT EXISTS public.standings (
    standings_key TEXT PRIMARY KEY,
    league_id TEXT,
    team_id TEXT,
    team_name TEXT,
    position INTEGER,
    played INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    goals_for INTEGER,
    goals_against INTEGER,
    goal_difference INTEGER,
    points INTEGER,
    last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW (),
        region_league TEXT
);

ALTER TABLE public.standings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access Standings" ON public.standings;

CREATE POLICY "Public Read Access Standings" ON public.standings FOR
SELECT USING (true);

-- fb_matches (17 columns) — CSV key: site_match_id
CREATE TABLE IF NOT EXISTS public.fb_matches (
    site_match_id TEXT PRIMARY KEY,
    date TEXT,
    time TEXT,
    home_team TEXT,
    away_team TEXT,
    league TEXT,
    url TEXT,
    last_extracted TEXT,
    fixture_id TEXT,
    matched TEXT,
    odds TEXT,
    booking_status TEXT,
    booking_details TEXT,
    booking_code TEXT,
    booking_url TEXT,
    status TEXT,
    last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);

ALTER TABLE public.fb_matches ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access FBMatches" ON public.fb_matches;

CREATE POLICY "Public Read Access FBMatches" ON public.fb_matches FOR
SELECT USING (true);

-- live_scores (11 columns) — CSV key: fixture_id
CREATE TABLE IF NOT EXISTS public.live_scores (
    fixture_id TEXT PRIMARY KEY,
    home_team TEXT,
    away_team TEXT,
    home_score TEXT,
    away_score TEXT,
    minute TEXT,
    status TEXT,
    region_league TEXT,
    match_link TEXT,
    timestamp TIMESTAMP
    WITH
        TIME ZONE,
        last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);

ALTER TABLE public.live_scores ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access LiveScores" ON public.live_scores;

CREATE POLICY "Public Read Access LiveScores" ON public.live_scores FOR
SELECT USING (true);

-- match_odds (v8.0) — CSV key: fixture_id, market_id, exact_outcome, line
CREATE TABLE IF NOT EXISTS public.match_odds (
    fixture_id TEXT NOT NULL,
    site_match_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    base_market TEXT NOT NULL,
    category TEXT,
    exact_outcome TEXT NOT NULL,
    line TEXT,
    odds_value DECIMAL(10, 3),
    likelihood_pct INTEGER,
    rank_in_list INTEGER,
    extracted_at TEXT,
    last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW (),
        PRIMARY KEY (
            fixture_id,
            market_id,
            exact_outcome,
            line
        )
);

ALTER TABLE public.match_odds ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access MatchOdds" ON public.match_odds;

CREATE POLICY "Public Read Access MatchOdds" ON public.match_odds FOR
SELECT USING (true);

-- =============================================================================
-- 4. REPORTING & AUDIT
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.accuracy_reports (
    report_id TEXT PRIMARY KEY,
    timestamp TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW (),
        volume INTEGER DEFAULT 0,
        win_rate DECIMAL(5, 2) DEFAULT 0,
        return_pct DECIMAL(5, 2) DEFAULT 0,
        period TEXT DEFAULT 'last_24h',
        last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);

ALTER TABLE public.accuracy_reports ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access AccuracyReports" ON public.accuracy_reports;

CREATE POLICY "Public Read Access AccuracyReports" ON public.accuracy_reports FOR
SELECT USING (true);

CREATE TABLE IF NOT EXISTS public.audit_log (
    id UUID DEFAULT uuid_generate_v4 () PRIMARY KEY,
    timestamp TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW (),
        event_type TEXT NOT NULL,
        description TEXT,
        balance_before DECIMAL(15, 2),
        balance_after DECIMAL(15, 2),
        stake DECIMAL(15, 2),
        status TEXT DEFAULT 'success',
        last_updated TIMESTAMP
    WITH
        TIME ZONE DEFAULT NOW ()
);

ALTER TABLE public.audit_log ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access AuditLog" ON public.audit_log;

CREATE POLICY "Public Read Access AuditLog" ON public.audit_log FOR
SELECT USING (true);

-- =============================================================================
-- 5. ADAPTIVE LEARNING
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.learning_weights (
    region_league TEXT PRIMARY KEY,
    weights JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence_calibration JSONB NOT NULL DEFAULT '{"Very High": 0.70, "High": 0.60, "Medium": 0.50, "Low": 0.40}'::jsonb,
    predictions_analyzed INTEGER DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE public.learning_weights ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Public Read Access LearningWeights" ON public.learning_weights;

CREATE POLICY "Public Read Access LearningWeights" ON public.learning_weights FOR
SELECT USING (true);

-- =============================================================================
-- 6. AUTO-UPDATE TRIGGERS
-- =============================================================================

CREATE OR REPLACE FUNCTION update_last_updated_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.last_updated = NOW();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_profiles_last_updated ON public.profiles;

CREATE TRIGGER update_profiles_last_updated BEFORE UPDATE ON public.profiles FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_rules_last_updated ON public.custom_rules;

CREATE TRIGGER update_rules_last_updated BEFORE UPDATE ON public.custom_rules FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_predictions_last_updated ON public.predictions;

CREATE TRIGGER update_predictions_last_updated BEFORE UPDATE ON public.predictions FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_schedules_last_updated ON public.schedules;

CREATE TRIGGER update_schedules_last_updated BEFORE UPDATE ON public.schedules FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_teams_last_updated ON public.teams;

CREATE TRIGGER update_teams_last_updated BEFORE UPDATE ON public.teams FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_standings_last_updated ON public.standings;

CREATE TRIGGER update_standings_last_updated BEFORE UPDATE ON public.standings FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_fbmatches_last_updated ON public.fb_matches;

CREATE TRIGGER update_fbmatches_last_updated BEFORE UPDATE ON public.fb_matches FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_livescores_last_updated ON public.live_scores;

CREATE TRIGGER update_livescores_last_updated BEFORE UPDATE ON public.live_scores FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_reports_last_updated ON public.accuracy_reports;

CREATE TRIGGER update_reports_last_updated BEFORE UPDATE ON public.accuracy_reports FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_audit_last_updated ON public.audit_log;

CREATE TRIGGER update_audit_last_updated BEFORE UPDATE ON public.audit_log FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_learning_weights_last_updated ON public.learning_weights;

CREATE TRIGGER update_learning_weights_last_updated BEFORE UPDATE ON public.learning_weights FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

DROP TRIGGER IF EXISTS update_match_odds_last_updated ON public.match_odds;

CREATE TRIGGER update_match_odds_last_updated BEFORE UPDATE ON public.match_odds FOR EACH ROW EXECUTE PROCEDURE update_last_updated_column();

-- =============================================================================
-- 7. GRANTS
-- =============================================================================
GRANT SELECT ON public.predictions TO anon, authenticated;

GRANT SELECT ON public.schedules TO anon, authenticated;

GRANT SELECT ON public.teams TO anon, authenticated;

GRANT SELECT ON public.region_league TO anon, authenticated;

GRANT SELECT ON public.standings TO anon, authenticated;

GRANT SELECT ON public.fb_matches TO anon, authenticated;

GRANT SELECT ON public.live_scores TO anon, authenticated;

GRANT SELECT ON public.accuracy_reports TO anon, authenticated;

GRANT SELECT ON public.audit_log TO anon, authenticated;

GRANT SELECT ON public.learning_weights TO anon, authenticated;

GRANT SELECT ON public.match_odds TO anon, authenticated;

-- =============================================================================
-- 8. AUTH TRIGGERS (Moved to end to prevent blocking tables if permissions fail)
-- =============================================================================

DO $$
BEGIN
    -- Only try to create these if we can access the auth schema
    -- The service_role key sometimes lacks permission for triggers on auth.users
    BEGIN
        CREATE OR REPLACE FUNCTION public.handle_new_user()
        RETURNS TRIGGER AS $func$
        BEGIN
          INSERT INTO public.profiles (id, email, full_name, avatar_url)
          VALUES (new.id, new.email, new.raw_user_meta_data->>'full_name', new.raw_user_meta_data->>'avatar_url');
          RETURN new;
        END;
        $func$ LANGUAGE plpgsql SECURITY DEFINER;

        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'auth' AND table_name = 'users') THEN
            DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
            CREATE TRIGGER on_auth_user_created AFTER INSERT ON auth.users FOR EACH ROW EXECUTE PROCEDURE public.handle_new_user();
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Gracefully log but don't fail the whole script
        RAISE NOTICE 'Skipping auth.users trigger due to missing permissions: %', SQLERRM;
    END;
END $$;