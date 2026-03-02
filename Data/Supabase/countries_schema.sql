DROP TABLE IF EXISTS public.countries;
-- Country Table Creation
CREATE TABLE IF NOT EXISTS public.countries (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    continent TEXT,
    capital TEXT,
    flag_1x1 TEXT,
    flag_4x3 TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Enable RLS
ALTER TABLE public.countries ENABLE ROW LEVEL SECURITY;

-- Public read access
CREATE POLICY "Allow public read access" ON public.countries
    FOR SELECT USING (true);
