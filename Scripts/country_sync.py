import os
import pandas as pd
import logging
import sys
from pathlib import Path
from typing import Optional

# Add project root to sys.path for internal imports
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

try:
    from Data.Access.storage_manager import StorageManager
except ImportError:
    print("❌ Error: Could not import StorageManager.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class CountryAssetSyncer:
    def __init__(self):
        self.storage = StorageManager(bucket_name="logos")
        self.csv_path = project_root / "Data" / "Store" / "countries.csv"
        self.flags_base_dir = project_root / "Modules" / "Assets" / "flag-icons-main"

    def sync_country_assets(self):
        if not self.csv_path.exists():
            logger.error(f"❌ countries.csv not found at {self.csv_path}")
            return

        df = pd.read_csv(self.csv_path)
        total_rows = len(df)
        logger.info(f"🚀 Starting asset sync for {total_rows} countries...")

        for index, row in df.iterrows():
            country_name = row['name']
            code = str(row['code']).lower()
            
            # Handle flag_1x1
            f1x1_local = self.flags_base_dir / "flags" / "1x1" / f"{code}.svg"
            if f1x1_local.exists():
                remote_path = f"flags/1x1/{code}.svg"
                url = self.storage.upload_file(f1x1_local, remote_path, content_type="image/svg+xml")
                if url:
                    df.at[index, 'flag_1x1'] = url
            
            # Handle flag_4x3
            f4x3_local = self.flags_base_dir / "flags" / "4x3" / f"{code}.svg"
            if f4x3_local.exists():
                remote_path = f"flags/4x3/{code}.svg"
                url = self.storage.upload_file(f4x3_local, remote_path, content_type="image/svg+xml")
                if url:
                    df.at[index, 'flag_4x3'] = url

            if (index + 1) % 10 == 0:
                logger.info(f"📦 Progress: {index + 1}/{total_rows} countries processed.")

        # Save updated CSV
        df.to_csv(self.csv_path, index=False)
        logger.info(f"✅ countries.csv updated with public URLs.")

    def generate_supabase_sql(self):
        sql = """
-- Country Table Creation
CREATE TABLE IF NOT EXISTS public.countries (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    continent TEXT,
    capital TEXT,
    flag_1x1 TEXT,
    flag_4x3 TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Enable RLS
ALTER TABLE public.countries ENABLE ROW LEVEL SECURITY;

-- Public read access
CREATE POLICY "Allow public read access" ON public.countries
    FOR SELECT USING (true);
"""
        sql_path = project_root / "Data" / "Store" / "countries_schema.sql"
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(sql)
        logger.info(f"📜 Supabase SQL schema generated at {sql_path}")

if __name__ == "__main__":
    syncer = CountryAssetSyncer()
    syncer.sync_country_assets()
    syncer.generate_supabase_sql()
