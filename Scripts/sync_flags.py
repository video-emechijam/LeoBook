import os
import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Add project root to sys.path for internal imports
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

try:
    from Data.Access.storage_manager import StorageManager
    from Data.Access.metadata_linker import MetadataLinker
except ImportError:
    print("❌ Error: Could not import StorageManager or MetadataLinker.")
    print("Ensure you are running this from the project root or sys.path is correct.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class FlagSyncer:
    def __init__(self):
        self.project_root = project_root
        self.storage = StorageManager()
        self.linker = MetadataLinker(project_root)
        self.flags_base_dir = project_root / "Modules" / "Assets" / "flag-icons-main"
        self.country_json_path = self.flags_base_dir / "country.json"
        
        # Mapping overrides for regions that don't match country name 1:1
        self.MAPPING_OVERRIDES = {
            "NORTH & CENTRAL AMERICA": "North America",
            "DR CONGO": "Democratic Republic of the Congo",
            "IVORY COAST": "Côte d'Ivoire",
            "CZECH REPUBLIC": "Czech Republic",
            "SOUTH ASIA": "Asia", # Fallback
            "WORLD": "un", # United nations flag or similar
        }

    def load_country_data(self):
        if not self.country_json_path.exists():
            logger.error(f"❌ country.json not found at {self.country_json_path}")
            return []
        with open(self.country_json_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def find_flag_path(self, region_name: str, country_data: list) -> Optional[Path]:
        # Try override first
        search_name = self.MAPPING_OVERRIDES.get(region_name.upper(), region_name)
        
        # Specific code check (like 'un' for World or 'eu' for Europe)
        if search_name.lower() in ['un', 'eu', 'asean']:
            code = search_name.lower()
            return self.flags_base_dir / "flags" / "4x3" / f"{code}.svg"

        # Search in country_data
        for country in country_data:
            if country['name'].lower() == search_name.lower():
                # Prefer 4x3
                rel_path = country.get('flag_4x3', country.get('flag_1x1'))
                if rel_path:
                    return self.flags_base_dir / rel_path
        
        return None

    def sync(self):
        country_data = self.load_country_data()
        if not country_data:
            return

        # Get unique regions from CSV
        self.linker._load_leagues()
        if self.linker._leagues_df is None:
            logger.error("❌ Could not load region_league.csv")
            return

        regions = self.linker._leagues_df['region'].unique()
        logger.info(f"🌐 Found {len(regions)} unique regions to sync flags for.")

        success_count = 0
        fail_count = 0

        for region in regions:
            # Handle potential NaN values from pandas
            if not isinstance(region, str) or region.lower() == "unknown" or not region.strip():
                continue

            flag_file = self.find_flag_path(region, country_data)
            
            if not flag_file or not flag_file.exists():
                logger.warning(f"❓ No flag found for region: {region}")
                fail_count += 1
                continue

            # Upload to Supabase
            supabase_path = f"flags/{region.lower().replace(' ', '_')}.svg"
            public_url = self.storage.upload_file(flag_file, supabase_path)
            
            if public_url:
                # Link in CSV
                if self.linker.update_region_flag(region, public_url):
                    success_count += 1
                else:
                    fail_count += 1
            else:
                fail_count += 1

        self.linker.save()
        logger.info(f"🏁 Flag Sync Complete: {success_count} success, {fail_count} failed.")

if __name__ == "__main__":
    syncer = FlagSyncer()
    syncer.sync()
