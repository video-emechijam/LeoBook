# asset_manager.py: Module for Asset Synchronization.
# Part of LeoBook Assets Module
#
# Functions: sync_team_assets(), sync_league_assets(), sync_region_flags()
# Called by: Leo.py (--assets utility)

import os
import json
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from Data.Access.supabase_client import get_supabase_client
from Data.Access.league_db import DB_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = Path(__file__).parent.parent.parent
ASSETS_DIR = Path(__file__).parent
TEAMS_CSV = PROJECT_ROOT / "Data" / "Store" / "teams.csv"
LEAGUES_CSV = PROJECT_ROOT / "Data" / "Store" / "region_league.csv"
FLAG_ICONS_DIR = ASSETS_DIR / "flag-icons-main"
COUNTRY_JSON = FLAG_ICONS_DIR / "country.json"

# Manual overrides: Flashscore region name → ISO code
# For sub-national entities and naming mismatches not in country.json
REGION_TO_ISO_OVERRIDES = {
    "ENGLAND": "gb-eng",
    "SCOTLAND": "gb-sct",
    "WALES": "gb-wls",
    "NORTHERN IRELAND": "gb-nir",
    "IVORY COAST": "ci",
    "DR CONGO": "cd",
    "ESWATINI": "sz",
    "UNITED ARAB EMIRATES": "ae",
    "SOUTH KOREA": "kr",
    "NORTH MACEDONIA": "mk",
    "TRINIDAD AND TOBAGO": "tt",
    "BOSNIA AND HERZEGOVINA": "ba",
    "WORLD": "un",
    "EUROPE": "eu",
    "AFRICA": "af",           # Uses Afghanistan flag as placeholder — will use generic
    "SOUTH AMERICA": "br",    # Placeholder — no continent flag
    "NORTH & CENTRAL AMERICA": "us",  # Placeholder
    "AUSTRALIA & OCEANIA": "au",
    "OCEANIA": "au",
    "MACAO": "mo",
    "SEYCHELLES": "sc",
    "SIERRA LEONE": "sl",
    "MAURITIUS": "mu",
    "RWANDA": "rw",
    "BURUNDI": "bi",
    "CHAD": "td",
    "GUINEA": "gn",
    "LIBYA": "ly",
    "KUWAIT": "kw",
    "FIJI": "fj",
    "BOTSWANA": "bw",
    "BURKINA FASO": "bf",
    "TURKEY": "tr",
}

def _build_region_to_iso_map() -> dict:
    """Builds a region name → ISO code mapping from country.json + overrides."""
    mapping = dict(REGION_TO_ISO_OVERRIDES)  # Start with overrides

    if COUNTRY_JSON.exists():
        with open(COUNTRY_JSON, 'r', encoding='utf-8') as f:
            countries = json.load(f)
        for entry in countries:
            name_upper = entry['name'].upper()
            if name_upper not in mapping:
                mapping[name_upper] = entry['code']
    else:
        logger.warning(f"[!] country.json not found at {COUNTRY_JSON}")

    return mapping


def download_image(url: str, save_path: Path) -> bool:
    """Downloads an image from a URL and saves it temporarily."""
    if not url or url.lower() in ["unknown", "unknown url", "none"]:
        return False

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(response.content)
        return True
    except Exception as e:
        logger.error(f"[x] Error downloading {url}: {e}")
        return False

def upload_to_supabase(storage_client, bucket_name: str, file_path: Path, remote_filename: str):
    """Uploads a file to Supabase storage bucket."""
    try:
        with open(file_path, 'rb') as f:
            res = storage_client.from_(bucket_name).upload(
                path=remote_filename,
                file=f,
                file_options={"cache-control": "3600", "upsert": "true"}
            )
            logger.info(f"[+] Uploaded {remote_filename} → {bucket_name}")
            return res
    except Exception as e:
        logger.error(f"[x] Error uploading {remote_filename} to {bucket_name}: {e}")
        return None

def ensure_bucket_exists(storage_client, bucket_name: str):
    """Checks if a bucket exists, creates it if it doesn't."""
    try:
        buckets = storage_client.list_buckets()
        bucket_names = [b.name for b in buckets]
        if bucket_name not in bucket_names:
            logger.info(f"[*] Bucket '{bucket_name}' not found. Creating...")
            storage_client.create_bucket(bucket_name, options={"public": True})
            logger.info(f"[+] Bucket '{bucket_name}' created.")
        else:
            logger.info(f"[*] Bucket '{bucket_name}' exists.")
        return True
    except Exception as e:
        logger.error(f"[x] Error ensuring bucket '{bucket_name}': {e}")
        return False

def sync_team_assets(limit: Optional[int] = None):
    """Syncs team crests to Supabase storage."""
    client = get_supabase_client()
    if not client:
        return

    df = pd.read_csv(TEAMS_CSV)
    if limit:
        df = df.head(limit)

    storage = client.storage
    ensure_bucket_exists(storage, "teams")

    temp_dir = Path("temp_assets")
    temp_dir.mkdir(exist_ok=True)

    logger.info(f"[*] Starting team assets sync. Total teams: {len(df)}")

    for _, row in df.iterrows():
        team_id = row['team_id']
        url = row['team_crest']

        if team_id == "Unknown" or not url or url.lower() in ["unknown", "unknown url"]:
            continue

        filename = f"{team_id}.png"
        local_path = temp_dir / filename

        if download_image(url, local_path):
            upload_to_supabase(storage, "teams", local_path, filename)
            os.remove(local_path)

    if temp_dir.exists():
        try:
            temp_dir.rmdir()
        except:
            pass

def sync_league_assets(limit: Optional[int] = None):
    """Syncs league crests to Supabase storage."""
    client = get_supabase_client()
    if not client:
        return

    df = pd.read_csv(LEAGUES_CSV)
    if limit:
        df = df.head(limit)

    storage = client.storage
    ensure_bucket_exists(storage, "leagues")

    temp_dir = Path("temp_assets_leagues")
    temp_dir.mkdir(exist_ok=True)

    logger.info(f"[*] Starting league assets sync. Total leagues: {len(df)}")

    for _, row in df.iterrows():
        league_id = row['league_id']
        url = row['league_crest']

        if league_id == "Unknown" or not url or url.lower() in ["unknown", "unknown url", "none"]:
            continue

        filename = f"{league_id}.png"
        local_path = temp_dir / filename

        if download_image(url, local_path):
            upload_to_supabase(storage, "leagues", local_path, filename)
            os.remove(local_path)

    if temp_dir.exists():
        try:
            temp_dir.rmdir()
        except:
            pass

def sync_region_flags():
    """Syncs region flag SVGs from local flag-icons-main to Supabase storage.
    Also backfills the region_flag column in region_league.csv."""
    client = get_supabase_client()
    if not client:
        logger.error("[x] No Supabase client — aborting flag sync.")
        return

    if not LEAGUES_CSV.exists():
        logger.error(f"[x] {LEAGUES_CSV} not found.")
        return

    # Build mapping
    region_map = _build_region_to_iso_map()

    storage = client.storage
    ensure_bucket_exists(storage, "flags")

    # Get Supabase URL for constructing public URLs
    supabase_url = os.getenv("SUPABASE_URL", "").rstrip("/")

    df = pd.read_csv(LEAGUES_CSV)
    unique_regions = df['region'].dropna().unique()

    uploaded = 0
    skipped = 0
    not_found = []

    logger.info(f"[*] Starting region flag sync. Unique regions: {len(unique_regions)}")

    for region in sorted(unique_regions):
        region_upper = region.upper().strip()
        if region_upper in ("NONE", "UNKNOWN", ""):
            skipped += 1
            continue

        iso_code = region_map.get(region_upper)
        if not iso_code:
            not_found.append(region)
            skipped += 1
            continue

        # Locate local SVG file (4x3 ratio for flags)
        svg_path = FLAG_ICONS_DIR / "flags" / "4x3" / f"{iso_code}.svg"
        if not svg_path.exists():
            logger.warning(f"[!] SVG not found for {region} (code: {iso_code}): {svg_path}")
            not_found.append(f"{region} ({iso_code})")
            skipped += 1
            continue

        # Upload to Supabase Storage: flags/{iso_code}.svg
        remote_name = f"{iso_code}.svg"
        result = upload_to_supabase(storage, "flags", svg_path, remote_name)
        if result:
            uploaded += 1

        # Construct public URL and backfill CSV
        public_url = f"{supabase_url}/storage/v1/object/public/flags/{remote_name}"
        mask = df['region'].str.upper().str.strip() == region_upper
        df.loc[mask, 'region_flag'] = public_url
        df.loc[mask, 'last_updated'] = datetime.now(timezone.utc).isoformat()

    # Save updated CSV
    df.to_csv(LEAGUES_CSV, index=False)
    logger.info(f"[✓] Region flags: {uploaded} uploaded, {skipped} skipped.")
    if not_found:
        logger.warning(f"[!] Unmapped regions: {not_found}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sync assets to Supabase Storage.")
    parser.add_argument("--teams", action="store_true", help="Sync team crests")
    parser.add_argument("--leagues", action="store_true", help="Sync league crests")
    parser.add_argument("--flags", action="store_true", help="Sync region flags")
    parser.add_argument("--all", action="store_true", help="Sync all assets")
    parser.add_argument("--limit", type=int, help="Limit items for testing")

    args = parser.parse_args()

    if args.all or args.teams:
        sync_team_assets(limit=args.limit)
    if args.all or args.leagues:
        sync_league_assets(limit=args.limit)
    if args.all or args.flags:
        sync_region_flags()

    if not (args.all or args.teams or args.leagues or args.flags):
        parser.print_help()
