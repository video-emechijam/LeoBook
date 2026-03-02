# football_logos.py: Professional Football Logos Downloader (v4.0 - 2026-03-02)
# Part of LeoBook Scripts
#
# Features:
#   • ZIP collections for 31 major leagues (fast, CDN-direct)
#   • Countries mode: 160 countries × all team logos
#       - Primary:  requests + BeautifulSoup (no browser needed)
#       - Fallback: single shared Playwright browser (lazy-launched, reused)
#   • Hardcoded country list — no scraping required just to know what exists
#   • Connection-pooled requests.Session with auto-retry
#   • Parallel image downloads within each country
#   • tqdm progress bar for long country runs
#   • --force to re-download everything
#
# Install:  pip install requests beautifulsoup4 tqdm playwright
#           playwright install chromium   (only needed for JS-heavy country pages)
#
# Usage:    python football_logos.py                          # 31 league ZIPs
#           python football_logos.py --countries              # all 160 countries
#           python football_logos.py --countries --limit 10   # test first 10
#           python football_logos.py --countries --force      # re-download all
#           python football_logos.py --countries --workers 6  # more parallelism

import io
import logging
import zipfile
import argparse
import threading
import requests

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple

# ── Optional deps ──────────────────────────────────────────────
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────
PROJECT_ROOT      = Path(__file__).parent.parent
import sys
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

OUTPUT_DIR        = PROJECT_ROOT / "Modules" / "Assets" / "logos"
FLAGS_DIR         = PROJECT_ROOT / "Modules" / "Assets" / "flag-icons-main"
COUNTRIES_OUT_DIR = OUTPUT_DIR / "countries"

# ── Supabase Integration ──────────────────────────────────────
try:
    from Data.Access.storage_manager import StorageManager
    from Data.Access.metadata_linker import MetadataLinker
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

SITE_BASE = "https://football-logos.cc"

# ── CDN / league config ────────────────────────────────────────
CDN_BASE        = "https://assets.football-logos.cc/collections/"
CDN_SUFFIXES    = [
    "-2025-2026.football-logos.cc.zip",
    "-2024-2025.football-logos.cc.zip",
    ".football-logos.cc.zip",
]
CDN_FIXED_SLUGS = {"fifa-world-cup-2026"}

LEAGUE_SLUGS = [
    "fifa-world-cup-2026", "ucl-champions-league", "uefa-europa-league", "uefa-conference-league",
    "english-premier-league", "england-efl-championship", "england-efl-league-one", "england-efl-league-two",
    "spain-la-liga", "spain-la-liga-2", "italy-serie-a", "italy-serie-b",
    "germany-bundesliga", "germany-2-bundesliga", "france-ligue-1", "france-ligue-2",
    "portugal-primeira-liga", "netherlands-eredivisie", "belgium-pro-league", "turkey-super-lig",
    "scotland-premiership", "brazil-serie-a", "brazil-serie-b", "argentina-primera-division",
    "usa-mls", "saudi-arabia-pro-league", "romania-liga-1", "austria-bundesliga",
    "poland-ekstraklasa", "greece-super-league", "mexico-liga-mx",
]

# ── Country list — hardcoded from football-logos.cc/countries/ ─
# (160 entries as of 2026-03-02; run _refresh_country_list() to update)
COUNTRIES: list[dict] = [
    {"slug": "afghanistan",              "name": "Afghanistan",                      "count": 1},
    {"slug": "albania",                  "name": "Albania",                          "count": 21},
    {"slug": "algeria",                  "name": "Algeria",                          "count": 1},
    {"slug": "andorra",                  "name": "Andorra",                          "count": 9},
    {"slug": "angola",                   "name": "Angola",                           "count": 1},
    {"slug": "argentina",                "name": "Argentina",                        "count": 54},
    {"slug": "armenia",                  "name": "Armenia",                          "count": 10},
    {"slug": "australia",                "name": "Australia",                        "count": 17},
    {"slug": "austria",                  "name": "Austria",                          "count": 27},
    {"slug": "azerbaijan",               "name": "Azerbaijan",                       "count": 15},
    {"slug": "bahrain",                  "name": "Bahrain",                          "count": 1},
    {"slug": "bangladesh",               "name": "Bangladesh",                       "count": 1},
    {"slug": "belarus",                  "name": "Belarus",                          "count": 22},
    {"slug": "belgium",                  "name": "Belgium",                          "count": 28},
    {"slug": "benin",                    "name": "Benin",                            "count": 1},
    {"slug": "bolivia",                  "name": "Bolivia",                          "count": 3},
    {"slug": "bosnia-and-herzegovina",   "name": "Bosnia and Herzegovina",           "count": 20},
    {"slug": "botswana",                 "name": "Botswana",                         "count": 1},
    {"slug": "brazil",                   "name": "Brazil",                           "count": 48},
    {"slug": "burkina-faso",             "name": "Burkina Faso",                     "count": 1},
    {"slug": "burundi",                  "name": "Burundi",                          "count": 1},
    {"slug": "cabo-verde",               "name": "Cabo Verde",                       "count": 1},
    {"slug": "cameroon",                 "name": "Cameroon",                         "count": 2},
    {"slug": "canada",                   "name": "Canada",                           "count": 10},
    {"slug": "central-african-republic", "name": "Central African Republic",         "count": 1},
    {"slug": "chad",                     "name": "Chad",                             "count": 1},
    {"slug": "chile",                    "name": "Chile",                            "count": 17},
    {"slug": "china",                    "name": "China",                            "count": 20},
    {"slug": "colombia",                 "name": "Colombia",                         "count": 21},
    {"slug": "comoros",                  "name": "Comoros",                          "count": 1},
    {"slug": "congo-dr",                 "name": "Democratic Republic of the Congo", "count": 1},
    {"slug": "costa-rica",               "name": "Costa Rica",                       "count": 1},
    {"slug": "cote-d-ivoire",            "name": "Cote d'Ivoire",                    "count": 1},
    {"slug": "croatia",                  "name": "Croatia",                          "count": 16},
    {"slug": "cuba",                     "name": "Cuba",                             "count": 1},
    {"slug": "curacao",                  "name": "Curacao",                          "count": 1},
    {"slug": "cyprus",                   "name": "Cyprus",                           "count": 23},
    {"slug": "czech-republic",           "name": "Czech Republic",                   "count": 33},
    {"slug": "denmark",                  "name": "Denmark",                          "count": 29},
    {"slug": "djibouti",                 "name": "Djibouti",                         "count": 1},
    {"slug": "ecuador",                  "name": "Ecuador",                          "count": 16},
    {"slug": "egypt",                    "name": "Egypt",                            "count": 23},
    {"slug": "el-salvador",              "name": "El Salvador",                      "count": 2},
    {"slug": "england",                  "name": "England",                          "count": 185},
    {"slug": "equatorial-guinea",        "name": "Equatorial Guinea",                "count": 1},
    {"slug": "eritrea",                  "name": "Eritrea",                          "count": 1},
    {"slug": "estonia",                  "name": "Estonia",                          "count": 15},
    {"slug": "eswatini",                 "name": "Eswatini",                         "count": 1},
    {"slug": "ethiopia",                 "name": "Ethiopia",                         "count": 1},
    {"slug": "faroe-islands",            "name": "Faroe Islands",                    "count": 14},
    {"slug": "finland",                  "name": "Finland",                          "count": 24},
    {"slug": "france",                   "name": "France",                           "count": 63},
    {"slug": "gabon",                    "name": "Gabon",                            "count": 1},
    {"slug": "gambia",                   "name": "Gambia",                           "count": 1},
    {"slug": "georgia",                  "name": "Georgia",                          "count": 17},
    {"slug": "germany",                  "name": "Germany",                          "count": 65},
    {"slug": "ghana",                    "name": "Ghana",                            "count": 1},
    {"slug": "gibraltar",                "name": "Gibraltar",                        "count": 12},
    {"slug": "greece",                   "name": "Greece",                           "count": 23},
    {"slug": "guatemala",                "name": "Guatemala",                        "count": 1},
    {"slug": "guinea-bissau",            "name": "Guinea-Bissau",                    "count": 1},
    {"slug": "haiti",                    "name": "Haiti",                            "count": 1},
    {"slug": "honduras",                 "name": "Honduras",                         "count": 1},
    {"slug": "hungary",                  "name": "Hungary",                          "count": 32},
    {"slug": "iceland",                  "name": "Iceland",                          "count": 20},
    {"slug": "india",                    "name": "India",                            "count": 16},
    {"slug": "indonesia",                "name": "Indonesia",                        "count": 20},
    {"slug": "iran",                     "name": "Iran",                             "count": 18},
    {"slug": "iraq",                     "name": "Iraq",                             "count": 1},
    {"slug": "israel",                   "name": "Israel",                           "count": 25},
    {"slug": "italy",                    "name": "Italy",                            "count": 106},
    {"slug": "jamaica",                  "name": "Jamaica",                          "count": 1},
    {"slug": "japan",                    "name": "Japan",                            "count": 27},
    {"slug": "jordan",                   "name": "Jordan",                           "count": 1},
    {"slug": "kazakhstan",               "name": "Kazakhstan",                       "count": 22},
    {"slug": "kenya",                    "name": "Kenya",                            "count": 1},
    {"slug": "kosovo",                   "name": "Kosovo",                           "count": 17},
    {"slug": "latvia",                   "name": "Latvia",                           "count": 15},
    {"slug": "lebanon",                  "name": "Lebanon",                          "count": 1},
    {"slug": "lesotho",                  "name": "Lesotho",                          "count": 1},
    {"slug": "liberia",                  "name": "Liberia",                          "count": 1},
    {"slug": "libya",                    "name": "Libya",                            "count": 1},
    {"slug": "liechtenstein",            "name": "Liechtenstein",                    "count": 3},
    {"slug": "lithuania",                "name": "Lithuania",                        "count": 16},
    {"slug": "luxembourg",               "name": "Luxembourg",                       "count": 18},
    {"slug": "madagascar",               "name": "Madagascar",                       "count": 1},
    {"slug": "malawi",                   "name": "Malawi",                           "count": 1},
    {"slug": "malaysia",                 "name": "Malaysia",                         "count": 11},
    {"slug": "mali",                     "name": "Mali",                             "count": 1},
    {"slug": "malta",                    "name": "Malta",                            "count": 19},
    {"slug": "mauritania",               "name": "Mauritania",                       "count": 1},
    {"slug": "mauritius",                "name": "Mauritius",                        "count": 1},
    {"slug": "mexico",                   "name": "Mexico",                           "count": 21},
    {"slug": "moldova",                  "name": "Moldova",                          "count": 7},
    {"slug": "montenegro",               "name": "Montenegro",                       "count": 12},
    {"slug": "morocco",                  "name": "Morocco",                          "count": 18},
    {"slug": "mozambique",               "name": "Mozambique",                       "count": 1},
    {"slug": "namibia",                  "name": "Namibia",                          "count": 1},
    {"slug": "nepal",                    "name": "Nepal",                            "count": 1},
    {"slug": "netherlands",              "name": "Netherlands",                      "count": 40},
    {"slug": "new-zealand",              "name": "New Zealand",                      "count": 1},
    {"slug": "nicaragua",                "name": "Nicaragua",                        "count": 1},
    {"slug": "niger",                    "name": "Niger",                            "count": 1},
    {"slug": "nigeria",                  "name": "Nigeria",                          "count": 1},
    {"slug": "north-korea",              "name": "North Korea",                      "count": 1},
    {"slug": "north-macedonia",          "name": "North Macedonia",                  "count": 14},
    {"slug": "northern-ireland",         "name": "Northern Ireland",                 "count": 16},
    {"slug": "norway",                   "name": "Norway",                           "count": 30},
    {"slug": "oman",                     "name": "Oman",                             "count": 1},
    {"slug": "pakistan",                 "name": "Pakistan",                         "count": 1},
    {"slug": "palestine",                "name": "Palestine",                        "count": 1},
    {"slug": "panama",                   "name": "Panama",                           "count": 1},
    {"slug": "paraguay",                 "name": "Paraguay",                         "count": 11},
    {"slug": "peru",                     "name": "Peru",                             "count": 19},
    {"slug": "poland",                   "name": "Poland",                           "count": 33},
    {"slug": "portugal",                 "name": "Portugal",                         "count": 114},
    {"slug": "qatar",                    "name": "Qatar",                            "count": 22},
    {"slug": "republic-of-ireland",      "name": "Republic of Ireland",              "count": 26},
    {"slug": "romania",                  "name": "Romania",                          "count": 25},
    {"slug": "russia",                   "name": "Russia",                           "count": 33},
    {"slug": "rwanda",                   "name": "Rwanda",                           "count": 1},
    {"slug": "san-marino",               "name": "San Marino",                       "count": 14},
    {"slug": "sao-tome-and-principe",    "name": "Sao Tome and Principe",            "count": 1},
    {"slug": "saudi-arabia",             "name": "Saudi Arabia",                     "count": 33},
    {"slug": "scotland",                 "name": "Scotland",                         "count": 65},
    {"slug": "senegal",                  "name": "Senegal",                          "count": 1},
    {"slug": "serbia",                   "name": "Serbia",                           "count": 23},
    {"slug": "seychelles",               "name": "Seychelles",                       "count": 1},
    {"slug": "sierra-leone",             "name": "Sierra Leone",                     "count": 1},
    {"slug": "singapore",                "name": "Singapore",                        "count": 1},
    {"slug": "slovakia",                 "name": "Slovakia",                         "count": 20},
    {"slug": "slovenia",                 "name": "Slovenia",                         "count": 17},
    {"slug": "somalia",                  "name": "Somalia",                          "count": 1},
    {"slug": "south-africa",             "name": "South Africa",                     "count": 18},
    {"slug": "south-korea",              "name": "South Korea",                      "count": 16},
    {"slug": "south-sudan",              "name": "South Sudan",                      "count": 1},
    {"slug": "spain",                    "name": "Spain",                            "count": 116},
    {"slug": "sudan",                    "name": "Sudan",                            "count": 1},
    {"slug": "suriname",                 "name": "Suriname",                         "count": 1},
    {"slug": "sweden",                   "name": "Sweden",                           "count": 30},
    {"slug": "switzerland",              "name": "Switzerland",                      "count": 18},
    {"slug": "syria",                    "name": "Syria",                            "count": 1},
    {"slug": "tanzania",                 "name": "Tanzania",                         "count": 1},
    {"slug": "thailand",                 "name": "Thailand",                         "count": 7},
    {"slug": "togo",                     "name": "Togo",                             "count": 1},
    {"slug": "trinidad-and-tobago",      "name": "Trinidad and Tobago",              "count": 1},
    {"slug": "tunisia",                  "name": "Tunisia",                          "count": 16},
    {"slug": "turkey",                   "name": "Turkey",                           "count": 48},
    {"slug": "uae",                      "name": "United Arab Emirates",             "count": 1},
    {"slug": "uganda",                   "name": "Uganda",                           "count": 1},
    {"slug": "ukraine",                  "name": "Ukraine",                          "count": 23},
    {"slug": "uruguay",                  "name": "Uruguay",                          "count": 17},
    {"slug": "usa",                      "name": "USA",                              "count": 55},
    {"slug": "uzbekistan",               "name": "Uzbekistan",                       "count": 17},
    {"slug": "venezuela",                "name": "Venezuela",                        "count": 16},
    {"slug": "vietnam",                  "name": "Vietnam",                          "count": 17},
    {"slug": "wales",                    "name": "Wales",                            "count": 42},
    {"slug": "zambia",                   "name": "Zambia",                           "count": 1},
    {"slug": "zimbabwe",                 "name": "Zimbabwe",                         "count": 1},
]


# ══════════════════════════════════════════════════════════════════
#  SHARED INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════

def _build_session() -> requests.Session:
    """Pooled session with automatic retry on transient errors."""
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=0.6,
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods={"GET"},
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=64)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LeoBook-FootballLogos/4.0)",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return session

_SESSION = _build_session()   # shared for league ZIPs


def _dir_stats(directory: Path) -> Tuple[int, float]:
    if not directory.exists():
        return 0, 0.0
    files = [f for f in directory.rglob("*") if f.is_file()]
    size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
    return len(files), round(size_mb, 1)


def _safe_filename(name: str, suffix: str) -> str:
    """Convert a team name + extension into a safe filesystem name."""
    safe = "".join(c if c.isalnum() or c in " -_" else "_" for c in name).strip()
    return (safe[:120] or "logo") + suffix.lower()


def _normalise_url(src: str) -> str:
    """Ensure a src attribute is a full absolute URL."""
    if src.startswith("//"):
        return "https:" + src
    if src.startswith("/"):
        return SITE_BASE + src
    return src


def _is_team_logo(src: str) -> bool:
    """
    Filter heuristic: keep only URLs that are plausible team logo images.
    Rejects country-thumbnail OG images and non-image assets.
    """
    low = src.lower()
    if "/og/" in low:       # e.g. /og/england-700x700.png — country thumbnails
        return False
    return low.endswith(".png") or low.endswith(".svg")


# ══════════════════════════════════════════════════════════════════
#  LEAGUE ZIP DOWNLOADER  (unchanged from v3)
# ══════════════════════════════════════════════════════════════════

def _cdn_urls(slug: str):
    if slug in CDN_FIXED_SLUGS:
        yield f"{CDN_BASE}{slug}.football-logos.cc.zip"
    else:
        for suffix in CDN_SUFFIXES:
            yield f"{CDN_BASE}{slug}{suffix}"


def _download_league_zip(slug: str, force: bool = False) -> dict:
    league_dir = OUTPUT_DIR / slug.replace("-", "_")
    if not force and league_dir.exists() and any(league_dir.iterdir()):
        return {"slug": slug, "status": "skipped"}

    for url in _cdn_urls(slug):
        try:
            r = _SESSION.get(url, stream=True, timeout=30)
            r.raise_for_status()
            league_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                z.extractall(league_dir)
            files, size_mb = _dir_stats(league_dir)
            return {"slug": slug, "status": "ok", "files": files, "size_mb": size_mb}
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                continue
            return {"slug": slug, "status": "error", "reason": str(e)}
        except Exception as e:
            return {"slug": slug, "status": "error", "reason": str(e)}

    return {"slug": slug, "status": "not_found", "reason": "404 — all URL variants failed"}


def _upload_to_supabase(
    local_path: Path, 
    remote_subdir: str, 
    storage_mgr: Optional[StorageManager] = None,
    linker: Optional[MetadataLinker] = None,
    league_slug: Optional[str] = None,
    is_league_logo: bool = False
) -> Optional[str]:
    """Helper to upload a file to Supabase, log URL, and optionally link to database."""
    if not SUPABASE_AVAILABLE:
        return None
    
    mgr = storage_mgr or StorageManager(bucket_name="logos")
    remote_path = f"{remote_subdir}/{local_path.name}"
    
    try:
        url = mgr.upload_file(local_path, remote_path)
        if url:
            logger.info(f"  [↑] Joined Supabase: {url}")
            if linker:
                if is_league_logo and league_slug:
                    linker.update_league_logo(league_slug, url)
                elif league_slug:
                    # team logo
                    team_name = local_path.stem
                    linker.update_team_logo(team_name, league_slug, url)
        return url
    except Exception as e:
        logger.error(f"  [!] Supabase upload failed for {local_path.name}: {e}")
        return None


def download_all_logos(limit: Optional[int] = None, max_workers: int = 4, force: bool = False, upload_supabase: bool = False):
    slugs = LEAGUE_SLUGS[:limit] if limit else LEAGUE_SLUGS
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"🌍 Football logos — {len(slugs)} leagues")

    to_download, already_present = [], []
    for slug in slugs:
        league_dir = OUTPUT_DIR / slug.replace("-", "_")
        if not force and league_dir.exists() and any(league_dir.iterdir()):
            already_present.append(slug)
            logger.info(f"  [=] {slug}: already exists")
        else:
            to_download.append(slug)

    if not to_download:
        total_files, total_size = _dir_stats(OUTPUT_DIR)
        logger.info(f"✅ All {len(slugs)} leagues already present")
        logger.info(f"📊 Total: {total_files:,} files • {total_size:.1f} MB")
        return

    logger.info(f"📥 Downloading {len(to_download)} missing league packs...")
    counts: dict = {"ok": 0, "not_found": 0, "error": 0}
    
    storage_mgr = None
    linker = None
    if upload_supabase and SUPABASE_AVAILABLE:
        storage_mgr = StorageManager(bucket_name="logos")
        linker = MetadataLinker(PROJECT_ROOT)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_download_league_zip, s, force): s for s in to_download}
        for future in as_completed(futures):
            r = future.result()
            status = r["status"]
            counts[status] = counts.get(status, 0) + 1
            if status == "ok":
                logger.info(f"  [+] {r['slug']}: {r['files']} files ({r['size_mb']} MB)")
                if upload_supabase:
                    logger.info(f"  [↑] {r['slug']}: Uploading to Supabase Storage...")
                    league_dir = OUTPUT_DIR / r['slug'].replace("-", "_")
                    for img in league_dir.glob("*.*"):
                        _upload_to_supabase(
                            img, 
                            f"leagues/{r['slug']}", 
                            storage_mgr=storage_mgr, 
                            linker=linker,
                            league_slug=r['slug']
                        )
            elif status == "not_found":
                logger.warning(f"  [!] {r['slug']}: {r['reason']}")
            elif status == "error":
                logger.error(f"  [x] {r['slug']}: {r['reason']}")

    if linker:
        linker.save()

    logger.info(
        f"🏁 Done — {counts['ok']} new • {len(already_present)} skipped • "
        f"{counts.get('not_found', 0)} missing • {counts.get('error', 0)} errors"
    )


# ══════════════════════════════════════════════════════════════════
#  COUNTRY LOGO SCRAPER
# ══════════════════════════════════════════════════════════════════

# ── Playwright lazy pool ───────────────────────────────────────
# One shared browser, launched only when BS4 finds nothing on a page.
# Thread-safe: each call opens its own tab, all share the same process.

class _PlaywrightPool:
    """Thread-safe, lazily-launched shared Playwright browser."""

    def __init__(self):
        self._lock    = threading.Lock()
        self._pw      = None
        self._browser = None

    def _ensure_started(self):
        """Start Playwright + browser if not already running (call inside lock)."""
        if self._browser is None:
            if not PLAYWRIGHT_AVAILABLE:
                raise RuntimeError(
                    "Playwright not installed. Run: pip install playwright && playwright install chromium"
                )
            self._pw      = sync_playwright().start()
            self._browser = self._pw.chromium.launch(headless=True)
            logger.info("  [PW] Playwright browser launched (fallback mode)")

    def scrape_logo_urls(self, page_url: str) -> list[Tuple[str, str]]:
        """
        Open page_url in a new tab, collect (src, alt) for all .png/.svg images,
        close the tab, return results.  Thread-safe.
        """
        with self._lock:
            self._ensure_started()
            # new_page() itself is thread-safe on a live Chromium instance
            browser = self._browser

        page = browser.new_page()
        try:
            page.goto(page_url, timeout=30_000)
            page.wait_for_load_state("networkidle", timeout=15_000)

            results: list[Tuple[str, str]] = []
            for img in page.locator("img").all():
                src = img.get_attribute("src") or img.get_attribute("data-src") or ""
                if not src or not src.lower().endswith((".png", ".svg")):
                    continue
                alt = (img.get_attribute("alt") or "").strip()
                if not alt:
                    try:
                        alt = img.evaluate("el => el.parentElement.textContent.trim()")[:100]
                    except Exception:
                        alt = ""
                results.append((_normalise_url(src), alt))
            return results
        finally:
            page.close()

    def close(self):
        with self._lock:
            if self._browser:
                self._browser.close()
                self._pw.stop()
                self._browser = None
                self._pw      = None

_PW_POOL = _PlaywrightPool()


# ── Primary scraper: requests + BeautifulSoup ─────────────────

def _scrape_via_requests(url: str, session: requests.Session) -> list[Tuple[str, str]]:
    """
    Fetch a country page with plain HTTP and parse with BeautifulSoup.
    Returns [(absolute_src, alt_text), ...] for every .png/.svg <img> found.
    Falls back to an empty list on any error.
    """
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        logger.debug(f"    [BS4 fetch fail] {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results: list[Tuple[str, str]] = []
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if not src or not src.lower().endswith((".png", ".svg")):
            continue
        alt = (img.get("alt") or "").strip()
        if not alt:
            parent = img.parent
            alt    = parent.get_text(strip=True)[:100] if parent else ""
        results.append((_normalise_url(src), alt))
    return results


# ── Per-country pipeline ───────────────────────────────────────

def _download_country(
    country:       dict,
    session:       requests.Session,
    force:         bool = False,
    image_workers: int  = 8,
    upload_supabase: bool = False,
    storage_mgr: Optional[StorageManager] = None,
    linker: Optional[MetadataLinker] = None,
) -> dict:
    """
    Full download pipeline for one country:
      1. Try requests + BS4 to collect logo URLs
      2. If nothing found, fall back to Playwright
      3. Download all logos in parallel with image_workers threads
    """
    slug        = country["slug"]
    name        = country["name"]
    country_dir = COUNTRIES_OUT_DIR / slug
    country_url = f"{SITE_BASE}/{slug}/"

    if not force and country_dir.exists() and any(country_dir.iterdir()):
        return {"name": name, "slug": slug, "status": "skipped"}

    # ── 1. Collect logo URLs ───────────────────────────────────
    raw = _scrape_via_requests(country_url, session)
    logo_pairs = [(src, alt) for src, alt in raw if _is_team_logo(src)]

    # ── 2. Playwright fallback ─────────────────────────────────
    if not logo_pairs:
        logger.debug(f"  [~] {name}: BS4 found 0 team logos — trying Playwright")
        try:
            raw        = _PW_POOL.scrape_logo_urls(country_url)
            logo_pairs = [(src, alt) for src, alt in raw if _is_team_logo(src)]
        except Exception as e:
            return {"name": name, "slug": slug, "status": "error", "reason": f"Playwright: {e}"}

    if not logo_pairs:
        return {"name": name, "slug": slug, "status": "error", "reason": "no logo URLs found"}

    # Deduplicate by URL, keep first-seen alt text
    deduped: dict[str, str] = {}
    for src, alt in logo_pairs:
        if src not in deduped:
            deduped[src] = alt
    logo_pairs = list(deduped.items())

    # ── 3. Parallel image download ─────────────────────────────
    country_dir.mkdir(parents=True, exist_ok=True)

    def _fetch_one(item: Tuple[str, str]) -> bool:
        src, alt = item
        # Strip query strings before deriving the extension
        clean_path = src.split("?")[0]
        suffix     = Path(clean_path).suffix.lower() or ".png"
        filename   = _safe_filename(alt or Path(clean_path).stem, suffix)
        dest       = country_dir / filename
        if not force and dest.exists():
            return True
        try:
            r = session.get(src, timeout=15)
            r.raise_for_status()
            dest.write_bytes(r.content)
            return True
        except Exception:
            return False

    with ThreadPoolExecutor(max_workers=image_workers) as pool:
        ok = sum(1 for success in pool.map(_fetch_one, logo_pairs) if success)

    if ok == 0:
        return {"name": name, "slug": slug, "status": "error", "reason": "all image downloads failed"}

    files, size_mb = _dir_stats(country_dir)
    if upload_supabase:
        logger.info(f"  [↑] {name}: Uploading {ok} logos to Supabase...")
        for img in country_dir.glob("*.*"):
            _upload_to_supabase(
                img, 
                f"countries/{slug}", 
                storage_mgr=storage_mgr, 
                linker=linker,
                league_slug=slug
            )

    return {
        "name": name, "slug": slug, "status": "ok",
        "files": files, "size_mb": size_mb, "downloaded": ok,
    }


# ── Countries orchestrator ─────────────────────────────────────

def download_all_countries(
    limit:       Optional[int] = None,
    max_workers: int           = 5,
    force:       bool          = False,
    upload_supabase: bool      = False,
):
    """
    Download team logos for all 160 countries.

    Primary strategy: requests + BeautifulSoup (zero browser overhead).
    Fallback:         shared Playwright Chromium, launched lazily only if BS4
                      finds nothing on a given page.
    """
    if not BS4_AVAILABLE:
        logger.error("❌ BeautifulSoup4 not installed.  pip install beautifulsoup4")
        return

    countries = COUNTRIES[:limit] if limit else COUNTRIES
    total     = len(countries)
    COUNTRIES_OUT_DIR.mkdir(parents=True, exist_ok=True)
    session = _build_session()   # dedicated session (separate pool from league ZIPs)

    logger.info(f"🌍 Countries mode — {total} countries, {max_workers} workers")
    logger.info(f"   Strategy: requests+BS4 primary {'| Playwright fallback' if PLAYWRIGHT_AVAILABLE else '(install Playwright for JS fallback)'}")

    counts: dict = {"ok": 0, "skipped": 0, "error": 0}

    # tqdm progress bar (optional, degrades gracefully)
    progress = tqdm(total=total, unit="country", ncols=90, leave=True) if TQDM_AVAILABLE else None

    def _log(msg: str):
        if progress:
            progress.write(msg)
        else:
            logger.info(msg)

    storage_mgr = None
    linker = None
    if upload_supabase and SUPABASE_AVAILABLE:
        storage_mgr = StorageManager(bucket_name="logos")
        linker = MetadataLinker(PROJECT_ROOT)

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_download_country, c, session, force, 8, upload_supabase, storage_mgr, linker): c["name"]
                for c in countries
            }
            for future in as_completed(futures):
                r      = future.result()
                status = r["status"]
                counts[status] = counts.get(status, 0) + 1

                if status == "ok":
                    _log(f"  [+] {r['name']}: {r['files']} logos ({r['size_mb']} MB)")
                elif status == "skipped":
                    _log(f"  [=] {r['name']}: already exists")
                else:
                    _log(f"  [!] {r['name']}: {r.get('reason', '?')}")

                if progress:
                    progress.update(1)

        if linker:
            linker.save()

    finally:
        if progress:
            progress.close()
        _PW_POOL.close()

    total_files, total_size = _dir_stats(COUNTRIES_OUT_DIR)
    logger.info(
        f"🏁 Done — {counts['ok']} processed • {counts['skipped']} skipped • {counts['error']} errors"
    )
    logger.info(f"📊 Total: {total_files:,} files • {total_size:.1f} MB")


# ══════════════════════════════════════════════════════════════════
#  UTILITY: refresh the hardcoded COUNTRIES list when the site updates
# ══════════════════════════════════════════════════════════════════

def _refresh_country_list():
    """
    Re-scrapes football-logos.cc/countries/ and prints an updated COUNTRIES
    block to stdout. Run manually when the site adds new countries:

        python -c "from football_logos import _refresh_country_list; _refresh_country_list()"
    """
    import re
    if not BS4_AVAILABLE:
        print("pip install beautifulsoup4 first")
        return

    session = _build_session()
    r = session.get(f"{SITE_BASE}/countries/", timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    seen:    set  = set()
    entries: list = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        m    = re.search(r"(\d+)\s*$", text)
        if not (href.startswith("/") and href.endswith("/") and m):
            continue
        slug = href.strip("/")
        skip = {"", "countries", "collections", "all", "tournaments", "map", "new"}
        if slug in skip or slug in seen:
            continue
        seen.add(slug)
        # Strip emoji + whitespace from name
        name_raw = text[: m.start()].strip()
        name     = re.sub(r"[^\x00-\x7F]+", "", name_raw).strip()
        entries.append((slug, name or slug, int(m.group(1))))

    entries.sort(key=lambda x: x[1])
    print(f"# {len(entries)} countries — scraped {SITE_BASE}/countries/")
    print("COUNTRIES: list[dict] = [")
    for slug, name, count in entries:
        print(f'    {{"slug": "{slug:<40}", "name": "{name:<42}", "count": {count}}},')
    print("]")


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Professional Football Logos Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python football_logos.py                        # download 31 league ZIP packs
  python football_logos.py --countries            # download all 160 countries
  python football_logos.py --countries --limit 5  # test with 5 countries
  python football_logos.py --force                # re-download everything
  python football_logos.py --supabase             # upload to Supabase
        """,
    )
    parser.add_argument("--limit",     type=int,            help="Process only the first N leagues/countries")
    parser.add_argument("--workers",   type=int, default=5, help="Concurrent country workers (default: 5)")
    parser.add_argument("--countries", action="store_true", help="Download team logos for all 160 countries")
    parser.add_argument("--force",     action="store_true", help="Re-download even if files already exist")
    parser.add_argument("--supabase",  action="store_true", help="Upload logos to Supabase Storage")
    args = parser.parse_args()

    if args.supabase and not SUPABASE_AVAILABLE:
        logger.error("❌ Supabase dependencies not found. Ensure Data/Access/storage_manager.py exists.")
        exit(1)

    if args.countries:
        download_all_countries(limit=args.limit, max_workers=args.workers, force=args.force, upload_supabase=args.supabase)
    else:
        download_all_logos(limit=args.limit, max_workers=args.workers, force=args.force, upload_supabase=args.supabase)