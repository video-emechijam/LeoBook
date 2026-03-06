# enrich_leagues.py: Extract Flashscore league pages -> SQLite database.
# Part of LeoBook Scripts — Data Collection
#
# Enrichment Modes:
#   (default)  Smart gap scan — only leagues with missing data
#   --refresh  Re-process stale leagues (>7 days old)
#   --reset    Full reset — re-enrich ALL leagues from scratch
#
# Season targeting:
#   --season N   Specific season by offset (0=current, 1=2024/2025, 2=2023/2024...)
#   --seasons N  Last N past seasons (e.g. 5 = 2021-2025)
#   --all-seasons  All available seasons
#
# Usage:
#   python -m Scripts.enrich_leagues                     # Gap scan (default)
#   python -m Scripts.enrich_leagues --limit 5           # First 5 with gaps
#   python -m Scripts.enrich_leagues --limit 501-1000    # Range-based
#   python -m Scripts.enrich_leagues --season 0          # Current season only
#   python -m Scripts.enrich_leagues --season 1          # Most recent past season
#   python -m Scripts.enrich_leagues --seasons 5         # Last 5 past seasons
#   python -m Scripts.enrich_leagues --refresh           # Stale leagues only
#   python -m Scripts.enrich_leagues --reset             # Full reset
#
# Features:
#   - Workload announced at start
#   - Cloud sync at every 20% checkpoint
#   - All CSS selectors loaded from Config/knowledge.json via SelectorManager

import asyncio
import argparse
import json
import os
import re
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from typing import List, Dict, Any, Optional

import requests
from playwright.async_api import async_playwright, Page

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from Core.Utils.constants import now_ng
from Core.Intelligence.aigo_suite import AIGOSuite
from Core.Intelligence.selector_manager import SelectorManager
from Data.Access.league_db import (
    init_db, get_connection, upsert_league, upsert_team, upsert_fixture,
    bulk_upsert_fixtures, mark_league_processed, get_unprocessed_leagues,
    get_leagues_with_gaps, get_stale_leagues,
    get_league_db_id, get_team_id,
)
from Core.Browser.site_helpers import fs_universal_popup_dismissal

# ── Selectors (Unified Knowledge Base) ───────────────────────────────────────
selector_mgr = SelectorManager()
CONTEXT_LEAGUE = "fs_league_page"

# ── Paths (RELATIVE from project root) ───────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEAGUES_JSON = os.path.join(BASE_DIR, "Data", "Store", "leagues.json")
CRESTS_DIR = os.path.join("Data", "Store", "crests")
LEAGUE_CRESTS_DIR = os.path.join(CRESTS_DIR, "leagues")
TEAM_CRESTS_DIR = os.path.join(CRESTS_DIR, "teams")

# ── Config ───────────────────────────────────────────────────────────────────
MAX_CONCURRENCY = 3          # Parallel browser tabs
MAX_SHOW_MORE = 50           # Exhaustive "Show more" clicks
DOWNLOAD_WORKERS = 8         # ThreadPool workers for image downloads
REQUEST_TIMEOUT = 15         # Seconds for image download timeout

# ── Globals ──────────────────────────────────────────────────────────────────
executor = ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS)


# ═══════════════════════════════════════════════════════════════════════════════
#  Image Download (runs in ThreadPoolExecutor)
# ═══════════════════════════════════════════════════════════════════════════════

def _download_image(url: str, dest_path: str) -> str:
    """Download an image to disk. Returns the local path or empty string on failure."""
    if not url or url.startswith("data:"):
        return ""
    # Resolve relative path from BASE_DIR for actual disk I/O
    abs_dest = os.path.join(BASE_DIR, dest_path) if not os.path.isabs(dest_path) else dest_path
    if os.path.exists(abs_dest):
        return dest_path  # Return the relative path
    try:
        os.makedirs(os.path.dirname(abs_dest), exist_ok=True)
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Referer": "https://www.flashscore.com/",
        })
        if resp.status_code == 200 and len(resp.content) > 100:
            with open(abs_dest, "wb") as f:
                f.write(resp.content)
            return dest_path  # Return the relative path
    except Exception:
        pass
    return ""


def _slugify(name: str) -> str:
    """Convert a name to a filesystem-safe slug."""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "_", s)
    return s.strip("_")


def schedule_image_download(url: str, dest_path: str) -> "Future":
    """Submit an image download to the thread pool. Returns a Future."""
    return executor.submit(_download_image, url, dest_path)


# ── Supabase storage upload helper ────────────────────────────────────────────
_supabase_storage = None
_supabase_url = ""
_uploaded_crests = set()  # Dedup: track {bucket/remote_name} already uploaded this session

def _init_supabase_storage():
    """Initialize Supabase storage client (once). Auto-creates buckets."""
    global _supabase_storage, _supabase_url
    if _supabase_storage is not None:
        return _supabase_storage, _supabase_url
    try:
        from Data.Access.supabase_client import get_supabase_client
        client = get_supabase_client()
        if client:
            _supabase_storage = client.storage
            _supabase_url = os.getenv("SUPABASE_URL", "").rstrip("/")
            # Auto-create buckets if they don't exist
            try:
                existing = [b.name for b in _supabase_storage.list_buckets()]
                for bucket in ("league-crests", "team-crests"):
                    if bucket not in existing:
                        _supabase_storage.create_bucket(bucket, options={"public": True})
                        print(f"  [Supabase] Created bucket: {bucket}")
            except Exception as e:
                print(f"  [Supabase] Bucket check failed: {e}")
            return _supabase_storage, _supabase_url
    except Exception:
        pass
    _supabase_storage = False  # Mark as attempted but unavailable
    return None, ""


def upload_crest_to_supabase(local_path: str, bucket: str, remote_name: str) -> str:
    """Upload a local crest file to Supabase storage. Returns public URL or ''.
    Deduplicates: skips if same bucket/filename already uploaded this session.
    """
    key = f"{bucket}/{remote_name}"
    if key in _uploaded_crests:
        # Already uploaded this session — just return the URL
        storage, sb_url = _init_supabase_storage()
        if sb_url:
            return f"{sb_url}/storage/v1/object/public/{key}"
        return ""

    storage, sb_url = _init_supabase_storage()
    if not storage or not sb_url:
        return ""  # Supabase not available, fallback to local path

    abs_path = os.path.join(BASE_DIR, local_path) if not os.path.isabs(local_path) else local_path
    if not os.path.exists(abs_path):
        return ""

    try:
        with open(abs_path, 'rb') as f:
            storage.from_(bucket).upload(
                path=remote_name,
                file=f,
                file_options={"cache-control": "3600", "upsert": "true"}
            )
        _uploaded_crests.add(key)
        public_url = f"{sb_url}/storage/v1/object/public/{bucket}/{remote_name}"
        return public_url
    except Exception as e:
        # Silently fail — local path is still valid
        return ""


# ═══════════════════════════════════════════════════════════════════════════════
#  Step 1: Seed leagues from JSON
# ═══════════════════════════════════════════════════════════════════════════════

def seed_leagues_from_json(conn):
    """Read leagues.json and INSERT all leagues into the SQLite leagues table."""
    print(f"\n  [Seed] Reading {LEAGUES_JSON}...")
    with open(LEAGUES_JSON, "r", encoding="utf-8") as f:
        leagues = json.load(f)

    count = 0
    for lg in leagues:
        upsert_league(conn, {
            "league_id": lg["league_id"],
            "country_code": lg.get("country_code"),
            "continent": lg.get("continent"),
            "name": lg["name"],
            "url": lg.get("url"),
        })
        count += 1

    print(f"  [Seed] [OK] Upserted {count} leagues into database.")


# ═══════════════════════════════════════════════════════════════════════════════
#  JS Extraction Scripts
# ═══════════════════════════════════════════════════════════════════════════════

# ── JS to extract all match data with smart year detection + team IDs ────────
# seasonContext is passed from Python: {startYear, endYear, isSplitSeason, tab, selectors}
EXTRACT_MATCHES_JS = r"""(ctx) => {
    const matches = [];
    const s = ctx.selectors;

    // Season-aware year inference
    const startYear = ctx.startYear || new Date().getFullYear();
    const endYear = ctx.endYear || startYear;
    const isSplitSeason = ctx.isSplitSeason || false;
    const tab = ctx.tab || 'results';
    const today = new Date();

    function inferYear(day, month) {
        if (!isSplitSeason) return startYear;
        if (month >= 7) return startYear;
        return endYear;
    }

    // Walk ALL sibling elements in the event list
    const container = document.querySelector(s.main_container)?.parentElement || document.body;
    const allEls = container.querySelectorAll(`${s.match_round}, ${s.match_row}`);
    let currentRound = '';

    allEls.forEach(el => {
        if (el.matches(s.match_round)) {
            currentRound = el.innerText.trim();
            return;
        }
        const rowId = el.getAttribute('id') || '';
        if (!rowId || !rowId.startsWith('g_1_')) return;

        const row = el;
        const fixtureId = rowId.replace('g_1_', '');

        // ── Time + Date ──
        const timeEl = row.querySelector(s.match_time);
        let matchTime = '';
        let matchDate = '';
        let extraTag = '';

        if (timeEl) {
            const stageInTime = timeEl.querySelector(`${s.match_stage_block}, ${s.match_stage_pkv}, ${s.match_stage}`);
            if (stageInTime) {
                extraTag = stageInTime.innerText.trim();
            }

            let raw = '';
            for (const node of timeEl.childNodes) {
                if (node.nodeType === 3) raw += node.textContent;
                else if (node.classList && node.classList.contains('lineThrough')) raw += node.textContent;
            }
            raw = raw.trim();
            if (!raw) raw = timeEl.innerText.trim().replace(/FRO|Postp\.?|Canc\.?|Abn\.?/gi, '').trim();

            const fullMatch = raw.match(/(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})/);
            if (fullMatch) {
                matchDate = `${fullMatch[3]}-${fullMatch[2]}-${fullMatch[1]}`;
                matchTime = `${fullMatch[4]}:${fullMatch[5]}`;
            } else {
                const shortMatch = raw.match(/(\d{2})\.(\d{2})\.\s*(\d{2}):(\d{2})/);
                if (shortMatch) {
                    const day = parseInt(shortMatch[1]);
                    const month = parseInt(shortMatch[2]);
                    const year = inferYear(day, month);
                    matchDate = `${year}-${shortMatch[2]}-${shortMatch[1]}`;
                    matchTime = `${shortMatch[3]}:${shortMatch[4]}`;
                } else {
                    const justTime = raw.match(/(\d{2}):(\d{2})/);
                    if (justTime) matchTime = `${justTime[1]}:${justTime[2]}`;
                }
            }
        }

        // ── Home & Away teams ──
        const homeEl = row.querySelector(s.home_participant);
        const homeName = homeEl ?
            (homeEl.querySelector(s.participant_name) || homeEl)
                .innerText.trim().replace(/\s*\(.*?\)\s*$/, '') : '';
        const awayEl = row.querySelector(s.away_participant);
        const awayName = awayEl ?
            (awayEl.querySelector(s.participant_name) || awayEl)
                .innerText.trim().replace(/\s*\(.*?\)\s*$/, '') : '';

        // ── Scores ──
        const homeScoreEl = row.querySelector(s.match_score_home);
        const awayScoreEl = row.querySelector(s.match_score_away);
        const homeScoreText = homeScoreEl ? homeScoreEl.innerText.trim() : '';
        const awayScoreText = awayScoreEl ? awayScoreEl.innerText.trim() : '';
        const homeScore = homeScoreText && homeScoreText !== '-' ? parseInt(homeScoreText) : null;
        const awayScore = awayScoreText && awayScoreText !== '-' ? parseInt(awayScoreText) : null;

        // ── Match status ──
        let matchStatus = '';
        const stageEl = row.querySelector(`${s.match_stage_block}, ${s.match_stage}`);
        if (stageEl && !stageEl.closest(s.match_time)) {
            matchStatus = stageEl.innerText.trim();
        } else if (homeScoreEl) {
            const state = homeScoreEl.getAttribute('data-state') || '';
            const isFinal = homeScoreEl.className.includes('isFinal') ||
                            homeScoreEl.className.includes('Final');
            if (state === 'final' || isFinal) matchStatus = 'FT';
            else if (homeScore !== null) matchStatus = 'FT';
        }

        // ── Team crests ──
        const homeImg = row.querySelector(s.match_logo_home);
        const awayImg = row.querySelector(s.match_logo_away);
        const homeCrest = homeImg ? (homeImg.src || homeImg.getAttribute('data-src') || '') : '';
        const awayCrest = awayImg ? (awayImg.src || awayImg.getAttribute('data-src') || '') : '';

        // ── Team ID + URL from match link ──
        let homeTeamId = '', awayTeamId = '', homeTeamUrl = '', awayTeamUrl = '';
        // eventRowLink is a SIBLING <a> linked via aria-describedby, NOT a child/parent
        // DOM: <a class="eventRowLink" aria-describedby="g_1_XXXXX" href="..."></a>
        let linkEl = row.querySelector(s.match_link);
        if (!linkEl) linkEl = document.querySelector(`a[aria-describedby="${rowId}"]`);
        const mLink = linkEl ? linkEl.getAttribute('href') : '';
        if (mLink && mLink.includes('/match/football/')) {
            const cleanPath = mLink.replace(/^(.*\/match\/football\/)/, '');
            const parts = cleanPath.split('/').filter(p => p && !p.startsWith('?'));
            if (parts.length >= 2) {
                const hSeg = parts[0]; const aSeg = parts[1];
                homeTeamId = hSeg.substring(hSeg.lastIndexOf('-') + 1);
                awayTeamId = aSeg.substring(aSeg.lastIndexOf('-') + 1);
                const hSlug = hSeg.substring(0, hSeg.lastIndexOf('-'));
                const aSlug = aSeg.substring(0, aSeg.lastIndexOf('-'));
                if (hSlug && homeTeamId) homeTeamUrl = `https://www.flashscore.com/team/${hSlug}/${homeTeamId}/`;
                if (aSlug && awayTeamId) awayTeamUrl = `https://www.flashscore.com/team/${aSlug}/${awayTeamId}/`;
            }
        }

        matches.push({
            fixture_id: fixtureId,
            date: matchDate,
            time: matchTime,
            home_team_name: homeName,
            away_team_name: awayName,
            home_team_id: homeTeamId,
            away_team_id: awayTeamId,
            home_team_url: homeTeamUrl,
            away_team_url: awayTeamUrl,
            home_score: homeScore,
            away_score: awayScore,
            match_status: matchStatus,
            home_crest_url: homeCrest,
            away_crest_url: awayCrest,
            league_stage: currentRound,
            extra: extraTag || null,
            url: `/match/${fixtureId}/#/match-summary`,
            match_link: mLink || ''
        });
    });

    return matches;
}"""

# ── JS to extract season text ───────────────────────────────────────────────
EXTRACT_SEASON_JS = r"""(selectors) => {
    const s = selectors;
    const possible = s.season_info.split(',').map(x => x.trim());
    for (const sel of possible) {
        const el = document.querySelector(sel);
        if (el) {
            const text = el.innerText.trim();
            const match = text.match(/(\d{4}(?:\/\d{4})?)/);
            if (match) return match[1];
        }
    }
    const breadcrumbs = document.querySelectorAll(s.breadcrumb_text);
    for (const b of breadcrumbs) {
        const match = b.innerText.match(/(\d{4}(?:\/\d{4})?)/);
        if (match) return match[1];
    }
    return '';
}"""

# ── JS to extract league crest URL ──────────────────────────────────────────
EXTRACT_CREST_JS = r"""(selectors) => {
    const img = document.querySelector(selectors.league_crest);
    return img ? (img.src || img.getAttribute('data-src') || '') : '';
}"""

# ── JS to extract fs_league_id from Flashscore's internal config ──────────
EXTRACT_FS_LEAGUE_ID_JS = r"""() => {
    // 1. Direct access to Flashscore's internal data object (Most Stable)
    if (window.leaguePageHeaderData && window.leaguePageHeaderData.tournamentStageId) {
        return window.leaguePageHeaderData.tournamentStageId;
    }
    if (window.tournament_id) return window.tournament_id;
    if (window.config && window.config.tournamentStage) return window.config.tournamentStage;

    // 2. Fallback: Parse from URL path (matches the current structure)
    const path = window.location.pathname || '';
    const pathMatch = path.match(/-([A-Za-z0-9]{6,10})\/?$/);
    if (pathMatch) return pathMatch[1];

    // 3. Last Resort: Check navigation links
    const navLinks = document.querySelectorAll('a[href*="/standings/"], a[href*="/results/"]');
    for (const link of navLinks) {
        const href = link.getAttribute('href') || '';
        const m = href.match(/\/([A-Za-z0-9]{6,10})\/standings\//);
        if (m) return m[1];
    }
    
    // 4. Legacy Hash Fallback
    const hash = window.location.hash || '';
    const hashMatch = hash.match(/#\/([A-Za-z0-9]{6,10})\//);
    if (hashMatch) return hashMatch[1];

    return '';
}"""

# ── JS to extract archive season links ──────────────────────────────────────
EXTRACT_ARCHIVE_JS = r"""(selectors) => {
    const s = selectors;
    const seasons = [];
    const seen = new Set();
    
    // Strategy: scan ALL links on the archive page for season URL patterns
    // Selectors from knowledge.json are tried first, then fallback to all <a> tags
    const selectorSources = [
        s.archive_links,
        s.archive_table_links,
        'a[href*="/football/"]',  // Broadest possible catch-all
    ];
    
    for (const sel of selectorSources) {
        if (!sel) continue;
        const links = document.querySelectorAll(sel);
        for (const a of links) {
            const href = a.getAttribute('href') || '';
            
            // Match split season: e.g. premier-league-2023-2024
            const match = href.match(/\/football\/([^/]+)\/([^/]+-(\d{4})-(\d{4}))\/?/);
            if (match && !seen.has(match[2])) {
                seen.add(match[2]);
                seasons.push({
                    slug: match[2],
                    country: match[1],
                    start_year: parseInt(match[3]),
                    end_year: parseInt(match[4]),
                    url: href.startsWith('http') ? href : 'https://www.flashscore.com' + href
                });
            }
            
            // Match calendar year: e.g. ligue-1-2024
            const calMatch = href.match(/\/football\/([^/]+)\/([^/]+-(\d{4}))\/?$/);
            if (calMatch && !seen.has(calMatch[2])) {
                seen.add(calMatch[2]);
                seasons.push({
                    slug: calMatch[2],
                    country: calMatch[1],
                    start_year: parseInt(calMatch[3]),
                    end_year: parseInt(calMatch[3]),
                    url: href.startsWith('http') ? href : 'https://www.flashscore.com' + href
                });
            }
        }
    }
    seasons.sort((a, b) => b.start_year - a.start_year);
    return seasons;
}"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Season Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def parse_season_string(season_str: str) -> dict:
    """Parse a season string like '2024/2025' or '2024' into context."""
    if not season_str:
        year = now_ng().year
        return {"startYear": year, "endYear": year, "isSplitSeason": False}

    # Try split season: 2023/2024
    m = re.match(r"(\d{4})[/\-](\d{4})", season_str)
    if m:
        return {
            "startYear": int(m.group(1)),
            "endYear": int(m.group(2)),
            "isSplitSeason": True,
        }
    # Calendar year: 2024
    m = re.match(r"(\d{4})", season_str)
    if m:
        year = int(m.group(1))
        return {"startYear": year, "endYear": year, "isSplitSeason": False}

    year = datetime.now().year
    return {"startYear": year, "endYear": year, "isSplitSeason": False}


@AIGOSuite.aigo_retry(max_retries=2, delay=3.0)
async def get_archive_seasons(page: Page, league_url: str) -> List[Dict]:
    """Navigate to the archive page and extract all available seasons."""
    archive_url = league_url.rstrip("/") + "/archive/"
    print(f"    [Archive] Navigating to {archive_url}")
    try:
        await page.goto(archive_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)
        await fs_universal_popup_dismissal(page)
        selectors = selector_mgr.get_all_selectors_for_context(CONTEXT_LEAGUE)
        seasons = await page.evaluate(EXTRACT_ARCHIVE_JS, selectors)
        print(f"    [Archive] Found {len(seasons)} historical seasons")
        return seasons or []
    except Exception as e:
        print(f"    [Archive] Failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
#  Core Extraction
# ═══════════════════════════════════════════════════════════════════════════════

@AIGOSuite.aigo_retry(max_retries=2, delay=2.0)
async def _expand_show_more(page: Page, max_clicks: int = MAX_SHOW_MORE):
    """Click 'Show more matches' exhaustively."""
    clicks = 0
    selector = selector_mgr.get_selector(CONTEXT_LEAGUE, "show_more_matches")
    while clicks < max_clicks:
        try:
            btn = page.locator(selector)
            if await btn.count() > 0 and await btn.first.is_visible(timeout=3000):
                await btn.first.click()
                await asyncio.sleep(1.5)
                clicks += 1
            else:
                break
        except Exception:
            break
    if clicks:
        print(f"      [Expand] Clicked 'Show more' {clicks} times")


@AIGOSuite.aigo_retry(max_retries=2, delay=3.0)
async def extract_tab(page: Page, league_url: str, tab: str, conn,
                     league_id: str, season: str, country_code: str,
                     region_league: str = "") -> int:
    """Navigate to a league tab (fixtures or results), expand, extract, and save matches.

    Args:
        league_id: Flashscore league_id string (NOT SQLite auto-increment).
    """
    url = league_url.rstrip("/") + f"/{tab}/"
    print(f"    [{tab.upper()}] Navigating to {url}")

    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)
        await fs_universal_popup_dismissal(page)

        # Detect 404 or redirect (league may not play in this season)
        if resp and resp.status >= 400:
            print(f"    [{tab.upper()}] HTTP {resp.status} — season not available")
            return 0
        # Flashscore redirects invalid seasons to the main league page
        actual_url = page.url.rstrip('/')
        expected_base = url.rstrip('/')
        if actual_url != expected_base and tab not in actual_url:
            print(f"    [{tab.upper()}] Redirected (season not available)")
            return 0
    except Exception as e:
        print(f"    [{tab.upper()}] Navigation failed: {e}")
        return 0

    # Expand all matches
    await _expand_show_more(page)

    # Build season context for smart year detection
    season_ctx = parse_season_string(season)
    season_ctx["tab"] = tab
    season_ctx["selectors"] = selector_mgr.get_all_selectors_for_context(CONTEXT_LEAGUE)

    # Extract match data with season context
    try:
        matches_raw = await page.evaluate(EXTRACT_MATCHES_JS, season_ctx)
    except Exception as e:
        print(f"    [{tab.upper()}] Extraction failed: {e}")
        return 0

    if not matches_raw:
        print(f"    [{tab.upper()}] No matches found")
        return 0

    # Process matches
    fixture_rows = []
    crest_futures = []
    today = date.today()

    for m in matches_raw:
        home_name = m.get("home_team_name", "")
        away_name = m.get("away_team_name", "")
        if not home_name or not away_name:
            continue

        # Use Flashscore team IDs from match link
        home_team_id = m.get("home_team_id", "")
        away_team_id = m.get("away_team_id", "")
        home_team_url = m.get("home_team_url", "")
        away_team_url = m.get("away_team_url", "")

        # Upsert teams with Flashscore team_id and URL
        if home_name:
            team_data = {
                "name": home_name,
                "country_code": country_code,
                "league_ids": [league_id],
            }
            if home_team_id:
                team_data["team_id"] = home_team_id
            if home_team_url:
                team_data["url"] = home_team_url
            upsert_team(conn, team_data)

        if away_name:
            team_data = {
                "name": away_name,
                "country_code": country_code,
                "league_ids": [league_id],
            }
            if away_team_id:
                team_data["team_id"] = away_team_id
            if away_team_url:
                team_data["url"] = away_team_url
            upsert_team(conn, team_data)

        # Schedule team crest downloads (relative paths)
        home_crest_url = m.get("home_crest_url", "")
        away_crest_url = m.get("away_crest_url", "")
        home_crest_path = ""
        away_crest_path = ""

        if home_crest_url and not home_crest_url.startswith("data:"):
            dest = os.path.join(TEAM_CRESTS_DIR, f"{_slugify(home_name)}.png")
            crest_futures.append((schedule_image_download(home_crest_url, dest), "home", home_name, dest))
            home_crest_path = dest

        if away_crest_url and not away_crest_url.startswith("data:"):
            dest = os.path.join(TEAM_CRESTS_DIR, f"{_slugify(away_name)}.png")
            crest_futures.append((schedule_image_download(away_crest_url, dest), "away", away_name, dest))
            away_crest_path = dest

        # Determine match status + extra
        status = m.get("match_status", "")
        extra = m.get("extra")  # FRO, Postp, etc. from JS

        # Status normalization
        if status:
            status_upper = status.upper()
            if "FT" in status_upper or "FINISHED" in status_upper:
                status = "finished"
            elif "AET" in status_upper:
                status = "finished"
                extra = extra or "AET"
            elif "PEN" in status_upper:
                status = "finished"
                extra = extra or "PEN"
            elif "POST" in status_upper:
                status = "postponed"
                extra = extra or "Postp"
            elif "CANC" in status_upper:
                status = "cancelled"
                extra = extra or "Canc"
            elif "ABD" in status_upper or "ABAN" in status_upper:
                status = "abandoned"
                extra = extra or "Abn"
            elif "LIVE" in status_upper or "'" in status:
                status = "live"
            elif "HT" in status_upper:
                status = "halftime"
            elif status == "-":
                status = "scheduled"

        # If match date is in the future -> SCHEDULED
        match_date_str = m.get("date", "")
        if match_date_str and not status:
            try:
                match_dt = datetime.strptime(match_date_str, "%Y-%m-%d").date()
                if match_dt > today:
                    status = "scheduled"
            except ValueError:
                pass
        if not status:
            status = "scheduled" if tab == "fixtures" else "finished"

        # Extra tag normalization
        if extra:
            extra_upper = extra.upper().strip()
            if "FRO" in extra_upper:
                extra = "FRO"
            elif "POSTP" in extra_upper:
                extra = "Postp"
            elif "CANC" in extra_upper:
                extra = "Canc"
            elif "ABN" in extra_upper or "ABAN" in extra_upper:
                extra = "Abn"

        fixture_rows.append({
            "fixture_id": m.get("fixture_id", ""),
            "date": match_date_str,
            "time": m.get("time", ""),
            "league_id": league_id,            # Flashscore league_id string
            "home_team_id": home_team_id,       # Flashscore team_id string
            "home_team_name": home_name,
            "away_team_id": away_team_id,       # Flashscore team_id string
            "away_team_name": away_name,
            "home_score": m.get("home_score"),
            "away_score": m.get("away_score"),
            "extra": extra,
            "league_stage": m.get("league_stage", ""),
            "match_status": status,
            "season": season,
            "home_crest": home_crest_path,
            "away_crest": away_crest_path,
            "url": f"https://www.flashscore.com/match/{m.get('fixture_id', '')}/#/match-summary",
            "region_league": region_league,
            "match_link": m.get("match_link", ""),
        })

    # Bulk insert fixtures
    if fixture_rows:
        bulk_upsert_fixtures(conn, fixture_rows)

    # Wait for crest downloads + upload to Supabase
    downloaded = 0
    for fut, side, name, dest in crest_futures:
        try:
            result = fut.result(timeout=30)
            if result:
                downloaded += 1
                # Upload to Supabase and use public URL
                remote_name = f"{_slugify(name)}.png"
                sb_url = upload_crest_to_supabase(result, "team-crests", remote_name)
                crest_value = sb_url if sb_url else dest  # Supabase URL or local fallback
                conn.execute(
                    "UPDATE teams SET crest = ? WHERE name = ? AND country_code = ?",
                    (crest_value, name, country_code)
                )
        except Exception:
            pass
    if downloaded:
        conn.commit()

    print(f"    [{tab.upper()}] [OK] Saved {len(fixture_rows)} matches, downloaded {downloaded} crests")
    return len(fixture_rows)


async def enrich_single_league(context, league: Dict[str, Any], conn,
                                idx: int, total: int,
                                num_seasons: int = 0, all_seasons: bool = False,
                                target_season: Optional[int] = None):
    """Process a single league: region + crest + season + fixtures + results.

    Args:
        num_seasons: Number of past seasons to extract (0 = current only)
        all_seasons: If True, extract ALL available seasons from archive
        target_season: Season offset (0=current, 1=last past, 2=second past, etc.)
                       When 0 or None, current season runs. When >=1, ONLY that archive season.
    """
    league_id = league["league_id"]
    name = league["name"]
    url = league.get("url", "")
    country_code = league.get("country_code", "")
    continent = league.get("continent", "")

    print(f"\n{'='*60}")
    print(f"  [{idx}/{total}] {name} ({league_id})")
    print(f"  URL: {url}")
    print(f"{'='*60}")

    if not url:
        print(f"  [SKIP] No URL for {name}")
        mark_league_processed(conn, league_id)
        return

    page = await context.new_page()
    try:
        # ── Navigate to league page ──────────────────────────────────────
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(4)
        await fs_universal_popup_dismissal(page)

        # ── Extract fs_league_id from page config ─────────────────────────
        fs_league_id = await page.evaluate(EXTRACT_FS_LEAGUE_ID_JS)
        if fs_league_id:
            print(f"    [FS ID] {fs_league_id}")

        # Retrieve all selectors once for this context
        selectors = selector_mgr.get_all_selectors_for_context(CONTEXT_LEAGUE)

        # ── Extract region from URL (primary) + breadcrumb (fallback) ────
        # URL pattern: /football/{country}/{league-slug}/ — always reliable
        region_name = ""
        region_url_href = ""
        url_parts = url.rstrip('/').split('/')
        # Find 'football' in URL parts, country is the next segment
        try:
            fb_idx = url_parts.index('football')
            if fb_idx + 1 < len(url_parts):
                country_slug = url_parts[fb_idx + 1]
                region_name = country_slug.replace('-', ' ').title()  # e.g. "albania" -> "Albania"
                region_url_href = f"https://www.flashscore.com/football/{country_slug}/"
        except ValueError:
            pass

        # Fallback: try breadcrumb if URL parsing failed
        if not region_name:
            try:
                await page.wait_for_selector(selectors.get('breadcrumb_links', '.breadcrumb__link'), timeout=5000)
            except Exception:
                pass  # Breadcrumbs may not load — URL is our primary source
            region_name = await page.evaluate("""(s) => {
                const links = document.querySelectorAll(s.breadcrumb_links);
                if (links.length >= 2) return links[1].innerText.trim();
                if (links.length >= 1) return links[0].innerText.trim();
                return '';
            }""", selectors)

        # Try to upgrade region_name with breadcrumb text (proper casing/official name)
        breadcrumb_region = await page.evaluate("""(s) => {
            const links = document.querySelectorAll(s.breadcrumb_links);
            if (links.length >= 2) return links[1].innerText.trim();
            return '';
        }""", selectors)
        if breadcrumb_region and breadcrumb_region.upper() != 'FOOTBALL':
            region_name = breadcrumb_region  # Use official Flashscore name
            
        if not region_url_href:
            region_url_href = await page.evaluate("""(s) => {
                const links = document.querySelectorAll(s.breadcrumb_links);
                const el = links.length >= 2 ? links[1] : links[0];
                if (!el) return '';
                const href = el.getAttribute('href') || '';
                return href.startsWith('http') ? href : (href ? 'https://www.flashscore.com' + href : '');
            }""", selectors)

        # Region flag: Flashscore uses CSS sprite classes (e.g. fl_5), not <img> tags
        # We still attempt extraction in case they switch to images in the future
        region_flag_url = await page.evaluate("""(s) => {
            const links = document.querySelectorAll(s.breadcrumb_links);
            const target = links.length >= 2 ? links[1] : links[0];
            if (!target) return '';
            const img = document.querySelector(s.region_flag_img) || target.querySelector('img');
            return img ? (img.src || img.getAttribute('data-src') || '') : '';
        }""", selectors)

        if region_name:
            print(f"    [Region] {region_name}")

        # Download region flag if available
        region_flag_path = ""
        if region_flag_url and not region_flag_url.startswith("data:"):
            flag_dest = os.path.join(CRESTS_DIR, "flags", f"{_slugify(region_name or country_code or 'unknown')}.png")
            try:
                os.makedirs(os.path.join(BASE_DIR, os.path.dirname(flag_dest)), exist_ok=True)
                future = schedule_image_download(region_flag_url, flag_dest)
                result = future.result(timeout=10)
                if result:
                    region_flag_path = result
            except Exception:
                pass

        # ── Extract + download league crest ──────────────────────────────
        crest_url = await page.evaluate(EXTRACT_CREST_JS, selectors)
        crest_path = ""
        if crest_url and not crest_url.startswith("data:"):
            local_dest = os.path.join(LEAGUE_CRESTS_DIR, f"{_slugify(league_id)}.png")
            future = schedule_image_download(crest_url, local_dest)
            try:
                result = future.result(timeout=15)
                if result:
                    # Upload to Supabase and store public URL
                    remote_name = f"{_slugify(league_id)}.png"
                    sb_url = upload_crest_to_supabase(result, "league-crests", remote_name)
                    crest_path = sb_url if sb_url else result  # Supabase URL or local fallback
                    src = "Supabase" if sb_url else "local"
                    print(f"    [Crest] [OK] League crest -> {src}: {os.path.basename(local_dest)}")
            except Exception:
                print(f"    [Crest] [!] Failed to download crest")

        # ── Extract current season ───────────────────────────────────────
        season = await page.evaluate(EXTRACT_SEASON_JS, selectors)
        print(f"    [Season] {season or '(not found)'}")

        # ── Construct region_league from seed data ────────────────────────
        # continent is from leagues.json (e.g. "Europe", "Africa") — always available
        region_league = f"{continent}: {name}" if continent else name

        # ── Update league in DB with all extracted data ───────────────────
        upsert_league(conn, {
            "league_id": league_id,
            "fs_league_id": fs_league_id or None,
            "name": name,
            "country_code": country_code,
            "continent": league.get("continent"),
            "crest": crest_path,
            "current_season": season,
            "url": url,
            "region": region_name or None,
            "region_flag": region_flag_path or None,
            "region_url": region_url_href or None,
        })

        # ── Extract current season (Fixtures + Results tabs) ─────────────
        fixtures_count = await extract_tab(
            page, url, "fixtures", conn, league_id, season, country_code,
            region_league=region_league
        )
        results_count = await extract_tab(
            page, url, "results", conn, league_id, season, country_code,
            region_league=region_league
        )
        total_matches = fixtures_count + results_count

        # ── Historical seasons (if requested) ────────────────────────
        # URL construction: {base}/football/{country}/{slug}-{startYear}-{endYear}/
        # No archive page needed — construct directly from year offset
        # --season 0 = current only, --season N = offset N past season
        # --seasons N = last N past seasons, --all-seasons = archive fallback
        need_past_seasons = (
            (target_season is not None and target_season >= 1) or
            num_seasons > 0 or
            all_seasons
        )
        if need_past_seasons:
            # Extract slug and country from league URL
            # URL: https://www.flashscore.com/football/{country}/{slug}/
            url_stripped = url.rstrip('/')
            slug = url_stripped.split('/')[-1]       # e.g. "npfl"
            country_slug = url_stripped.split('/')[-2]  # e.g. "nigeria"

            from datetime import datetime
            current_year = datetime.now().year  # e.g. 2026

            if all_seasons:
                # --all-seasons: fall back to archive page for full discovery
                archive_seasons = await get_archive_seasons(page, url)
                season_urls = []
                for s in archive_seasons:
                    s_slug = s.get("slug", "")
                    s_start = s.get("start_year", 0)
                    s_end = s.get("end_year", 0)
                    label = f"{s_start}/{s_end}" if s_start != s_end else str(s_start)
                    s_url = f"https://www.flashscore.com/football/{s.get('country', '')}/{s_slug}/"
                    season_urls.append((label, s_url))
            else:
                # Direct URL construction from year offset
                season_urls = []
                if target_season is not None and target_season >= 1:
                    # --season N: only the Nth past season
                    offsets = [target_season]
                else:
                    # --seasons N: last N past seasons (offsets 1..N)
                    offsets = list(range(1, num_seasons + 1))

                for offset_n in offsets:
                    start_yr = current_year - 1 - offset_n  # e.g. offset 1 -> 2024
                    end_yr = current_year - offset_n        # e.g. offset 1 -> 2025
                    season_label = f"{start_yr}/{end_yr}"
                    season_slug = f"{slug}-{start_yr}-{end_yr}"
                    season_url = f"https://www.flashscore.com/football/{country_slug}/{season_slug}/"
                    season_urls.append((season_label, season_url))

            for s_idx, (season_label, season_base_url) in enumerate(season_urls, 1):
                print(f"\n    [Season {s_idx}/{len(season_urls)}] {season_label}")
                print(f"      URL: {season_base_url}")

                # Results tab for historical seasons
                r_count = await extract_tab(
                    page, season_base_url, "results", conn,
                    league_id, season_label, country_code,
                    region_league=region_league
                )
                total_matches += r_count

                # Fixtures tab (some historical seasons may still have upcoming fixtures)
                f_count = await extract_tab(
                    page, season_base_url, "fixtures", conn,
                    league_id, season_label, country_code,
                    region_league=region_league
                )
                total_matches += f_count

        # ── Mark as processed ────────────────────────────────────────────
        mark_league_processed(conn, league_id)
        print(f"\n  [{idx}/{total}] [OK] {name} COMPLETE -- {total_matches} total matches")

    except Exception as e:
        print(f"\n  [{idx}/{total}] [FAIL] {name} FAILED: {e}")
        traceback.print_exc()
    finally:
        await page.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  Main entry point
# ═══════════════════════════════════════════════════════════════════════════════

async def main(limit: Optional[int] = None, offset: int = 0, reset: bool = False,
               num_seasons: int = 0, all_seasons: bool = False,
               weekly: bool = False, target_season: Optional[int] = None,
               refresh: bool = False):
    """Main enrichment entry point.

    Enrichment modes (in priority order):
        1. --reset:   Re-process ALL leagues from scratch
        2. --refresh: Re-process leagues not updated in 7+ days
        3. (default): Smart gap scan — only leagues with missing data

    Args:
        limit: Max number of leagues to process (after offset)
        offset: Number of leagues to skip from the start (0-indexed)
        reset: Reset all leagues to unprocessed before starting
        num_seasons: Number of past seasons to extract per league
        all_seasons: If True, extract ALL available seasons
        weekly: If True, only process leagues updated >7 days ago
        target_season: If set, extract ONLY the Nth most recent past season (1-indexed)
        refresh: If True, re-process stale leagues (>7 days old)
    """
    print("\n" + "=" * 60)
    print("  FLASHSCORE LEAGUE ENRICHMENT -> SQLite")
    print("=" * 60)

    # ── Initialize DB ────────────────────────────────────────────────────
    conn = init_db()
    print(f"  [DB] Initialized at {os.path.abspath(conn.execute('PRAGMA database_list').fetchone()[2])}")

    if reset:
        conn.execute("UPDATE leagues SET processed = 0")
        conn.commit()
        print("  [DB] Reset all leagues to unprocessed")

    # ── Seed leagues from JSON ───────────────────────────────────────────
    seed_leagues_from_json(conn)

    # ── Select leagues to process ────────────────────────────────────────
    if reset:
        # Full reset: process everything
        leagues = get_unprocessed_leagues(conn)
        scan_mode = "FULL RESET"
    elif refresh or weekly:
        # Stale refresh: leagues not updated in 7+ days
        leagues = get_stale_leagues(conn, days=7)
        scan_mode = "STALE REFRESH (>7 days)"
    else:
        # Default: smart gap scan — leagues with missing critical data
        leagues = get_leagues_with_gaps(conn)
        scan_mode = "GAP SCAN (missing data)"
        
        # If history enrichment is requested, also include leagues that need history
        if num_seasons > 0 or target_season is not None or all_seasons:
            from Data.Access.league_db import get_leagues_missing_seasons
            min_needed = num_seasons if num_seasons > 0 else 2
            if target_season is not None:
                min_needed = max(min_needed, target_season + 1)
            
            history_leagues = get_leagues_missing_seasons(conn, min_seasons=min_needed)
            
            # Merge and dedup
            existing_ids = {lg['league_id'] for lg in leagues}
            added_count = 0
            for lg in history_leagues:
                if lg['league_id'] not in existing_ids:
                    leagues.append(lg)
                    added_count += 1
            if added_count > 0:
                print(f"  [Scan] Identified {added_count} additional leagues missing historical seasons ({min_needed}+ needed)")

    if offset > 0:
        leagues = leagues[offset:]
    if limit:
        leagues = leagues[:limit]

    if not leagues:
        print(f"\n  [Done] No leagues need enrichment ({scan_mode}). Use --reset to force re-process all.")
        return

    total = len(leagues)
    mode_label = "current season"
    if all_seasons:
        mode_label = "ALL seasons"
    elif num_seasons > 0:
        mode_label = f"last {num_seasons} past seasons"
    elif target_season is not None:
        if target_season == 0:
            mode_label = "current season only"
        else:
            mode_label = f"season offset #{target_season} only"
    # ── Workload announcement ─────────────────────────────────────────────
    sync_interval = max(1, total // 5)  # 20% of total
    sync_checkpoints = set(range(sync_interval, total + 1, sync_interval))
    print(f"\n  [Enrich] {total} leagues to process ({scan_mode}, {mode_label}, concurrency={MAX_CONCURRENCY})")
    print(f"  [Sync]   Cloud sync every {sync_interval} leagues (20% checkpoints at: {sorted(sync_checkpoints)})")
    print(f"  [Workload] Leagues #{offset + 1} to #{offset + total}:")

    # ── Ensure crest directories exist (from project root) ───────────────
    os.makedirs(os.path.join(BASE_DIR, LEAGUE_CRESTS_DIR), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, TEAM_CRESTS_DIR), exist_ok=True)

    # ── Optional: import SyncManager for checkpoints ─────────────────────
    sync_mgr = None
    try:
        from Data.Access.sync_manager import SyncManager, TABLE_CONFIG
        sync_mgr = SyncManager()
    except Exception:
        pass  # SyncManager not available (standalone mode or no Supabase)

    completed_count = 0

    # ── Launch Playwright ────────────────────────────────────────────────
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            timezone_id="Africa/Lagos",
        )

        # Process leagues with concurrency control + 20% sync checkpoints
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        crash_counter = 0  # Track consecutive crashes to trigger context restart

        async def _worker(league, idx):
            nonlocal completed_count, context, browser, crash_counter
            async with sem:
                try:
                    await enrich_single_league(
                        context, league, conn, idx, total,
                        num_seasons=num_seasons, all_seasons=all_seasons,
                        target_season=target_season,
                    )
                    crash_counter = 0  # Reset on success
                except Exception as e:
                    err_msg = str(e).lower()
                    if 'crashed' in err_msg or 'target closed' in err_msg:
                        crash_counter += 1
                        if crash_counter >= 2:
                            print(f"\n  [Recovery] Browser crashed {crash_counter}x — recycling browser...")
                            try:
                                await context.close()
                            except Exception:
                                pass
                            try:
                                await browser.close()
                            except Exception:
                                pass
                            browser = await p.chromium.launch(headless=True)
                            context = await browser.new_context(
                                user_agent=(
                                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                                ),
                                viewport={"width": 1920, "height": 1080},
                                timezone_id="Africa/Lagos",
                            )
                            crash_counter = 0
                            print(f"  [Recovery] Fresh browser ready. Continuing enrichment...")

                completed_count += 1

                # 20% sync checkpoint
                if completed_count in sync_checkpoints:
                    pct = int((completed_count / total) * 100)
                    print(f"\n  [Checkpoint] {pct}% complete ({completed_count}/{total})")
                    if sync_mgr and sync_mgr.supabase:
                        try:
                            print(f"  [Sync] Running cloud sync at {pct}%...")
                            for tkey in ('schedules', 'teams', 'leagues'):
                                cfg = TABLE_CONFIG.get(tkey)
                                if cfg:
                                    await sync_mgr._sync_table(tkey, cfg)
                            print(f"  [Sync] Cloud sync at {pct}% complete")
                        except Exception as e:
                            print(f"  [Sync] Cloud sync at {pct}% failed: {e}")

        tasks = [_worker(lg, i) for i, lg in enumerate(leagues, 1)]
        await asyncio.gather(*tasks)

        await context.close()
        await browser.close()

    # ── Final summary ────────────────────────────────────────────────────
    league_count = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
    fixture_count = conn.execute("SELECT COUNT(*) FROM schedules").fetchone()[0]
    team_count = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    processed = conn.execute("SELECT COUNT(*) FROM leagues WHERE processed = 1").fetchone()[0]
    gaps = conn.execute(
        """SELECT COUNT(*) FROM leagues WHERE url IS NOT NULL AND url != ''
           AND (processed = 0 OR fs_league_id IS NULL OR fs_league_id = ''
                OR region IS NULL OR region = '' OR crest IS NULL OR crest = ''
                OR current_season IS NULL OR current_season = '')"""
    ).fetchone()[0]

    # Auto-propagate Supabase crest URLs from teams into schedules
    from Data.Access.db_helpers import propagate_crest_urls
    propagate_crest_urls()

    print(f"\n{'='*60}")
    print(f"  SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"  Leagues:  {league_count} total, {processed} processed, {gaps} with gaps")
    print(f"  Fixtures: {fixture_count}")
    print(f"  Teams:    {team_count}")
    print(f"  DB:       {os.path.abspath(conn.execute('PRAGMA database_list').fetchone()[2])}")
    print(f"{'='*60}\n")

    conn.close()
    executor.shutdown(wait=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich Flashscore leagues -> SQLite")
    parser.add_argument("--limit", type=str, default=None,
                        metavar='N or START-END',
                        help='Limit items processed. Single number (5) or range (501-1000)')
    parser.add_argument("--reset", action="store_true", help="Reset all leagues to unprocessed")
    parser.add_argument("--refresh", action="store_true", help="Re-process stale leagues (>7 days old)")
    parser.add_argument("--seasons", type=int, default=0,
                        help="Number of past seasons to extract (e.g. 5 = last 5 past seasons)")
    parser.add_argument("--season", type=int, default=None,
                        metavar='N',
                        help='Season offset: 0=current, 1=most recent past, 2=second past, etc.')
    parser.add_argument("--all-seasons", action="store_true", help="Extract all available seasons")
    args = parser.parse_args()

    # Parse --limit: single int or range "START-END"
    limit_count = None
    offset = 0
    if args.limit:
        if '-' in args.limit:
            parts = args.limit.split('-', 1)
            start = int(parts[0].strip())
            end = int(parts[1].strip())
            offset = start - 1  # Convert 1-indexed to 0-indexed
            limit_count = end - start + 1
        else:
            limit_count = int(args.limit)

    asyncio.run(main(limit=limit_count, offset=offset, reset=args.reset,
                     num_seasons=args.seasons, all_seasons=args.all_seasons,
                     target_season=args.season, refresh=args.refresh))
