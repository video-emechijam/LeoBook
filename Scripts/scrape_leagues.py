# scrape_leagues.py: Scrape Flashscore league pages -> SQLite database.
# Part of LeoBook Scripts — Data Collection
#
# Usage:
#   python -m Scripts.scrape_leagues              # All leagues
#   python -m Scripts.scrape_leagues --limit 5    # First 5 unprocessed
#   python -m Scripts.scrape_leagues --reset      # Reset processed flags
#
# Reads Data/Store/leagues.json -> populates leagues/teams/fixtures tables
# Downloads crests concurrently via ThreadPoolExecutor

import asyncio
import argparse
import json
import os
import re
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from playwright.async_api import async_playwright, Page

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from Data.Access.league_db import (
    init_db, get_connection, upsert_league, upsert_team, upsert_fixture,
    bulk_upsert_fixtures, mark_league_processed, get_unprocessed_leagues,
    get_league_db_id, get_team_id,
)
from Core.Browser.site_helpers import fs_universal_popup_dismissal

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEAGUES_JSON = os.path.join(BASE_DIR, "Data", "Store", "leagues.json")
CRESTS_DIR = os.path.join(BASE_DIR, "Data", "Store", "crests")
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
    if os.path.exists(dest_path):
        return dest_path
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Referer": "https://www.flashscore.com/",
        })
        if resp.status_code == 200 and len(resp.content) > 100:
            with open(dest_path, "wb") as f:
                f.write(resp.content)
            return dest_path
    except Exception as e:
        pass  # Silently skip failed downloads
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
#  Step 2-7: Scrape a single league page
# ═══════════════════════════════════════════════════════════════════════════════

# ── JS to extract all match data from the page ──────────────────────────────
EXTRACT_MATCHES_JS = r"""() => {
    const matches = [];
    const currentYear = new Date().getFullYear();

    // Walk ALL sibling elements in the event list to track round context
    // Elements appear in order: round headers, then match rows, then more rounds, etc.
    const container = document.querySelector('.sportName, .leagues--static, [class*="event__"]')?.parentElement
        || document.body;

    // Gather all relevant elements in DOM order
    const allEls = container.querySelectorAll(
        '.event__round, [id^="g_1_"]'
    );

    let currentRound = '';

    allEls.forEach(el => {
        // Track round headers
        if (el.classList.contains('event__round')) {
            currentRound = el.innerText.trim();
            return;
        }

        // Must be a match row
        if (!el.id || !el.id.startsWith('g_1_')) return;

        const row = el;
        const fixtureId = row.id.replace('g_1_', '');

        // ── Time + Date (desktop format: "DD.MM. HH:MM" or "DD.MM.YYYY HH:MM") ──
        const timeEl = row.querySelector('.event__time');
        let matchTime = '';
        let matchDate = '';
        if (timeEl) {
            const raw = timeEl.innerText.trim();
            // Try DD.MM.YYYY HH:MM first
            const fullMatch = raw.match(/(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})/);
            if (fullMatch) {
                matchDate = `${fullMatch[3]}-${fullMatch[2]}-${fullMatch[1]}`;
                matchTime = `${fullMatch[4]}:${fullMatch[5]}`;
            } else {
                // Try DD.MM. HH:MM (no year)
                const shortMatch = raw.match(/(\d{2})\.(\d{2})\.\s*(\d{2}):(\d{2})/);
                if (shortMatch) {
                    matchDate = `${currentYear}-${shortMatch[2]}-${shortMatch[1]}`;
                    matchTime = `${shortMatch[3]}:${shortMatch[4]}`;
                } else {
                    // Just time HH:MM
                    const justTime = raw.match(/(\d{2}):(\d{2})/);
                    if (justTime) matchTime = `${justTime[1]}:${justTime[2]}`;
                }
            }
        }

        // ── Home team ──
        const homeEl = row.querySelector('.event__homeParticipant');
        const homeName = homeEl ?
            (homeEl.querySelector('[class*="wcl-name"], .event__participant--name') || homeEl)
                .innerText.trim().replace(/\s*\(.*?\)\s*$/, '') : '';

        // ── Away team ──
        const awayEl = row.querySelector('.event__awayParticipant');
        const awayName = awayEl ?
            (awayEl.querySelector('[class*="wcl-name"], .event__participant--name') || awayEl)
                .innerText.trim().replace(/\s*\(.*?\)\s*$/, '') : '';

        // ── Scores ──
        const homeScoreEl = row.querySelector('.event__score--home');
        const awayScoreEl = row.querySelector('.event__score--away');
        const homeScoreText = homeScoreEl ? homeScoreEl.innerText.trim() : '';
        const awayScoreText = awayScoreEl ? awayScoreEl.innerText.trim() : '';
        const homeScore = homeScoreText && homeScoreText !== '-' ? parseInt(homeScoreText) : null;
        const awayScore = awayScoreText && awayScoreText !== '-' ? parseInt(awayScoreText) : null;

        // ── Match status ──
        // Desktop: finished matches have data-state="final" or class wcl-isFinal on score
        // Live/special: .event__stage--block or .event__stage contains "75'", "HT", "Postp." etc.
        let matchStatus = '';
        const stageEl = row.querySelector('.event__stage--block, .event__stage');
        if (stageEl) {
            matchStatus = stageEl.innerText.trim();
        } else if (homeScoreEl) {
            // Check data-state or isFinal class
            const state = homeScoreEl.getAttribute('data-state') || '';
            const isFinal = homeScoreEl.className.includes('isFinal') ||
                            homeScoreEl.className.includes('Final');
            if (state === 'final' || isFinal) {
                matchStatus = 'FT';
            } else if (homeScore !== null) {
                matchStatus = 'FT';  // Has score = finished
            }
        }

        // ── Team crests ──
        // Desktop: logos are separate elements, not inside participant containers
        const homeImg = row.querySelector('.event__logo--home img, .event__homeParticipant img');
        const awayImg = row.querySelector('.event__logo--away img, .event__awayParticipant img');
        const homeCrest = homeImg ? (homeImg.src || homeImg.getAttribute('data-src') || '') : '';
        const awayCrest = awayImg ? (awayImg.src || awayImg.getAttribute('data-src') || '') : '';

        matches.push({
            fixture_id: fixtureId,
            date: matchDate,
            time: matchTime,
            home_team_name: homeName,
            away_team_name: awayName,
            home_score: homeScore,
            away_score: awayScore,
            match_status: matchStatus,
            home_crest_url: homeCrest,
            away_crest_url: awayCrest,
            league_stage: currentRound,
            url: `/match/${fixtureId}/#/match-summary`
        });
    });

    return matches;
}"""

# ── JS to extract season text ───────────────────────────────────────────────
EXTRACT_SEASON_JS = r"""() => {
    // Try multiple selectors for the season/competition header
    const selectors = [
        '.heading__info',
        '.heading__title--desc',
        '.tournamentHeader__season',
        '.heading__category'
    ];
    for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el) {
            const text = el.innerText.trim();
            // Look for year patterns like 2024/2025 or 2025
            const match = text.match(/(\d{4}(?:\/\d{4})?)/);
            if (match) return match[1];
        }
    }
    // Fallback: check breadcrumbs
    const breadcrumbs = document.querySelectorAll('.breadcrumb__text');
    for (const b of breadcrumbs) {
        const match = b.innerText.match(/(\d{4}(?:\/\d{4})?)/);
        if (match) return match[1];
    }
    return '';
}"""

# ── JS to extract league crest URL ──────────────────────────────────────────
EXTRACT_CREST_JS = r"""() => {
    const img = document.querySelector('img.heading__logo, .heading__logo img, .tournamentHeader__logo img');
    return img ? (img.src || img.getAttribute('data-src') || '') : '';
}"""


async def _expand_show_more(page: Page, max_clicks: int = MAX_SHOW_MORE):
    """Click 'Show more matches' exhaustively."""
    clicks = 0
    while clicks < max_clicks:
        try:
            btn = page.locator(".event__more, a.event__more--static")
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


async def scrape_tab(page: Page, league_url: str, tab: str, conn, league_db_id: int,
                     season: str, country_code: str) -> int:
    """Navigate to a league tab (fixtures or results), expand, extract, and save matches."""
    url = league_url.rstrip("/") + f"/{tab}/"
    print(f"    [{tab.upper()}] Navigating to {url}")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)
        await fs_universal_popup_dismissal(page)
    except Exception as e:
        print(f"    [{tab.upper()}] Navigation failed: {e}")
        return 0

    # Expand all matches
    await _expand_show_more(page)

    # Extract match data
    try:
        matches_raw = await page.evaluate(EXTRACT_MATCHES_JS)
    except Exception as e:
        print(f"    [{tab.upper()}] Extraction failed: {e}")
        return 0

    if not matches_raw:
        print(f"    [{tab.upper()}] No matches found")
        return 0

    # Process matches: schedule image downloads + build fixture rows
    fixture_rows = []
    crest_futures = []

    for m in matches_raw:
        home_name = m.get("home_team_name", "")
        away_name = m.get("away_team_name", "")
        if not home_name or not away_name:
            continue

        # Upsert teams
        home_team_id = None
        away_team_id = None
        if home_name:
            upsert_team(conn, {
                "name": home_name,
                "country_code": country_code,
                "league_ids": [league_db_id],
            })
            home_team_id = get_team_id(conn, home_name, country_code)

        if away_name:
            upsert_team(conn, {
                "name": away_name,
                "country_code": country_code,
                "league_ids": [league_db_id],
            })
            away_team_id = get_team_id(conn, away_name, country_code)

        # Schedule team crest downloads
        home_crest_url = m.get("home_crest_url", "")
        away_crest_url = m.get("away_crest_url", "")
        home_crest_path = ""
        away_crest_path = ""

        if home_crest_url and not home_crest_url.startswith("data:"):
            ext = ".png"
            dest = os.path.join(TEAM_CRESTS_DIR, f"{_slugify(home_name)}{ext}")
            crest_futures.append((schedule_image_download(home_crest_url, dest), "home", home_name, dest))
            home_crest_path = dest

        if away_crest_url and not away_crest_url.startswith("data:"):
            ext = ".png"
            dest = os.path.join(TEAM_CRESTS_DIR, f"{_slugify(away_name)}{ext}")
            crest_futures.append((schedule_image_download(away_crest_url, dest), "away", away_name, dest))
            away_crest_path = dest

        # Determine match extra info based on status
        status = m.get("match_status", "")
        extra = None
        if status:
            status_upper = status.upper()
            if "FT" in status_upper or "FINISHED" in status_upper:
                extra = "FINISHED"
            elif "AET" in status_upper:
                extra = "AFTER EXTRA TIME (AET)"
            elif "PEN" in status_upper:
                extra = "AFTER PENALTY (AFTER PEN)"
            elif "POST" in status_upper:
                extra = "POSTPONED"
            elif "CANC" in status_upper:
                extra = "CANCELED"
            elif "ABD" in status_upper or "ABAN" in status_upper:
                extra = "ABANDONED"
            elif "LIVE" in status_upper or "'" in status:
                extra = "TO FINISH"
            elif "HT" in status_upper:
                extra = "TO FINISH"
            elif status == "-":
                extra = "SCHEDULED"

        fixture_rows.append({
            "fixture_id": m.get("fixture_id", ""),
            "date": m.get("date", ""),
            "time": m.get("time", ""),
            "league_id": league_db_id,
            "home_team_id": home_team_id,
            "home_team_name": home_name,
            "away_team_id": away_team_id,
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
        })

    # Bulk insert fixtures
    if fixture_rows:
        bulk_upsert_fixtures(conn, fixture_rows)

    # Wait for crest downloads to finish (non-blocking for the event loop)
    downloaded = 0
    for fut, side, name, dest in crest_futures:
        try:
            result = fut.result(timeout=30)
            if result:
                downloaded += 1
                # Update team crest path in DB
                conn.execute(
                    "UPDATE teams SET crest = ? WHERE name = ? AND country_code = ?",
                    (dest, name, country_code)
                )
        except Exception:
            pass
    if downloaded:
        conn.commit()

    print(f"    [{tab.upper()}] [OK] Saved {len(fixture_rows)} matches, downloaded {downloaded} crests")
    return len(fixture_rows)


async def scrape_single_league(context, league: Dict[str, Any], conn, idx: int, total: int):
    """Process a single league: crest + season + fixtures + results."""
    league_id = league["league_id"]
    name = league["name"]
    url = league.get("url", "")
    country_code = league.get("country_code", "")

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

        # ── Extract + download league crest ──────────────────────────────
        crest_url = await page.evaluate(EXTRACT_CREST_JS)
        crest_path = ""
        if crest_url and not crest_url.startswith("data:"):
            ext = ".png"
            dest = os.path.join(LEAGUE_CRESTS_DIR, f"{_slugify(league_id)}{ext}")
            future = schedule_image_download(crest_url, dest)
            # Don't block — we'll check later
            try:
                result = future.result(timeout=15)
                if result:
                    crest_path = result
                    print(f"    [Crest] [OK] Downloaded league crest -> {os.path.basename(dest)}")
            except Exception:
                print(f"    [Crest] [!] Failed to download crest")

        # ── Extract current season ───────────────────────────────────────
        season = await page.evaluate(EXTRACT_SEASON_JS)
        print(f"    [Season] {season or '(not found)'}")

        # ── Update league in DB ──────────────────────────────────────────
        upsert_league(conn, {
            "league_id": league_id,
            "name": name,
            "country_code": country_code,
            "continent": league.get("continent"),
            "crest": crest_path,
            "current_season": season,
            "url": url,
        })
        league_db_id = get_league_db_id(conn, league_id)

        # ── Scrape Fixtures tab ──────────────────────────────────────────
        fixtures_count = await scrape_tab(
            page, url, "fixtures", conn, league_db_id, season, country_code
        )

        # ── Scrape Results tab ───────────────────────────────────────────
        results_count = await scrape_tab(
            page, url, "results", conn, league_db_id, season, country_code
        )

        # ── Mark as processed ────────────────────────────────────────────
        mark_league_processed(conn, league_id)
        total_matches = fixtures_count + results_count
        print(f"\n  [{idx}/{total}] [OK] {name} COMPLETE — {total_matches} total matches")

    except Exception as e:
        print(f"\n  [{idx}/{total}] [FAIL] {name} FAILED: {e}")
        traceback.print_exc()
    finally:
        await page.close()


# ═══════════════════════════════════════════════════════════════════════════════
#  Main entry point
# ═══════════════════════════════════════════════════════════════════════════════

async def main(limit: Optional[int] = None, reset: bool = False):
    """Main scraper entry point."""
    print("\n" + "=" * 60)
    print("  FLASHSCORE LEAGUE SCRAPER -> SQLite")
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

    # ── Get unprocessed leagues ──────────────────────────────────────────
    leagues = get_unprocessed_leagues(conn)
    if limit:
        leagues = leagues[:limit]

    if not leagues:
        print("\n  [Done] All leagues have been processed. Use --reset to reprocess.")
        return

    total = len(leagues)
    print(f"\n  [Scrape] {total} leagues to process (concurrency={MAX_CONCURRENCY})")

    # ── Ensure crest directories exist ───────────────────────────────────
    os.makedirs(LEAGUE_CRESTS_DIR, exist_ok=True)
    os.makedirs(TEAM_CRESTS_DIR, exist_ok=True)

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

        # Process leagues with concurrency control
        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        async def _worker(league, idx):
            async with sem:
                await scrape_single_league(context, league, conn, idx, total)

        tasks = [_worker(lg, i) for i, lg in enumerate(leagues, 1)]
        await asyncio.gather(*tasks)

        await context.close()
        await browser.close()

    # ── Final summary ────────────────────────────────────────────────────
    league_count = conn.execute("SELECT COUNT(*) FROM leagues").fetchone()[0]
    fixture_count = conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0]
    team_count = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    processed = conn.execute("SELECT COUNT(*) FROM leagues WHERE processed = 1").fetchone()[0]

    print(f"\n{'='*60}")
    print(f"  SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"  Leagues:  {league_count} total, {processed} processed")
    print(f"  Fixtures: {fixture_count}")
    print(f"  Teams:    {team_count}")
    print(f"  DB:       {os.path.abspath(conn.execute('PRAGMA database_list').fetchone()[2])}")
    print(f"{'='*60}\n")

    conn.close()
    executor.shutdown(wait=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Flashscore leagues -> SQLite")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of leagues to process")
    parser.add_argument("--reset", action="store_true", help="Reset all leagues to unprocessed")
    args = parser.parse_args()

    asyncio.run(main(limit=args.limit, reset=args.reset))
