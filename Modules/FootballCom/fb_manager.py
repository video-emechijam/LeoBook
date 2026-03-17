# fb_manager.py: Orchestration layer for Football.com odds + booking.
# Part of LeoBook Modules — Football.com
#
# Functions: _create_session(), _create_session_no_login(), run_odds_harvesting(), run_automated_booking()
# Called by: Leo.py (Chapter 1 Page 1, Chapter 2 Page 1)

"""
Football.com Orchestrator — v4.0 (Single-nav per league, fuzzy-only matching, concurrent odds)
Two exported functions with shared session setup.
"""

import asyncio
import json
import os
import sqlite3
import time
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional

from playwright.async_api import Playwright, Page

from Core.Utils.constants import MAX_CONCURRENCY, now_ng, WAIT_FOR_LOAD_STATE_TIMEOUT, FB_MOBILE_USER_AGENT, FB_MOBILE_VIEWPORT
from Core.Utils.utils import log_error_state
from Core.System.lifecycle import log_state
from Core.Intelligence.aigo_suite import AIGOSuite
from .odds_extractor import OddsExtractor, OddsResult
from .fb_session import launch_browser_with_retry
from .navigator import load_or_create_session, extract_balance, hide_overlays
from .extractor import extract_league_matches, validate_match_data
from Data.Access.db_helpers import (
    get_site_match_id, save_site_matches, save_match_odds,
    update_site_match_status,
)
from Data.Access.league_db import LEAGUES_JSON_PATH


# ── SearchDict guards ──────────────────────────────────────────────────
_ENRICHMENT_IN_PROGRESS: set[str] = set()
_ENRICHMENT_LOCK: asyncio.Lock = asyncio.Lock()

# ── Batch resume checkpoint ─────────────────────────────────────────────
_CHECKPOINT_PATH = Path("Data/Logs/batch_checkpoint.json")

def _load_checkpoint() -> int:
    """Return last completed batch index for today (0 = start fresh)."""
    if _CHECKPOINT_PATH.exists():
        try:
            c = json.loads(_CHECKPOINT_PATH.read_text(encoding='utf-8'))
            if c.get("date") == now_ng().strftime("%Y-%m-%d"):
                return int(c.get("last_batch", 0))
        except Exception:
            pass
    return 0

def _save_checkpoint(batch_idx: int) -> None:
    """Persist last completed batch index for today."""
    _CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CHECKPOINT_PATH.write_text(
        json.dumps({"date": now_ng().strftime("%Y-%m-%d"), "last_batch": batch_idx}),
        encoding='utf-8',
    )


# ── Shared session helpers ──────────────────────────────────────────────

async def _create_session(playwright: Playwright):
    """Full session setup: launch browser, login, extract balance. For bet placement."""
    user_data_dir = Path("Data/Auth/ChromeData_v3").absolute()
    user_data_dir.mkdir(parents=True, exist_ok=True)

    context = await launch_browser_with_retry(playwright, user_data_dir)
    _, page = await load_or_create_session(context)

    current_balance = await extract_balance(page)
    from Core.Utils.constants import CURRENCY_SYMBOL
    print(f"  [Balance] Current: {CURRENCY_SYMBOL}{current_balance:.2f}")

    return context, page, current_balance


async def _create_session_no_login(playwright: Playwright):
    """Lightweight session: fresh browser, NO login, NO saved state.
    Ch1P1 is anonymous — no ChromeData, no cookies, no session persistence."""
    from Core.Utils.constants import WAIT_FOR_LOAD_STATE_TIMEOUT

    # Auto-detect headless: Codespaces / CI have no display
    is_headless = os.getenv("CODESPACES") == "true" or (os.name != "nt" and not os.environ.get("DISPLAY"))

    browser = await playwright.chromium.launch(
        headless=is_headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage"
        ]
    )
    context = await browser.new_context(
        viewport=FB_MOBILE_VIEWPORT,
        user_agent=FB_MOBILE_USER_AGENT
    )
    page = await context.new_page()

    # Stash browser ref on context so we can close it later
    context._browser_ref = browser
    return context, page


# ── League fb_url loader ────────────────────────────────────────────────

def _load_fb_league_lookup() -> Dict[str, dict]:
    """Load leagues.json and return {league_id: entry} for entries with fb_url."""
    try:
        with open(LEAGUES_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {l['league_id']: l for l in data if l.get('fb_url')}
    except Exception as e:
        print(f"  [Error] Failed to load leagues.json: {e}")
        return {}


# ── Time filter ─────────────────────────────────────────────────────────

def _filter_imminent_matches(fixtures: List[dict], cutoff_hours: float = 0.5) -> List[dict]:
    """Remove matches whose start time is within cutoff_hours of now_ng().
    Returns only matches that are far enough in the future to extract odds for."""
    now = now_ng()
    cutoff = now + timedelta(hours=cutoff_hours)
    kept = []
    skipped = 0
    for f in fixtures:
        date_str = f.get('date', '')
        time_str = f.get('time', '') or '00:00'
        try:
            from datetime import datetime
            match_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            # Attach WAT timezone
            from Core.Utils.constants import TZ_NG
            match_dt = match_dt.replace(tzinfo=TZ_NG)
            if match_dt < cutoff:
                skipped += 1
                continue
        except (ValueError, TypeError):
            pass  # Can't parse -> keep it (don't drop on uncertainty)
        kept.append(f)

    if skipped:
        print(f"  [Filter] Skipped {skipped} matches starting within {cutoff_hours}h of now.")
    return kept


# ── Concurrent odds worker (semaphore-bounded) ─────────────────────────

async def _odds_worker(
    sem: asyncio.Semaphore,
    context,
    match_row: Dict,
    conn,
) -> Optional[OddsResult]:
    """
    Semaphore-bounded odds extractor worker.
    Opens its own page, extracts, closes page.

    v5.0 (2026-03-17): Handles context-closed errors gracefully,
    takes debug screenshot on 0 outcomes after final retry.
    """
    async with sem:
        odds_page = None
        try:
            odds_page = await context.new_page()
            await odds_page.set_viewport_size({"width": 500, "height": 640})

            match_url  = match_row.get("url", "")
            fixture_id = match_row.get("fixture_id", "")
            site_id    = match_row.get("site_match_id", "")

            if not match_url:
                return None

            await odds_page.goto(
                match_url,
                wait_until="domcontentloaded",
                timeout=25000,
            )
            await asyncio.sleep(1.5)

            result: Optional[OddsResult] = None
            # Retry loop: up to 3 attempts if 0 outcomes extracted
            for attempt in range(3):
                extractor = OddsExtractor(odds_page, conn)
                result = await extractor.extract(fixture_id, site_id)

                if result.outcomes_extracted > 0:
                    break  # Success — all done

                if attempt < 2:
                    delay = 2 * (attempt + 1)  # 2s, 4s
                    print(
                        f"    [Odds] {fixture_id}: 0 outcomes on attempt {attempt + 1}/3. "
                        f"Reloading page in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                    try:
                        await odds_page.reload(
                            wait_until="domcontentloaded", timeout=25000
                        )
                        await asyncio.sleep(1.5)
                    except Exception as reload_err:
                        print(f"    [Odds] {fixture_id}: reload failed: {reload_err}")
                        break

            # Debug screenshot on final 0-outcome result
            if result and result.outcomes_extracted == 0:
                try:
                    ss_name = f"debug_odds_final_{fixture_id}_{int(time.time())}.png"
                    await odds_page.screenshot(path=ss_name)
                    print(f"    [Debug] Final 0-outcome screenshot: {ss_name}")
                except Exception:
                    pass

            print(
                f"    [Odds] {fixture_id} -> "
                f"{result.markets_found} markets, "
                f"{result.outcomes_extracted} outcomes "
                f"({result.duration_ms}ms)"
            )

            # Save extracted date/time to fb_matches
            if result.match_date or result.match_time:
                try:
                    update_kwargs = {}
                    if result.match_date:
                        update_kwargs["date"] = result.match_date
                    if result.match_time:
                        update_kwargs["match_time"] = result.match_time
                    if site_id and update_kwargs:
                        update_site_match_status(
                            site_id, "odds_extracted",
                            fixture_id=fixture_id,
                            **update_kwargs,
                        )
                        print(f"    [Odds] {fixture_id}: saved date={result.match_date} time={result.match_time}")
                except Exception as dt_save_err:
                    print(f"    [Odds] {fixture_id}: date/time save skipped: {dt_save_err}")

            return result

        except Exception as e:
            err_str = str(e)
            is_closed = "closed" in err_str.lower()
            if is_closed:
                print(f"    [Odds] {match_row.get('fixture_id')}: context/page closed — skipping gracefully")
            else:
                print(f"    [Odds] ERROR {match_row.get('fixture_id')}: {e}")
            # Try screenshot before giving up
            if odds_page:
                try:
                    ss_name = f"debug_odds_crash_{match_row.get('fixture_id', 'unknown')}_{int(time.time())}.png"
                    await odds_page.screenshot(path=ss_name)
                except Exception:
                    pass
            return OddsResult(
                fixture_id=match_row.get("fixture_id", ""),
                site_match_id=match_row.get("site_match_id", ""),
                markets_found=0, outcomes_extracted=0,
                duration_ms=0, error=err_str,
            )
        finally:
            if odds_page:
                try:
                    await odds_page.close()
                except Exception:
                    pass



# ── Concurrent league worker (semaphore-bounded) ───────────────────────

async def _league_worker(
    semaphore: asyncio.Semaphore,
    browser_context,
    league_id: str,
    league_name: str,
    fs_fixtures: List[Dict],
    fb_url: str,
    conn: sqlite3.Connection,
    matcher,
) -> List[Dict]:
    """
    Semaphore-bounded worker: one league → one page.
    EXTRACTION ONLY — opens a fresh page, extracts all matches from
    football.com, pairs each FS fixture with its candidate fb matches,
    then closes the page and returns the pairs.

    Resolution (fuzzy + LLM) is intentionally NOT done here.
    It runs in a dedicated sequential phase AFTER all leagues have
    been extracted, so that:
      - All browser pages are closed before any LLM quota is consumed.
      - LLM health checks and resolver calls never interleave with
        concurrent page extraction.

    Returns: list of dicts, each with keys:
        'fs_fix'      — original FS fixture dict
        'candidates'  — list of fb match dicts from the page
        'league_name' — for logging in the resolution phase
    """
    async with semaphore:
        page = None
        try:
            page = await browser_context.new_page()
            await page.set_viewport_size({"width": 500, "height": 640})

            print(f"\n  [League] {league_name} ({len(fs_fixtures)} fixtures) → {fb_url}")

            first_date = fs_fixtures[0].get('date', '') if fs_fixtures else ''
            all_page_matches = await extract_league_matches(
                page,
                first_date,
                target_league_name=league_name,
                fb_url=fb_url,
                expected_count=len(fs_fixtures),
            )

            if not all_page_matches:
                print(f"  [League] {league_name}: no matches on page")
                return []

            all_page_matches = await validate_match_data(all_page_matches)

            # Pair each FS fixture with its page candidates — no resolution yet.
            extraction_pairs = []
            for fs_fix in fs_fixtures:
                home = (fs_fix.get('home_team_name') or '').strip()
                away = (fs_fix.get('away_team_name') or '').strip()
                fix_date = fs_fix.get('date', '')

                if not home or not away:
                    continue

                # Normalise key names before passing to the resolver.
                # extract_league_matches() returns dicts with 'home'/'away' keys,
                # but GrokMatcher.resolve_with_cascade() reads 'home_team'/'away_team'.
                # Adding both aliases here means neither side needs to change.
                raw_candidates = [
                    m for m in all_page_matches
                    if not fix_date or m.get('date', '') == fix_date
                ] or all_page_matches

                candidates = [
                    {**m, 'home_team': m.get('home', ''), 'away_team': m.get('away', '')}
                    for m in raw_candidates
                ]

                extraction_pairs.append({
                    'fs_fix': fs_fix,
                    'candidates': candidates,
                    'league_name': league_name,
                })

            return extraction_pairs

        except Exception as e:
            print(f"  [League] ERROR {league_name}: {e}")
            return []
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass


# ── SearchDict Enrichment logic ──────────────────────────────────────

async def _run_searchdict_enrichment(
    resolved_matches: List[Dict],
    conn: sqlite3.Connection,
) -> None:
    """
    One-shot SearchDict enrichment for all unique teams
    resolved this session. Runs ONCE between league phase
    and odds phase. Deduplicates by team name before LLM call.
    """
    try:
        team_pairs = []
        seen_names = set()
        
        for m in resolved_matches:
            # matched == True (only resolved fixtures) checked by caller
            # Each match dict has keys: home, away (Football.com names)
            for prefix in ["home", "away"]:
                name = m.get(prefix)
                tid = m.get(f"{prefix}_id") # FS ID added in _league_worker
                
                if name and name not in seen_names:
                    if name not in _ENRICHMENT_IN_PROGRESS:
                        if tid:
                            team_pairs.append({"team_id": tid, "team_name": name})
                            seen_names.add(name)

        if not team_pairs:
            print("    [SearchDict] All teams already enriched this session.")
            return

        async with _ENRICHMENT_LOCK:
            # Re-filter inside lock for concurrency safety
            final_pairs = [p for p in team_pairs if p["team_name"] not in _ENRICHMENT_IN_PROGRESS]
            if not final_pairs:
                print("    [SearchDict] All teams already enriched this session.")
                return

            for p in final_pairs:
                _ENRICHMENT_IN_PROGRESS.add(p["team_name"])

            from Scripts.build_search_dict import enrich_batch_teams_search_dict
            print(f"    [SearchDict] Enriched {len(final_pairs)} teams in batch.")
            await enrich_batch_teams_search_dict(final_pairs)

    except Exception as e:
        print(f"    [SearchDict] Enrichment failed: {e} — continuing.")


# ── CHAPTER 1 PAGE 1 — Odds Harvesting ─────────────────────────────────

@AIGOSuite.aigo_retry(max_retries=2, delay=5.0)
async def run_odds_harvesting(playwright: Playwright):
    """
    Chapter 1 Page 1: Direct fb_url Odds Harvesting (V4 — single-nav, fuzzy-only).

    Flow:
    1. Load weekly fixtures from schedules table
    2. Filter out matches starting within 30 min
    3. Lookup fb_url per league from unified leagues.json
    4. ONE navigation per league, extract ALL matches from page
    5. Fuzzy-match each fixture (sync, no LLM) via resolve_fixture_to_fb_match
    6. Save resolved matches to SQLite immediately
    7. Concurrent odds extraction (semaphore-bounded, MAX_CONCURRENCY pages)
    8. Post-session Supabase sync
    """
    print("\n--- Running Football.com Direct Odds Extraction (Chapter 1 P1 v9) ---")

    from Core.Intelligence.prediction_pipeline import get_weekly_fixtures
    from Data.Access.league_db import init_db, get_connection
    from .match_resolver import GrokMatcher

    conn = init_db()
    weekly_fixtures = get_weekly_fixtures(conn)
    if not weekly_fixtures:
        print("  [Info] No scheduled fixtures found for the next 7 days.")
        return

    # 1. Time filter — drop matches starting within 30 min
    weekly_fixtures = _filter_imminent_matches(weekly_fixtures, cutoff_hours=0.5)
    if not weekly_fixtures:
        print("  [Info] All remaining fixtures are too imminent (<30 min). Nothing to extract.")
        return

    # 2. Load fb_url lookup
    fb_lookup = _load_fb_league_lookup()
    if not fb_lookup:
        print("  [Warning] No fb_url mappings found in leagues.json. Cannot extract odds.")
        return
    print(f"  [Leagues] {len(fb_lookup)} leagues with fb_url loaded.")

    # 3. Group fixtures by league_id (only for leagues that have fb_url)
    leagues_to_extract: Dict[str, List[dict]] = {}
    skipped_no_fb = 0
    for f in weekly_fixtures:
        lid = f.get('league_id', '')
        if lid in fb_lookup:
            leagues_to_extract.setdefault(lid, []).append(f)
        else:
            skipped_no_fb += 1

    if skipped_no_fb:
        print(f"  [Info] {skipped_no_fb} fixtures skipped (league not mapped to football.com).")

    if not leagues_to_extract:
        print("  [Info] No fixtures matched any mapped league. Nothing to extract.")
        return

    total_fixtures = sum(len(v) for v in leagues_to_extract.values())
    total_leagues = len(leagues_to_extract)
    print(f"  [Pipeline] {total_fixtures} fixtures across "
          f"{total_leagues} leagues to process "
          f"({total_leagues} page loads, was {total_fixtures}).")

    # 4. Launch matcher
    matcher = GrokMatcher()
    all_resolved_matches: List[Dict] = []
    total_session_odds_count = 0

    # 5. Process in batches to prevent OOM
    BATCH_SIZE = 25
    league_ids = list(leagues_to_extract.keys())
    batches = [league_ids[i:i + BATCH_SIZE] for i in range(0, len(league_ids), BATCH_SIZE)]

    # Resume from last completed batch (same calendar day only)
    resume_from = _load_checkpoint()
    if resume_from > 0:
        print(f"  [Resume] Checkpoint found — skipping batches 1–{resume_from}, "
              f"starting at batch {resume_from + 1}/{len(batches)}")

    print(f"  [System] Processing {total_leagues} leagues in {len(batches)} batches (Size: {BATCH_SIZE})...")

    for batch_idx, batch_ids in enumerate(batches):
        if batch_idx < resume_from:   # already completed today
            continue
        batch_num = batch_idx + 1
        print(f"\n  [Batch {batch_num}/{len(batches)}] Starting extraction for {len(batch_ids)} leagues...")
        
        context = None
        try:
            context, _ = await _create_session_no_login(playwright)
            league_sem = asyncio.Semaphore(MAX_CONCURRENCY)
            
            league_tasks = []
            for lid in batch_ids:
                league_entry = fb_lookup[lid]
                fb_url = league_entry['fb_url']
                lname = league_entry.get('fb_league_name', league_entry.get('name', lid))
                fs_fixtures = leagues_to_extract[lid]
                league_tasks.append(
                    _league_worker(
                        league_sem, context,
                        lid, lname,
                        fs_fixtures, fb_url, conn, matcher,
                    )
                )

            batch_extraction_results = await asyncio.gather(*league_tasks, return_exceptions=True)
            
            # Close browser context immediately after extraction to free memory
            if context:
                await context.close()
                if hasattr(context, '_browser_ref'):
                    await context._browser_ref.close()
                context = None

            # Flatten pairs for this batch
            batch_pairs: List[Dict] = []
            for res in batch_extraction_results:
                if isinstance(res, list):
                    batch_pairs.extend(res)

            if not batch_pairs:
                print(f"    [Batch {batch_num}] No fixtures extracted.")
                continue

            # 6. Resolve batch immediately
            print(f"    [Batch {batch_num}] Resolving {len(batch_pairs)} fixtures...")
            batch_resolved = []
            for pair in batch_pairs:
                fs_fix = pair['fs_fix']
                candidates = pair['candidates']

                match_row, score, method = await matcher.resolve_with_cascade(
                    fs_fix, candidates, conn
                )

                if match_row:
                    match_row["home_id"] = fs_fix.get("home_team_id") or fs_fix.get("home_id")
                    match_row["away_id"] = fs_fix.get("away_team_id") or fs_fix.get("away_id")
                    match_row["resolution_method"] = method
                    save_site_matches([match_row])  # immediate SQLite save
                    batch_resolved.append(match_row)
                    all_resolved_matches.append(match_row)
                else:
                    all_resolved_matches.append({"status": "failed", "resolution_method": "failed"})

            # 7. Extract odds for batch (also requires a browser session)
            if batch_resolved:
                print(f"    [Batch {batch_num}] Extracting odds for {len(batch_resolved)} matches...")
                context_odds, _ = await _create_session_no_login(playwright)
                try:
                    odds_sem = asyncio.Semaphore(MAX_CONCURRENCY)
                    odds_conn = get_connection()
                    results = await asyncio.gather(
                        *[
                            _odds_worker(odds_sem, context_odds, m, odds_conn)
                            for m in batch_resolved
                        ],
                        return_exceptions=True,
                    )
                    
                    batch_outcomes = sum(
                        r.outcomes_extracted for r in results 
                        if isinstance(r, OddsResult)
                    )
                    total_session_odds_count += batch_outcomes
                    print(f"    [Batch {batch_num}] Odds extracted: {batch_outcomes} outcomes.")
                finally:
                    if context_odds:
                        await context_odds.close()
                        if hasattr(context_odds, '_browser_ref'):
                            await context_odds._browser_ref.close()

        except Exception as e:
            print(f"  [Batch {batch_num}] CRITICAL ERROR: {e}")
            if context:
                await context.close()
        
        # Small cooldown between batches
        await asyncio.sleep(2)
        _save_checkpoint(batch_idx + 1)  # mark this batch complete

    # Full session completed — clear checkpoint so next day starts fresh
    _CHECKPOINT_PATH.unlink(missing_ok=True)

    # 8. Post-Harvest Processing (SearchDict + Sync)
    print("\n  [Post-Harvest] Starting global enrichment and sync...")
    
    # ── SearchDict: one-shot batch enrichment ──────────────
    all_resolved = [m for m in all_resolved_matches if m.get("matched")]
    if all_resolved:
        await _run_searchdict_enrichment(all_resolved, conn)
    
    # ── Supabase Sync ──────────────────────────────────────
    if all_resolved or total_session_odds_count > 0:
        try:
            from Data.Access.sync_manager import SyncManager, TABLE_CONFIG
            manager = SyncManager()
            await manager._sync_table('fb_matches', TABLE_CONFIG['fb_matches'])
            await manager._sync_table('match_odds', TABLE_CONFIG['match_odds'])
            print(f"  [Sync] Complete: {len(all_resolved)} matches, {total_session_odds_count} odds outcomes.")
        except Exception as e:
            print(f"  [Sync] [Warning] Supabase push failed: {e}")

    # Session Summary
    method_counts = {
        "search_terms": sum(1 for m in all_resolved_matches if m.get("resolution_method") == "search_terms"),
        "fuzzy":        sum(1 for m in all_resolved_matches if m.get("resolution_method") == "fuzzy"),
        "llm":          sum(1 for m in all_resolved_matches if m.get("resolution_method") == "llm"),
        "failed":       sum(1 for m in all_resolved_matches if m.get("resolution_method") == "failed"),
    }
    resolved_count = method_counts["search_terms"] + method_counts["fuzzy"] + method_counts["llm"]

    print(f"\n    [Ch1 P1] -- Session Summary --------------------------")
    print(f"    [Ch1 P1] Fixtures processed  : {total_fixtures}")
    print(f"    [Ch1 P1] Leagues navigated   : {total_leagues}")
    print(f"    [Ch1 P1] Resolved            : {resolved_count}")
    print(f"    [Ch1 P1]   - search_terms    : {method_counts['search_terms']}")
    print(f"    [Ch1 P1]   - fuzzy           : {method_counts['fuzzy']}")
    print(f"    [Ch1 P1]   - llm             : {method_counts['llm']}")
    print(f"    [Ch1 P1] Unresolved          : {method_counts['failed']}")
    print(f"    [Ch1 P1] Odds outcomes       : {total_session_odds_count}")
    print(f"    [Ch1 P1] -------------------------------------------------\n")



# ── CHAPTER 2 PAGE 1 — Automated Booking (unchanged) ───────────────────

@AIGOSuite.aigo_retry(max_retries=2, delay=5.0)
async def run_automated_booking(playwright: Playwright):
    """
    Chapter 2 Page 1: Automated Booking.
    Reads harvested codes and places multi-bets. Does NOT harvest.
    """
    # ── Safety Guardrails ──
    from Core.System.guardrails import check_kill_switch, is_dry_run
    if check_kill_switch():
        print("  [KILL SWITCH] STOP_BETTING file detected. Aborting booking.")
        return
    if is_dry_run():
        print("  [DRY-RUN] Automated booking skipped (dry-run mode).")
        return

    print("\n--- Running Automated Booking (Chapter 2A) ---")

    from .fb_setup import get_pending_predictions_by_date
    predictions_by_date = await get_pending_predictions_by_date()
    if not predictions_by_date:
        return

    booking_queue = {}
    print("  [System] Building booking queue from registry...")
    from .fb_url_resolver import get_harvested_matches_for_date

    for target_date in sorted(predictions_by_date.keys()):
        harvested = await get_harvested_matches_for_date(target_date)
        if harvested:
            booking_queue[target_date] = harvested

    if not booking_queue:
        print("  [System] No harvested matches found for any pending dates. Exiting.")
        return

    max_restarts = 3
    restarts = 0

    while restarts <= max_restarts:
        context = None
        try:
            print(f"  [System] Launching Booking Session (Restart {restarts}/{max_restarts})...")
            context, page, current_balance = await _create_session(playwright)
            log_state(chapter="Chapter 2A", action="Placing bets")

            from .booker.placement import place_stairway_accumulator

            for target_date, harvested in booking_queue.items():
                print(f"\n--- Booking Date: {target_date} ---")
                await place_stairway_accumulator(page, harvested, current_balance)
                log_state(chapter="Chapter 2A", action="Booking Complete",
                          next_step=f"Processed {target_date}")

            break

        except Exception as e:
            is_fatal = "FatalSessionError" in str(type(e)) or "dirty" in str(e).lower()
            if is_fatal and restarts < max_restarts:
                print(f"\n[!!!] FATAL SESSION ERROR: {e}")
                restarts += 1
                if context:
                    await context.close()
                await asyncio.sleep(5)
                continue
            else:
                await log_error_state(None, "booking_fatal", e)
                print(f"  [CRITICAL] Booking failed: {e}")
                break
        finally:
            if context:
                try:
                    await context.close()
                    if hasattr(context, '_browser_ref'):
                        await context._browser_ref.close()
                except Exception:
                    pass


# Backward compat
async def run_football_com_booking(playwright: Playwright):
    """Legacy wrapper: runs both harvesting and booking sequentially."""
    await run_odds_harvesting(playwright)
    await run_automated_booking(playwright)
