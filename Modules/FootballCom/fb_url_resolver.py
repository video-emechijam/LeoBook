# fb_url_resolver.py: Handles resolution of matches between Flashscore and Football.com.
# Part of LeoBook Modules — Football.com
#
# Functions: get_harvested_matches_for_date(), resolve_urls_stable(), resolve_fixture_to_fb_match()
# Called by: Leo.py (Chapter 1 Page 1) | fb_manager.py

from typing import Dict, List, Optional
from playwright.async_api import Page
from playwright._impl._errors import TargetClosedError
from Data.Access.db_helpers import load_site_matches, get_site_match_id, save_site_matches


async def get_harvested_matches_for_date(target_date: str) -> list:
    """Retrieves matches for the date that have valid booking codes and haven't been booked yet."""
    site_matches = load_site_matches(target_date)
    harvested = [
        m for m in site_matches
        if m.get('booking_code') and m.get('booking_code') != 'N/A'
        and m.get('status') not in ('booked', 'placed')
    ]
    already_booked = sum(1 for m in site_matches if m.get('status') in ('booked', 'placed'))
    if already_booked:
        print(f"  [Registry] ⏭ {already_booked} already booked for {target_date} (skipped)")
    print(f"  [Registry] Found {len(harvested)} unbooked harvested codes for {target_date}.")
    return harvested


async def _get_fresh_page(context) -> Page:
    """
    Opens a new page from the existing browser context with the
    correct viewport. Closes any previous page if provided.
    Never touches the browser context itself — only pages.
    """
    page = await context.new_page()
    from .navigator import MOBILE_VIEWPORT
    await page.set_viewport_size(MOBILE_VIEWPORT)
    return page


def resolve_fixture_to_fb_match(
    fixture: Dict,
    page_matches: List[Dict],
    league_id: str,
    matcher,
    threshold: int = 80,
) -> Optional[Dict]:
    """
    Thin wrapper: fuzzy-match one FS fixture against a list
    of football.com page match results.
    Returns a fb_matches-compatible dict or None.
    Calls match_resolver.py logic — no duplicate fuzzy code.

    Uses GrokMatcher._fuzzy_resolve() (sync) to avoid LLM costs
    for bulk resolution. The async .resolve() path with LLM fallback
    is reserved for difficult cases in the booking pipeline.
    """
    home = (fixture.get("home_team_name") or "").strip()
    away = (fixture.get("away_team_name") or "").strip()
    fixture_id = fixture.get("fixture_id", "")
    date = fixture.get("date", "")

    if not home or not away:
        return None

    fs_name = f"{home} vs {away}".lower()

    # Normalize page_matches keys: extractor returns 'home'/'away',
    # but GrokMatcher._fuzzy_resolve reads 'home_team'/'away_team'.
    normalized = []
    for pm in page_matches:
        nm = dict(pm)
        if "home" in nm and "home_team" not in nm:
            nm["home_team"] = nm["home"]
        if "away" in nm and "away_team" not in nm:
            nm["away_team"] = nm["away"]
        normalized.append(nm)

    # Call existing GrokMatcher fuzzy logic (sync — no LLM API call)
    best_match, score = matcher._fuzzy_resolve(fs_name, normalized)

    if best_match and score >= threshold:
        fb_home = best_match.get("home", best_match.get("home_team", ""))
        fb_away = best_match.get("away", best_match.get("away_team", ""))
        site_id = get_site_match_id(date, fb_home, fb_away)

        return {
            "date": date,
            "time": best_match.get("time", "N/A"),
            "home": fb_home,
            "away": fb_away,
            "league": best_match.get("league", ""),
            "url": best_match.get("url", ""),
            "fixture_id": fixture_id,
            "matched": f"{home} vs {away}",
            "status": "pending",
            "site_match_id": site_id,
        }

    return None


async def resolve_urls_stable(page: Page, leagues_to_process: dict, target_date: str):
    """
    STABLE URL RESOLVER:
    Outer loop iterates over leagues (O(leagues) navigations).
    Inner loop fuzzy-matches each FS fixture against page results.
    Saves each resolved fixture to SQLite immediately.
    Extracts odds for each resolved fixture (sequential per match).
    Pushes fb_matches + match_odds to Supabase after the outer loop exits.
    Recycles the Page object after each league to release Chrome memory.
    Catches TargetClosedError per-league to prevent session crashes.
    """
    from .extractor import extract_league_matches, validate_match_data
    from .match_resolver import GrokMatcher
    from .odds_extractor import OddsExtractor
    from Data.Access.league_db import get_fb_url_for_league, get_connection
    import asyncio

    context = page.context
    current_page = page
    matcher = GrokMatcher()
    conn = get_connection()

    # Metrics Tracking
    direct_nav_count = 0
    resolved_count = 0
    unresolved_count = 0
    session_odds_count = 0
    session_match_rows: List[Dict] = []
    remaining_leagues = []
    day_fs_matches = [m for sublist in leagues_to_process.values() for m in sublist]
    total_fixtures = len(day_fs_matches)
    total_leagues = len(leagues_to_process)

    print(f"  [URL Resolver] {total_fixtures} fixtures across "
          f"{total_leagues} unique leagues — {total_leagues} page "
          f"loads required (was {total_fixtures})")

    for l_id, fs_matches in list(leagues_to_process.items()):
        fb_url = get_fb_url_for_league(None, l_id)
        league_name = fs_matches[0].get("region_league", l_id) if fs_matches else l_id
        n = len(fs_matches)

        if fb_url:
            direct_nav_count += 1
            print(f"\n  [League] {league_name} ({n} fixture(s)) → {fb_url}")
            try:
                # FIX 1 integration: extraction now handles its own navigation
                league_matches = await extract_league_matches(
                    current_page, target_date, fb_url=fb_url
                )

                if not league_matches:
                    print(f"    [League] {league_name}: no matches on page (empty or off-season)")
                else:
                    league_matches = await validate_match_data(league_matches)

                    # ── GAP 1: Match each FS fixture against fb page results (in memory) ──
                    for fixture in fs_matches:
                        home = (fixture.get("home_team_name") or "").strip()
                        away = (fixture.get("away_team_name") or "").strip()
                        f_id = fixture.get("fixture_id")

                        match_row = resolve_fixture_to_fb_match(
                            fixture, league_matches, l_id, matcher
                        )

                        if match_row:
                            # GAP 2: IMMEDIATE save — one fixture at a time
                            save_site_matches([match_row])
                            resolved_count += 1
                            session_match_rows.append(match_row)
                            print(f"    [Match] ✓ {home} vs {away} "
                                  f"→ {match_row.get('url', '?')[:60]}")

                            # ── PART 3: Odds extraction ────────────────────
                            match_url = match_row.get("url", "")
                            site_match_id = match_row.get("site_match_id", "")

                            if match_url:
                                try:
                                    odds_page = await _get_fresh_page(context)
                                    try:
                                        await odds_page.goto(
                                            match_url,
                                            wait_until="domcontentloaded",
                                            timeout=25000,
                                        )
                                        await asyncio.sleep(1.5)  # JS settle

                                        extractor = OddsExtractor(odds_page, conn)
                                        result = await extractor.extract(
                                            f_id, site_match_id
                                        )

                                        print(
                                            f"    [Odds]  {f_id} → "
                                            f"{result.markets_found} markets, "
                                            f"{result.outcomes_extracted} outcomes "
                                            f"({result.duration_ms}ms)"
                                        )
                                        session_odds_count += result.outcomes_extracted

                                    finally:
                                        try:
                                            await odds_page.close()
                                        except Exception:
                                            pass

                                except RuntimeError:
                                    raise  # login guard — fatal
                                except Exception as e:
                                    print(f"    [Odds]  ERROR {f_id}: {e}")
                            else:
                                print(f"    [Odds]  SKIP {f_id}: no match URL")

                        else:
                            unresolved_count += 1
                            print(f"    [Match] ✗ {home} vs {away} (no fb match)")

                # Recycle page between leagues to release Chrome memory
                try:
                    await current_page.close()
                except Exception:
                    pass
                current_page = await _get_fresh_page(context)
                await asyncio.sleep(1.0)

            except TargetClosedError:
                print(f"    [URL Resolver] Page closed during {l_id} — opening fresh page")
                try:
                    await current_page.close()
                except Exception:
                    pass
                current_page = await _get_fresh_page(context)
                await asyncio.sleep(2.0)
                continue
            except Exception as e:
                print(f"    [URL Resolver] Nav/Extract FAIL for {l_id}: {e}")
        else:
            remaining_leagues.append(l_id)

    # ── PART 4: Post-session Supabase sync (fb_matches + match_odds) ───────
    if session_match_rows or session_odds_count > 0:
        try:
            from Data.Access.sync_manager import SyncManager, TABLE_CONFIG
            manager = SyncManager()
            await manager._sync_table('fb_matches', TABLE_CONFIG['fb_matches'])
            await manager._sync_table('match_odds', TABLE_CONFIG['match_odds'])
            print(
                f"  [Sync] Ch1 P1 complete: "
                f"{resolved_count} fb_matches, "
                f"{session_odds_count} odds outcomes pushed to Supabase"
            )
        except Exception as e:
            print(f"  [Sync] [Warning] Supabase push failed: {e}")

    # Session Summary
    print(f"\n    [URL Resolver] ── Session Summary ────────────────────────────")
    print(f"    [URL Resolver] Upcoming fixtures (7-day)   : {total_fixtures}")
    print(f"    [URL Resolver] Unique leagues              : {total_leagues}")
    print(f"    [URL Resolver] Page navigations            : {direct_nav_count} (was {total_fixtures})")
    if total_fixtures:
        print(f"    [URL Resolver] Fixtures resolved           : {resolved_count} ({resolved_count/total_fixtures*100:.1f}%)")
    print(f"    [URL Resolver] Fixtures unresolved         : {unresolved_count}")
    print(f"    [URL Resolver] Odds outcomes extracted      : {session_odds_count}")
    print(f"    [URL Resolver] Unmapped leagues (no fb_url): {len(remaining_leagues)}")
    print(f"    [URL Resolver] SQLite saves (immediate)    : {len(session_match_rows)}")
    print(f"    [URL Resolver] Supabase sync               : {'fb_matches + match_odds pushed' if session_match_rows else 'nothing to push'}")
    print(f"    [URL Resolver] ─────────────────────────────────────────────\n")

    return current_page
