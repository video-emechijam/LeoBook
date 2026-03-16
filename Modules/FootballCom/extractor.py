# extractor.py: Schedule scraper for Football.com.
# Part of LeoBook Modules — Football.com
#
# Functions: extract_league_matches(), validate_match_data()

"""
Extractor Module
Handles extraction of leagues and matches from Football.com schedule pages.
"""

import asyncio
from typing import List, Dict

from playwright.async_api import Page

from Core.Intelligence.selector_manager import SelectorManager

from Core.Utils.constants import WAIT_FOR_LOAD_STATE_TIMEOUT
from .navigator import hide_overlays
from Core.Intelligence.aigo_suite import AIGOSuite


async def _activate_and_wait_for_matches(
    page: Page,
    expected_count: int = 0,
) -> bool:
    """
    Hydrates a football.com tournament page and waits for match cards.
    Uses the proven stability-polling scroll from fs_league_hydration
    (RULEBOOK §2.16 — Reuse First) instead of fixed-time budgets.

    Returns True if cards found, False if page is genuinely empty.
    """
    from Modules.Flashscore.fs_league_hydration import _scroll_to_load

    # Football.com match card selector (non-skeleton only)
    CARD_SEL = (
        "section.match-card:not(.skeleton), "
        "div.match-card:not(.skeleton), "
        "[class*='match-card']:not([class*='skeleton'])"
    )

    # Phase 0: early exit — "No upcoming games" indicators
    NO_DATA_SELECTORS = [
        ".match-card-error-message",
        ".flex-column.no-data",
        ".match-cards-wrapper-adaptor:has-text('no upcoming games')",
    ]
    for sel in NO_DATA_SELECTORS:
        try:
            if await page.locator(sel).count() > 0:
                print("    [Extractor] Info: League page indicates no upcoming matches.")
                return False
        except Exception:
            pass

    # Phase 1: tab switch — activate "All" tab if present
    try:
        tab_locators = page.locator("li.m-snap-nav-item")
        count = await tab_locators.count()
        for j in range(count):
            tab = tab_locators.nth(j)
            text = (await tab.inner_text()).lower()
            if any(x in text for x in ["all", "result", "finish"]):
                await tab.click(force=True)
                await asyncio.sleep(1.5)
                break
    except Exception:
        pass

    # Phase 2: stability-polling scroll (proven, reused from fs_league_hydration)
    # _scroll_to_load scrolls one full viewport per step, polls every 0.4s,
    # stops when count is stable for 2s or DOM bottom reached.
    found = await _scroll_to_load(page, CARD_SEL)

    # Phase 3: result
    if expected_count > 0 and found < expected_count:
        print(
            f"    [Extractor] Partial hydration: "
            f"{found}/{expected_count} cards — proceeding."
        )
    elif found == 0:
        # Re-check for "no games" message (may have loaded after scroll)
        for sel in NO_DATA_SELECTORS:
            try:
                if await page.locator(sel).count() > 0:
                    print("    [Extractor] Info: League page indicates no upcoming matches.")
                    return False
            except Exception:
                pass
        return False
    else:
        print(
            f"    [Extractor] {found}/{expected_count if expected_count else '?'} "
            f"cards hydrated."
        )
    return True


async def dismiss_overlays(page: Page) -> int:
    """Attempts to dismiss common overlays on football.com."""
    dismissed = 0
    OVERLAY_SELECTORS = [
        "button[id*='accept']", "button[class*='accept']", "button[class*='cookie']",
        ".overlay-close", ".popup-close", "button[class*='close']"
    ]
    for selector in OVERLAY_SELECTORS:
        try:
            el = await page.query_selector(selector)
            if el and await el.is_visible():
                await el.click(timeout=1000)
                dismissed += 1
                await asyncio.sleep(0.2)
        except Exception: pass
    return dismissed


@AIGOSuite.aigo_retry(max_retries=2, delay=2.0, context_key="fb_schedule_page", element_key="league_section")
async def extract_league_matches(page: Page, target_date: str = None, target_league_name: str = None, fb_url: str = None, expected_count: int = 0) -> List[Dict]:
    """Iterates leagues and extracts matches with AIGO protection and hydration support."""
    if fb_url:
        print(f"    [Extractor] Navigating to {fb_url}...")
        await page.goto(fb_url, wait_until='domcontentloaded', timeout=30000)
    
    current_url = page.url
    print(f"  [Harvest] Sequence for {target_league_name or 'league'} -> {current_url}")

    is_tournament_page = "sr:tournament:" in current_url or "/sport/football/sr:category:" in current_url

    # Selectors
    league_section_sel = SelectorManager.get_selector_strict("fb_schedule_page", "league_section")
    match_card_sel = SelectorManager.get_selector_strict("fb_schedule_page", "match_rows")
    match_url_sel = SelectorManager.get_selector_strict("fb_schedule_page", "match_url")
    league_title_sel = SelectorManager.get_selector_strict("fb_schedule_page", "league_title_link")
    home_team_sel = SelectorManager.get_selector_strict("fb_schedule_page", "match_row_home_team_name")
    away_team_sel = SelectorManager.get_selector_strict("fb_schedule_page", "match_row_away_team_name")
    time_sel = SelectorManager.get_selector_strict("fb_schedule_page", "match_row_time")
    collapsed_icon_sel = SelectorManager.get_selector_strict("fb_schedule_page", "league_expand_icon_collapsed")

    all_matches = []
    
    if is_tournament_page:
        print(f"    [Mode] Direct Tournament Page")
        await dismiss_overlays(page)
        content_ready = await _activate_and_wait_for_matches(
            page, expected_count=expected_count
        )

        if not content_ready:
            return []

        # Use flexible common selectors for cards
        MATCH_CARD_SELECTORS = ["section.match-card", "div.match-card", "[class*='match-card']", "[data-match-id]"]
        discovered_selector = None
        for sel in MATCH_CARD_SELECTORS:
            if await page.locator(sel).count() > 0:
                discovered_selector = sel
                break
        
        if discovered_selector:
            all_matches = await _extract_matches_from_container(
                page, discovered_selector, home_team_sel, away_team_sel,
                time_sel, match_url_sel, target_league_name or "Tournament Matches", target_date
            )
            
    else:
        print(f"    [Mode] Global Schedule Page")
        await dismiss_overlays(page)
        content_ready = await _activate_and_wait_for_matches(
            page, expected_count=expected_count
        )
        
        if not content_ready:
            return []

        league_headers = await page.locator(league_section_sel).all()
        if league_headers:
            for i, header_locator in enumerate(league_headers):
                league_element = header_locator.locator(league_title_sel).first
                league_text = (await league_element.inner_text()).strip().replace('\n', ' - ') if await league_element.count() > 0 else f"Unknown {i+1}"

                if league_text.startswith("Simulated Reality"): continue
                if target_league_name and target_league_name.lower() not in league_text.lower(): continue

                if await header_locator.locator(collapsed_icon_sel).count() > 0:
                    await header_locator.click(force=True)
                    await asyncio.sleep(1.0)

                matches_container = await header_locator.evaluate_handle('(el) => el.nextElementSibling')
                if matches_container:
                    matches_in_section = await _extract_matches_from_container(
                        matches_container, match_card_sel, home_team_sel, away_team_sel,
                        time_sel, match_url_sel, league_text, target_date
                    )
                    if matches_in_section: all_matches.extend(matches_in_section)
        
        if not all_matches:
            # Fallback direct scan
            all_matches = await _extract_matches_from_container(
                page, match_card_sel, home_team_sel, away_team_sel,
                time_sel, match_url_sel, target_league_name or "Unknown League", target_date
            )

    print(f"  [Harvest] Total: {len(all_matches)}")
    return all_matches


async def _extract_matches_from_container(container, match_card_sel, home_team_sel, away_team_sel, time_sel, match_url_sel, league_text, target_date):
    """Internal helper to JS-scrape matches from a container.

    FIX: The original evaluate() call used `document.querySelectorAll` unconditionally,
    meaning the JS function always queried the full page document rather than the
    scoped container element. In the Global Schedule path, this caused matches from
    other league sections to bleed into every section's results.

    The fix passes a scoped root reference into the JS:
    - For a Playwright Page object: root = document (correct, page-wide query is intentional).
    - For a Playwright JSHandle (nextElementSibling result): root = the handle element itself,
      so querySelectorAll is scoped to that section only.
    """
    if not hasattr(container, 'evaluate'):
        return []

    # Determine if container is a Page (use document) or an ElementHandle/JSHandle (use element).
    # Page objects have a .url attribute; ElementHandles do not.
    is_page = hasattr(container, 'url')

    if is_page:
        # Tournament / full-page path: query the entire document.
        return await container.evaluate(r"""(args) => {
            const { selectors, leagueText, targetDate } = args;
            const results = [];
            const cards = document.querySelectorAll(selectors.match_card_sel);
            cards.forEach(card => {
                const homeEl = card.querySelector(selectors.home_team_sel);
                const awayEl = card.querySelector(selectors.away_team_sel);
                // BUG2 FIX: fallback selector chain — return null if no time element found
                const timeEl = card.querySelector(selectors.time_sel)
                    || card.querySelector('.match-time')
                    || card.querySelector('.ko-time')
                    || card.querySelector('.fixture-time')
                    || card.querySelector('.start-time')
                    || card.querySelector('[class*="time"]:not([class*="team"]):not([class*="overtime"])')
                    || card.querySelector('[data-time]');
                const linkEl = card.querySelector(selectors.match_url_sel) || card.closest('a');
                if (homeEl && awayEl) {
                    const dateEl = card.querySelector(
                        '[data-date], [class*="match-date"], '
                        + '[class*="event-date"], [class*="matchdate"], '
                        + '[class*="date-label"]'
                    );
                    let cardDate = dateEl
                        ? (dateEl.dataset.date || dateEl.innerText.trim())
                        : targetDate;
                    if (cardDate && !/^\d{4}-\d{2}-\d{2}$/.test(cardDate)) {
                        cardDate = targetDate;
                    }
                    const rawTime = timeEl ? timeEl.innerText.trim() : null;
                    results.push({
                        home: homeEl.innerText.trim(),
                        away: awayEl.innerText.trim(),
                        time: rawTime,
                        league: leagueText,
                        url: linkEl ? linkEl.href : "",
                        date: cardDate
                    });
                }
            });
            return results;
        }""", {
            "selectors": {
                "match_card_sel": match_card_sel, "match_url_sel": match_url_sel,
                "home_team_sel": home_team_sel, "away_team_sel": away_team_sel, "time_sel": time_sel
            },
            "leagueText": league_text,
            "targetDate": target_date
        })
    else:
        # Global schedule path: container is a JSHandle for a specific league section.
        # FIX: evaluate() on a JSHandle passes the element as the first JS argument.
        # Use `element.querySelectorAll` to scope to this section only, preventing
        # cross-section bleed.
        return await container.evaluate(r"""(element, args) => {
            const { selectors, leagueText, targetDate } = args;
            const results = [];
            const cards = element.querySelectorAll(selectors.match_card_sel);
            cards.forEach(card => {
                const homeEl = card.querySelector(selectors.home_team_sel);
                const awayEl = card.querySelector(selectors.away_team_sel);
                // BUG2 FIX: fallback selector chain — return null if no time element found
                const timeEl = card.querySelector(selectors.time_sel)
                    || card.querySelector('.match-time')
                    || card.querySelector('.ko-time')
                    || card.querySelector('.fixture-time')
                    || card.querySelector('.start-time')
                    || card.querySelector('[class*="time"]:not([class*="team"]):not([class*="overtime"])')
                    || card.querySelector('[data-time]');
                const linkEl = card.querySelector(selectors.match_url_sel) || card.closest('a');
                if (homeEl && awayEl) {
                    const dateEl = card.querySelector(
                        '[data-date], [class*="match-date"], '
                        + '[class*="event-date"], [class*="matchdate"], '
                        + '[class*="date-label"]'
                    );
                    let cardDate = dateEl
                        ? (dateEl.dataset.date || dateEl.innerText.trim())
                        : targetDate;
                    if (cardDate && !/^\d{4}-\d{2}-\d{2}$/.test(cardDate)) {
                        cardDate = targetDate;
                    }
                    const rawTime = timeEl ? timeEl.innerText.trim() : null;
                    results.push({
                        home: homeEl.innerText.trim(),
                        away: awayEl.innerText.trim(),
                        time: rawTime,
                        league: leagueText,
                        url: linkEl ? linkEl.href : "",
                        date: cardDate
                    });
                }
            });
            return results;
        }""", {
            "selectors": {
                "match_card_sel": match_card_sel, "match_url_sel": match_url_sel,
                "home_team_sel": home_team_sel, "away_team_sel": away_team_sel, "time_sel": time_sel
            },
            "leagueText": league_text,
            "targetDate": target_date
        })


async def validate_match_data(matches: List[Dict]) -> List[Dict]:
    """Validate and clean extracted match data.
    Only requires home+away team names — url is optional (needed for odds
    extraction but not for resolution pairing; many cards on tournament pages
    have no href at the list level).

    BUG2 FIX: Normalise time field — strip literal 'N/A' and empty strings to None.
    Emit a warning log if significant portion of cards have no time element.
    """
    valid_matches = []
    missing_time = 0
    for match in matches:
        if match.get('home') and match.get('away'):
            # Normalise time: N/A / empty string → None so downstream can distinguish
            raw_time = match.get('time')
            if not raw_time or str(raw_time).strip().upper() in ('N/A', 'NONE', '-', ''):
                match['time'] = None
                missing_time += 1
            valid_matches.append(match)
    print(f"  [Validation] {len(valid_matches)}/{len(matches)} valid.")
    if missing_time:
        print(
            f"  [Time] WARNING: {missing_time}/{len(valid_matches)} cards have no time element "
            f"— selector chain exhausted. Check .header .time on current football.com build."
        )
    return valid_matches
