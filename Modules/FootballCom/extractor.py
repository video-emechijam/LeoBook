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


async def _activate_and_wait_for_matches(page: Page) -> bool:
    """
    Triggers lazy-load hydration on football.com tournament pages
    by scrolling before waiting for match card selectors.

    Returns True if match cards found, False if page is genuinely empty or has no upcoming games.
    """
    # Phase 0: Check for "No upcoming games" message early
    NO_DATA_SELECTORS = [
        ".match-card-error-message",
        ".flex-column.no-data",
        ".match-cards-wrapper-adaptor:has-text('no upcoming games')",
    ]
    
    # Phase 1: Deep Hydration (Wait for Tabs & Initial Content)
    try:
        # Sometimes tabs are inside a skeleton or lazy-loaded
        for i in range(3):
            # Early exit if "No games" message appears
            for sel in NO_DATA_SELECTORS:
                if await page.locator(sel).count() > 0:
                    print(f"    [Extractor] Info: League page indicates no upcoming matches.")
                    return False

            tab_locators = page.locator("li.m-snap-nav-item")
            count = await tab_locators.count()
            if count > 0:
                # Try to switch to 'All' or 'Results' if we see 'Upcoming'
                for j in range(count):
                    tab = tab_locators.nth(j)
                    text = (await tab.inner_text()).lower()
                    if any(x in text for x in ["all", "result", "finish"]):
                        print(f"    [Extractor] Switching to '{text.strip()}' tab...")
                        await tab.click(force=True)
                        await asyncio.sleep(2.0)
                        break
                break
            else:
                await page.evaluate("window.scrollBy(0, 200)")
                await asyncio.sleep(0.8)
    except Exception:
        pass

    # Phase 2: Incremental scroll to trigger match card hydration
    try:
        print("    [Extractor] Scrolling to trigger match hydration...")
        scroll_positions = [400, 800, 1500]
        for pos in scroll_positions:
            await page.evaluate(f"window.scrollTo(0, {pos})")
            await asyncio.sleep(1)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.5)
    except Exception:
        pass

    # Phase 3: Now wait for cards to appear
    REAL_CARD_SELECTOR = "section.match-card:not(.skeleton), div.match-card:not(.skeleton), [class*='match-card']:not([class*='skeleton'])"
    
    for attempt in range(3):
        # Final check for "No games" before timing out
        for sel in NO_DATA_SELECTORS:
            if await page.locator(sel).count() > 0:
                return False

        try:
            # USER REQUEST: Wait for visible with 5s timeout
            await page.wait_for_selector(REAL_CARD_SELECTOR, state="visible", timeout=5000)
            
            has_text = await page.evaluate(f"""
                (sel) => {{
                    const cards = document.querySelectorAll(sel);
                    for (const c of cards) {{
                        if (c.innerText.length > 20) return true;
                    }}
                    return false;
                }}
            """, REAL_CARD_SELECTOR)
            
            if has_text:
                print(f"    [Extractor] Real match content verified.")
                # USER REQUEST: Extra delay before extraction
                await asyncio.sleep(2.0)
                return True
            else:
                await page.evaluate("window.scrollBy(0, 600)")
                await asyncio.sleep(1.0)
        except Exception:
            await page.evaluate("window.scrollBy(0, 600)")
            await asyncio.sleep(1.0)

    return False


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
async def extract_league_matches(page: Page, target_date: str, target_league_name: str = None, fb_url: str = None) -> List[Dict]:
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
        content_ready = await _activate_and_wait_for_matches(page)
        
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
        content_ready = await _activate_and_wait_for_matches(page)
        
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
    """Internal helper to JS-scrape matches from a container."""
    if hasattr(container, 'evaluate'):
        return await container.evaluate("""(args) => {
            const root = document;
            const { selectors, leagueText, targetDate } = args;
            const results = [];
            const cards = document.querySelectorAll(selectors.match_card_sel);
            cards.forEach(card => {
                const homeEl = card.querySelector(selectors.home_team_sel);
                const awayEl = card.querySelector(selectors.away_team_sel);
                const timeEl = card.querySelector(selectors.time_sel);
                const linkEl = card.querySelector(selectors.match_url_sel) || card.closest('a');
                if (homeEl && awayEl) {
                    results.push({
                        home: homeEl.innerText.trim(),
                        away: awayEl.innerText.trim(),
                        time: timeEl ? timeEl.innerText.trim() : "N/A",
                        league: leagueText,
                        url: linkEl ? linkEl.href : "",
                        date: targetDate
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
    return []


async def validate_match_data(matches: List[Dict]) -> List[Dict]:
    """Validate and clean extracted match data."""
    valid_matches = []
    for match in matches:
        if all(k in match for k in ['home', 'away', 'url', 'league']) and match['home'] and match['away'] and match['url']:
            valid_matches.append(match)
    print(f"  [Validation] {len(valid_matches)}/{len(matches)} valid.")
    return valid_matches
