# odds_extractor.py: Extracts all ranked market-outcome odds for
#                    a single match from football.com. No-login only.
# Part of LeoBook Modules — FootballCom
#
# v5.1 (2026-03-17): + match date/time extraction from header.
# v5.0 (2026-03-17): Intro dialog dismissal + recursive scroll + recursive
#   expand + knowledge.json selectors + ranked_markets extraction + debug
#   screenshots on failure.
#
# Functions: OddsExtractor.extract(), _assert_no_login(),
#            _parse_line(), _load_market_catalogue()
# Called by: fb_manager._odds_worker()

import asyncio
import json
import re
import time
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional

from playwright.async_api import Page

from Core.Utils.constants import now_ng
from Core.Intelligence.selector_manager import SelectorManager
from Data.Access.league_db import upsert_match_odds_batch


# ── Market Catalogue (loaded once at import) ──────────────────────────────

def _load_market_catalogue() -> List[Dict]:
    path = Path(__file__).parent.parent.parent / \
        "Data" / "Store" / "ranked_markets_likelihood_updated_with_team_ou.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("ranked_market_outcomes", [])
    except Exception as e:
        print(f"  [OddsExtractor] Failed to load market catalogue: {e}")
        return []


_MARKET_CATALOGUE: List[Dict] = _load_market_catalogue()


# ── Result dataclass ──────────────────────────────────────────────────────

@dataclass
class OddsResult:
    fixture_id: str
    site_match_id: str
    markets_found: int
    outcomes_extracted: int
    duration_ms: int
    error: Optional[str] = None
    match_date: Optional[str] = None   # YYYY-MM-DD from page header
    match_time: Optional[str] = None   # HH:MM from page header


# ── Helpers ───────────────────────────────────────────────────────────────

def _sel(key: str) -> str:
    """Shorthand to get a fb_match_page selector from knowledge.json."""
    return SelectorManager.get_selector("fb_match_page", key)


async def _safe_screenshot(page: Page, fixture_id: str, tag: str = "") -> None:
    """Take a debug screenshot (swallowed on error)."""
    try:
        name = f"debug_odds_fail_{fixture_id}_{tag}_{int(time.time())}.png"
        await page.screenshot(path=name)
        print(f"    [Debug] Screenshot saved: {name}")
    except Exception:
        pass


def _parse_fb_date(raw: str) -> Optional[str]:
    """Parse football.com header date like '17 Mar, Tuesday' into 'YYYY-MM-DD'.
    Falls back to None if parsing fails."""
    from datetime import datetime
    # Remove day-of-week part: '17 Mar, Tuesday' -> '17 Mar'
    cleaned = raw.split(',')[0].strip() if ',' in raw else raw.strip()
    now_year = datetime.now().year
    for fmt in ("%d %b", "%d %B", "%b %d", "%B %d"):
        try:
            parsed = datetime.strptime(cleaned, fmt).replace(year=now_year)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ── Intro Dialog Dismissal ────────────────────────────────────────────────

async def _dismiss_intro_dialog(page: Page) -> None:
    """
    Football.com match pages show a multi-step intro dialog on first visit.
    Step 1: Click "Next" button (may appear multiple times).
    Step 2: Click "GOT IT!" / "Got it" button.
    Uses knowledge.json selectors + hardcoded fallbacks.
    """
    NEXT_SELECTORS = [
        _sel("intro_dialog_btn") or ".intro-dialog button",
        'span[data-cms-key="next"]',
        'button:has-text("Next")',
        'span:has-text("Next")',
    ]
    CONFIRM_SELECTORS = [
        'span[data-cms-key="confirm_btn_text"]',
        'button:has-text("GOT IT!")',
        'button:has-text("Got it")',
        'button:has-text("OK")',
        'span:has-text("GOT IT!")',
        'span:has-text("Got it")',
    ]

    # Click "Next" up to 5 times (some tours have multiple steps)
    for _ in range(5):
        clicked = False
        for sel in NEXT_SELECTORS:
            if not sel:
                continue
            try:
                loc = page.locator(sel).first
                if await loc.is_visible(timeout=800):
                    await loc.click(force=True)
                    clicked = True
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                continue
        if not clicked:
            break

    # Click "GOT IT!" / confirm
    for sel in CONFIRM_SELECTORS:
        try:
            loc = page.locator(sel).first
            if await loc.is_visible(timeout=800):
                await loc.click(force=True)
                await asyncio.sleep(0.4)
                print("    [Dialog] Intro dialog dismissed.")
                return
        except Exception:
            continue


# ── Recursive Scroll ──────────────────────────────────────────────────────

async def _recursive_scroll_markets(page: Page) -> int:
    """
    Scrolls to load ALL [data-market-id] containers.
    Stops only after 3 consecutive scrolls add zero new containers.
    Returns: total count of containers found.
    """
    MARKET_CONTAINER_SEL = _sel("market_items") or "[data-market-id]"
    no_growth_count = 0
    prev_count = 0

    for scroll_round in range(30):  # safety cap
        count = await page.locator(MARKET_CONTAINER_SEL).count()

        if count > prev_count:
            no_growth_count = 0
            prev_count = count
        else:
            no_growth_count += 1

        if no_growth_count >= 3:
            break

        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await asyncio.sleep(0.8)

    return prev_count


# ── Recursive Expand ──────────────────────────────────────────────────────

async def _expand_all_markets(page: Page) -> int:
    """
    Expands every collapsed market container on the page.
    Uses knowledge.json fb_match_page.market_toggle_icon + market_header.
    Returns: count of containers expanded.
    """
    toggle_icon_sel = _sel("market_toggle_icon") or ".m-market-title .toggle-all-icon"
    market_header_sel = _sel("market_header") or ".market-title-bar, .m-market-title"
    row_sel = ".m-table-row"

    expanded_count = 0
    containers = await page.locator("[data-market-id]").all()

    for container in containers:
        try:
            # Detect collapsed: no visible rows OR aria-expanded=false
            is_collapsed = await container.evaluate("""(el) => {
                const hdr = el.querySelector('[aria-expanded="false"], .collapsed, [class*="collapsed"]');
                if (hdr) return true;
                const rows = el.querySelectorAll('.m-table-row');
                if (rows.length === 0) return true;
                const first = rows[0];
                const style = window.getComputedStyle(first);
                return style.display === 'none' || style.visibility === 'hidden'
                    || parseFloat(style.height || '1') < 1;
            }""")

            if not is_collapsed:
                continue

            # Try clicking the toggle icon first, then the header
            clicked = False
            for sel in [toggle_icon_sel, market_header_sel, "[data-toggle]", "[aria-expanded]"]:
                try:
                    toggle = container.locator(sel).first
                    if await toggle.count() > 0:
                        await toggle.click(force=True)
                        clicked = True
                        break
                except Exception:
                    continue

            if not clicked:
                # Last resort: click the container itself
                try:
                    await container.click(force=True)
                except Exception:
                    await container.evaluate("el => el.click()")

            # Wait for rows to appear
            market_id = await container.get_attribute("data-market-id") or ""
            try:
                await page.wait_for_selector(
                    f"[data-market-id='{market_id}'] {row_sel}",
                    state="visible", timeout=1500,
                )
            except Exception:
                pass

            await asyncio.sleep(0.15)
            expanded_count += 1

        except Exception:
            continue

    return expanded_count


# ── Extractor ─────────────────────────────────────────────────────────────

class OddsExtractor:
    """
    Extracts all ranked market odds from a football.com match detail page.
    Page MUST already be navigated — extract() never calls page.goto().
    Saves each market batch to SQLite immediately after extraction.

    v5.0 pipeline per match:
      1. Dismiss intro dialog
      2. Recursive scroll until no new [data-market-id] containers
      3. Expand ALL collapsed markets
      4. Extract outcomes from each ranked market
      5. Screenshot + log on zero outcomes
    """

    def __init__(self, page: Page, conn: sqlite3.Connection) -> None:
        self.page = page
        self.conn = conn

    # ── No-login guard ────────────────────────────────────────────────

    @staticmethod
    async def _assert_no_login(page: Page) -> None:
        LOGIN_INDICATORS = [
            ".user-account", ".user-balance", ".logout-btn",
            "[data-test='user-menu']", "[class*='user-logged']",
            "[class*='account-balance']", ".m-account-info",
            ".m-user-panel", "a[href*='logout']",
        ]
        for sel in LOGIN_INDICATORS:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    raise RuntimeError(
                        "OddsExtractor: active login session detected. "
                        "Odds extraction must run without login."
                    )
            except RuntimeError:
                raise
            except Exception:
                continue

    # ── Line parser ───────────────────────────────────────────────────

    @staticmethod
    def _parse_line(text: str) -> Optional[str]:
        """Extract numeric line from outcome label.
        'Over 2.5' → '2.5', 'Under 1.5' → '1.5', 'Home' → None"""
        m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
        return m.group() if m else None

    # ── Main extraction ───────────────────────────────────────────────

    async def extract(
        self,
        fixture_id: str,
        site_match_id: str,
    ) -> OddsResult:
        """
        Page is ALREADY navigated to the match detail URL.
        Do NOT call page.goto() inside this method.

        Pipeline:
          1. Dismiss intro dialog (Next → GOT IT!)
          2. Recursive scroll all [data-market-id]
          3. Expand all collapsed markets
          4. Iterate ranked_markets catalogue and extract outcomes
          5. Immediate batch save per market
        """
        start = time.monotonic()
        markets_found = 0
        outcomes_written = 0

        # Selectors from knowledge.json
        outcome_row_sel = _sel("market_outcome_row") or ".m-table-row.un-gap-4"
        outcome_label_sel = (
            _sel("market_outcome_label")
            or "span.un-text-rem-\\[12px\\], span[class*='un-text-rem'][class*='12']"
        )
        outcome_odds_sel = (
            _sel("market_outcome_odds")
            or "span.un-text-rem-\\[14px\\].un-font-bold, span.un-font-bold[class*='un-text-rem']"
        )

        # Date/time from match header
        extracted_date: Optional[str] = None
        extracted_time: Optional[str] = None

        try:
            await self._assert_no_login(self.page)

            # ── Step 0: Extract match date + time from header ──
            try:
                date_sel = _sel("match_detail_date") or ".estimate-start-time .date"
                time_sel_hdr = _sel("match_detail_time_elapsed") or ".estimate-start-time .time"

                date_el = self.page.locator(date_sel).first
                time_el = self.page.locator(time_sel_hdr).first

                if await date_el.count() > 0:
                    raw_date = (await date_el.inner_text()).strip()
                    # Parse "17 Mar, Tuesday" → "2026-03-17"
                    extracted_date = _parse_fb_date(raw_date)
                    print(f"    [Odds] {fixture_id}: date from header = {raw_date} → {extracted_date}")

                if await time_el.count() > 0:
                    extracted_time = (await time_el.inner_text()).strip()
                    print(f"    [Odds] {fixture_id}: time from header = {extracted_time}")
            except Exception as dt_err:
                print(f"    [Odds] {fixture_id}: date/time extraction skipped: {dt_err}")

            # ── Step 1: Dismiss intro dialog ──
            await _dismiss_intro_dialog(self.page)

            # ── Step 2: Recursive scroll to hydrate all market containers ──
            containers_found = await _recursive_scroll_markets(self.page)
            print(f"    [Odds] {fixture_id}: {containers_found} market containers loaded")

            # ── Step 3: Expand ALL collapsed markets ──
            expanded = await _expand_all_markets(self.page)
            if expanded:
                print(f"    [Odds] {fixture_id}: expanded {expanded} collapsed markets")

            # ── Step 4: Extract from ranked_markets catalogue ──
            seen_market_ids: set = set()

            for market in _MARKET_CATALOGUE:
                market_id = str(market.get("market_id", ""))
                base_market = market.get("base_market", "")
                category = market.get("category", "")
                likelihood = market.get("likelihood_percent", 0)
                rank = market.get("rank", 0)

                if not market_id or market_id in seen_market_ids:
                    continue

                # Find the container on the page
                container = await self.page.query_selector(
                    f"[data-market-id='{market_id}']"
                )
                if not container:
                    continue

                seen_market_ids.add(market_id)
                markets_found += 1

                # Extract outcome rows
                outcome_rows = await container.query_selector_all(
                    outcome_row_sel.replace("\\[", "[").replace("\\]", "]")
                    if "\\" in outcome_row_sel else outcome_row_sel
                )

                # Fallback: try broader selector if specific one got 0 rows
                if not outcome_rows:
                    outcome_rows = await container.query_selector_all(".m-table-row")

                batch: List[Dict] = []
                extracted_at = now_ng().isoformat()

                for row in outcome_rows:
                    try:
                        name_el = await row.query_selector(
                            "span.un-text-rem-\\[12px\\], "
                            "span[class*='un-text-rem'][class*='12']"
                        )
                        odds_el = await row.query_selector(
                            "span.un-text-rem-\\[14px\\].un-font-bold, "
                            "span.un-font-bold[class*='un-text-rem']"
                        )
                        if not name_el or not odds_el:
                            continue

                        name_text = (await name_el.inner_text()).strip()
                        odds_text = (await odds_el.inner_text()).strip()

                        try:
                            odds_val = float(odds_text.replace(",", "."))
                        except ValueError:
                            continue
                        if odds_val <= 1.0:
                            continue

                        batch.append({
                            "fixture_id": fixture_id,
                            "site_match_id": site_match_id,
                            "market_id": market_id,
                            "base_market": base_market,
                            "category": category,
                            "exact_outcome": name_text,
                            "line": self._parse_line(name_text),
                            "odds_value": odds_val,
                            "likelihood_pct": likelihood,
                            "rank_in_list": rank,
                            "extracted_at": extracted_at,
                        })
                    except Exception:
                        continue

                # IMMEDIATE save after each market
                if batch:
                    written = upsert_match_odds_batch(self.conn, batch)
                    outcomes_written += written

            # ── Step 5: Debug screenshot on zero outcomes ──
            if outcomes_written == 0 and markets_found > 0:
                print(
                    f"    [Odds] WARNING: {fixture_id} — {markets_found} markets found "
                    f"but 0 outcomes extracted. Saving debug screenshot."
                )
                await _safe_screenshot(self.page, fixture_id, "zero_outcomes")

        except RuntimeError:
            raise  # login guard — fatal, re-raise
        except Exception as e:
            elapsed = int((time.monotonic() - start) * 1000)
            is_context_closed = "closed" in str(e).lower()
            tag = "context_closed" if is_context_closed else "error"
            print(f"    [ODDS ERROR] {fixture_id}: {e}")
            await _safe_screenshot(self.page, fixture_id, tag)
            return OddsResult(
                fixture_id=fixture_id,
                site_match_id=site_match_id,
                markets_found=markets_found,
                outcomes_extracted=0,
                duration_ms=elapsed,
                error=str(e),
                match_date=extracted_date,
                match_time=extracted_time,
            )

        elapsed = int((time.monotonic() - start) * 1000)
        return OddsResult(
            fixture_id=fixture_id,
            site_match_id=site_match_id,
            markets_found=markets_found,
            outcomes_extracted=outcomes_written,
            duration_ms=elapsed,
            match_date=extracted_date,
            match_time=extracted_time,
        )
