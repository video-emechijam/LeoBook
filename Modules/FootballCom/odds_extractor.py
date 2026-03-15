# odds_extractor.py: Extracts all ranked market-outcome odds for
#                    a single match from football.com. No-login only.
# Part of LeoBook Modules — FootballCom
#
# Functions: OddsExtractor.extract(), _assert_no_login(),
#            _parse_line(), _load_market_catalogue()
# Called by: Modules/FootballCom/fb_url_resolver.py
#            (resolve_urls_stable inner fixture loop)

import json
import re
import time
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional

from playwright.async_api import Page

from Core.Utils.constants import now_ng
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


# ── Extractor ─────────────────────────────────────────────────────────────

class OddsExtractor:
    """
    Extracts all ranked market odds from a football.com match detail page.
    Page MUST already be navigated — extract() never calls page.goto().
    Saves each market batch to SQLite immediately after extraction.
    """

    def __init__(self, page: Page, conn: sqlite3.Connection) -> None:
        self.page = page
        self.conn = conn

    # ── No-login guard ────────────────────────────────────────────────

    @staticmethod
    async def _assert_no_login(page: Page) -> None:
        LOGIN_INDICATORS = [
            ".user-account", ".user-balance", ".logout-btn",
            "[data-test='user-menu']",
            "[class*='user-logged']",
            "[class*='account-balance']",
            ".m-account-info", ".m-user-panel",
            "a[href*='logout']",
        ]
        for sel in LOGIN_INDICATORS:
            try:
                el = await page.query_selector(sel)
                if el and await el.is_visible():
                    raise RuntimeError(
                        "OddsExtractor: active login session detected. "
                        "Odds extraction must run without login. "
                        "Restart without logging in."
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
        Scrolls the full page first to ensure all market containers are
        in the DOM (they are lazy-loaded), then iterates the catalogue.
        Saves each market batch to SQLite immediately after extraction.
        """
        from Modules.Flashscore.fs_league_hydration import _scroll_to_load

        start = time.monotonic()
        markets_found = 0
        outcomes_written = 0

        try:
            await self._assert_no_login(self.page)

            # Hydrate the page — scroll until all [data-market-id] containers
            # are stable in the DOM. Without this, containers below the initial
            # viewport are invisible to query_selector and silently skipped.
            containers_found = await _scroll_to_load(
                self.page, "[data-market-id]"
            )
            print(f"    [Odds] {fixture_id}: {containers_found} market containers hydrated")

            # De-duplicate market IDs so we visit each container once
            seen_market_ids: set = set()

            for market in _MARKET_CATALOGUE:
                market_id = str(market.get("market_id", ""))
                base_market = market.get("base_market", "")
                category = market.get("category", "")
                likelihood = market.get("likelihood_percent", 0)
                rank = market.get("rank", 0)

                if not market_id:
                    continue

                # Skip if this market_id container was already processed
                if market_id in seen_market_ids:
                    continue

                # Find the container on the page
                container = await self.page.query_selector(
                    f"[data-market-id='{market_id}']"
                )
                if not container:
                    continue

                seen_market_ids.add(market_id)
                markets_found += 1

                # Scroll into view to trigger lazy-loaded content
                try:
                    await container.scroll_into_view_if_needed()
                except Exception:
                    pass

                outcome_items = await container.query_selector_all(
                    ".m-outcome-item, [class*='m-outcome-item']"
                )

                batch: List[Dict] = []
                extracted_at = now_ng().isoformat()

                for item in outcome_items:
                    try:
                        name_el = await item.query_selector(
                            ".m-outcome-name, [class*='outcome-name'], .name, span"
                        )
                        odds_el = await item.query_selector(
                            ".m-odds-value, .m-price, [class*='odds-value'], [class*='m-price'], [class*='price']"
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

                # IMMEDIATE save after each market — not at extract() end
                if batch:
                    written = upsert_match_odds_batch(self.conn, batch)
                    outcomes_written += written

        except RuntimeError:
            raise  # login guard — fatal, re-raise
        except Exception as e:
            elapsed = int((time.monotonic() - start) * 1000)
            print(f"    [ODDS ERROR] {fixture_id}: {e}")
            return OddsResult(
                fixture_id=fixture_id,
                site_match_id=site_match_id,
                markets_found=markets_found,
                outcomes_extracted=0,
                duration_ms=elapsed,
                error=str(e),
            )

        elapsed = int((time.monotonic() - start) * 1000)
        return OddsResult(
            fixture_id=fixture_id,
            site_match_id=site_match_id,
            markets_found=markets_found,
            outcomes_extracted=outcomes_written,
            duration_ms=elapsed,
        )
