# fb_harvester.py: Batch processing of match booking codes.
# Part of LeoBook Modules — Football.com
#
# Functions: run_harvest_loop()
# Called by: Leo.py (Chapter 2 Page 1) via fb_manager.py

from playwright.async_api import Page
from Data.Access.db_helpers import (
    get_site_match_id, load_site_matches, log_audit_event
)
from Core.System.lifecycle import log_state
from Core.Utils.constants import now_ng
from .booker.booking_code import harvest_booking_codes


async def run_harvest_loop(page: Page, matched_urls: dict, day_preds: list,
                           target_date: str, current_balance: float) -> int:
    """
    Executes Phase 2a: Harvest booking codes for all matched URLs.
    Delegates to harvest_booking_codes() for the actual extraction.
    Returns count of successfully harvested codes.
    """
    print(f"  [Phase 2a] Entering Harvest for {len(matched_urls)} matches...")

    try:
        await harvest_booking_codes(page, matched_urls, day_preds, target_date)
        # Count how many got harvested
        matches_after = load_site_matches(target_date)
        harvested_count = sum(1 for m in matches_after if m.get('booking_status') == 'harvested')
        log_state("Harvest", "Complete", f"{harvested_count} codes for {target_date}")
    except Exception as e:
        print(f"    [Harvest Error] {target_date}: {e}")
        log_audit_event("HARVEST_ERROR", f"Date {target_date}: {e}",
                        current_balance, current_balance, 0, "FAILED")
        harvested_count = 0

    print(f"  [Harvest Complete] {harvested_count} codes harvested successfully.")
    return harvested_count
