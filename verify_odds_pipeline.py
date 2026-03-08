import asyncio
import os
import sys
from pathlib import Path
from playwright.async_api import async_playwright

# Ensure project root is in path
project_root = Path(__file__).parent.absolute()
sys.path.append(str(project_root))

from Modules.FootballCom.fb_manager import run_odds_harvesting
from Data.Access.league_db import init_db
from Data.Access.db_helpers import get_fb_url_for_league

async def verify():
    print("--- ODDS PIPELINE VERIFICATION ---")
    conn = init_db()
    
    # Target fixtures
    target_fixture_ids = ['AJMdTruK', 't2DSNM2s']
    
    # Check if they have fb_url
    for fid in target_fixture_ids:
        row = conn.execute("SELECT league_id FROM schedules WHERE fixture_id = ?", (fid,)).fetchone()
        if row:
            l_id = row['league_id']
            fb_url = get_fb_url_for_league(conn, l_id)
            print(f"Fixture {fid} (League {l_id}): fb_url={fb_url}")
        else:
            print(f"Fixture {fid} not found in schedules.")

    print("\n--- Running Odds Harvesting ---")
    async with async_playwright() as p:
        # We need to pass the playwright instance to run_odds_harvesting
        # But wait, looking at fb_manager.py: run_odds_harvesting(playwright: Playwright)
        await run_odds_harvesting(p)

    # Check results in match_odds
    print("\n--- Final Checks in match_odds table ---")
    for fid in target_fixture_ids:
        count = conn.execute("SELECT COUNT(*) FROM match_odds WHERE fixture_id = ?", (fid,)).fetchone()[0]
        print(f"Fixture {fid}: {count} odds entries found.")
        
        if count > 0:
            rows = conn.execute("SELECT market_name, outcome_name, odds FROM match_odds WHERE fixture_id = ? LIMIT 5", (fid,)).fetchall()
            for r in rows:
                print(f"  - {r['market_name']} | {r['outcome_name']} | {r['odds']}")

    print("\n--- VERIFICATION COMPLETE ---")

if __name__ == "__main__":
    asyncio.run(verify())
