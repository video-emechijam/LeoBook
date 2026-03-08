# db_helpers.py: High-level database access layer for LeoBook.
# Part of LeoBook Data — Access Layer
#
# Thin wrapper over league_db.py (the SQLite source of truth).
# All function signatures are preserved for backward compatibility.

"""
Database Helpers Module
High-level database operations for managing match data and predictions.
All data persisted to leobook.db via league_db.py.
"""

import os
import json
import hashlib
from datetime import datetime as dt
from typing import Dict, Any, List, Optional
import uuid

from Data.Access.league_db import (
    init_db, get_connection, upsert_prediction, update_prediction,
    get_predictions, upsert_fixture, bulk_upsert_fixtures,
    upsert_standing, get_standings as _get_standings_db,
    upsert_league, upsert_team, upsert_fb_match, upsert_live_score,
    log_audit_event as _log_audit_db, upsert_country,
    upsert_accuracy_report, query_all, DB_PATH,
    upsert_match_odds_batch, get_fb_url_for_league,
)

# Module-level connection (lazy init)
_conn = None

def _get_conn():
    global _conn
    if _conn is None:
        _conn = init_db()
    return _conn


# ─── Initialization ───

def init_csvs():
    """Initialize the database. Legacy name preserved for compatibility."""
    print("     Initializing databases...")
    conn = _get_conn()
    init_readiness_cache_table(conn)

def init_readiness_cache_table(conn=None):
    """Initialize the readiness_cache table (Section 2 - Scalability)."""
    conn = conn or _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS readiness_cache (
            gate_id TEXT PRIMARY KEY,
            is_ready INTEGER,
            details TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    print("     [Cache] Readiness cache table initialized.")


# ─── Audit Log ───

def log_audit_event(event_type: str, description: str, balance_before: Optional[float] = None,
                    balance_after: Optional[float] = None, stake: Optional[float] = None,
                    status: str = 'success'):
    """Logs a financial or system event to audit_log."""
    _log_audit_db(_get_conn(), {
        'id': str(uuid.uuid4()),
        'timestamp': dt.now().strftime("%Y-%m-%d %H:%M:%S"),
        'event_type': event_type,
        'description': description,
        'balance_before': balance_before,
        'balance_after': balance_after,
        'stake': stake,
        'status': status,
    })


# ─── Predictions ───

def save_prediction(match_data: Dict[str, Any], prediction_result: Dict[str, Any]):
    """UPSERTs a prediction into the database."""
    fixture_id = match_data.get('fixture_id') or match_data.get('id')
    if not fixture_id or fixture_id == 'unknown':
        print(f"   [Warning] Skipping prediction save: Missing unique fixture_id for "
              f"{match_data.get('home_team')} v {match_data.get('away_team')}")
        return

    date = match_data.get('date', dt.now().strftime("%Y-%m-%d"))

    row = {
        'fixture_id': fixture_id,
        'date': date,
        'match_time': match_data.get('match_time') or match_data.get('time', '00:00'),
        'region_league': match_data.get('region_league', 'Unknown'),
        'home_team': match_data.get('home_team', 'Unknown'),
        'away_team': match_data.get('away_team', 'Unknown'),
        'home_team_id': match_data.get('home_team_id', 'unknown'),
        'away_team_id': match_data.get('away_team_id', 'unknown'),
        'prediction': prediction_result.get('type', 'SKIP'),
        'confidence': prediction_result.get('confidence', 'Low'),
        'reason': " | ".join(prediction_result.get('reason', [])),
        'xg_home': str(prediction_result.get('xg_home', 0.0)),
        'xg_away': str(prediction_result.get('xg_away', 0.0)),
        'btts': prediction_result.get('btts', '50/50'),
        'over_2_5': prediction_result.get('over_2.5', prediction_result.get('over_2_5', '50/50')),
        'best_score': prediction_result.get('best_score', '1-1'),
        'top_scores': "|".join([f"{s['score']}({s['prob']})" for s in prediction_result.get('top_scores', [])]),
        'home_tags': "|".join(prediction_result.get('home_tags', [])),
        'away_tags': "|".join(prediction_result.get('away_tags', [])),
        'h2h_tags': "|".join(prediction_result.get('h2h_tags', [])),
        'standings_tags': "|".join(prediction_result.get('standings_tags', [])),
        'h2h_count': str(prediction_result.get('h2h_n', 0)),
        'home_form_n': str(prediction_result.get('home_form_n', 0)),
        'away_form_n': str(prediction_result.get('away_form_n', 0)),
        'generated_at': dt.now().isoformat(),
        'status': 'pending',
        'match_link': f"{match_data.get('match_link', '')}",
        'odds': str(prediction_result.get('odds', '')),
        'market_reliability_score': str(prediction_result.get('market_reliability', 0.0)),
        'home_crest_url': get_team_crest(match_data.get('home_team_id'), match_data.get('home_team')),
        'away_crest_url': get_team_crest(match_data.get('away_team_id'), match_data.get('away_team')),
        'recommendation_score': str(prediction_result.get('recommendation_score', 0)),
        'h2h_fixture_ids': json.dumps(prediction_result.get('h2h_fixture_ids', [])),
        'form_fixture_ids': json.dumps(prediction_result.get('form_fixture_ids', [])),
        'standings_snapshot': json.dumps(prediction_result.get('standings_snapshot', [])),
        'league_stage': match_data.get('league_stage', ''),
        'last_updated': dt.now().isoformat(),
    }

    upsert_prediction(_get_conn(), row)


def update_prediction_status(match_id: str, date: str, new_status: str, **kwargs):
    """Updates the status and optional fields for a prediction."""
    updates = {'status': new_status}
    updates.update(kwargs)
    update_prediction(_get_conn(), match_id, updates)


def backfill_prediction_entry(fixture_id: str, updates: Dict[str, str]):
    """Partially updates an existing prediction row. Only updates empty/Unknown fields."""
    if not fixture_id or not updates:
        return False

    conn = _get_conn()
    row = conn.execute("SELECT * FROM predictions WHERE fixture_id = ?", (fixture_id,)).fetchone()
    if not row:
        return False

    filtered = {}
    for key, value in updates.items():
        if value:
            current = row[key] if key in row.keys() else ''
            current = str(current).strip() if current else ''
            if not current or current in ('Unknown', 'N/A', 'unknown', 'None', ''):
                filtered[key] = value

    if filtered:
        update_prediction(conn, fixture_id, filtered)
        return True
    return False


def get_last_processed_info() -> Dict:
    """Loads last processed match info."""
    last_processed_info = {}
    conn = _get_conn()
    row = conn.execute(
        "SELECT fixture_id, date FROM predictions ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    if row:
        date_str = row['date']
        if date_str:
            try:
                last_processed_info = {
                    'date': date_str,
                    'id': row['fixture_id'],
                    'date_obj': dt.strptime(date_str, "%Y-%m-%d").date()
                }
                print(f"    [Resume] Last processed: ID {last_processed_info['id']} on {date_str}")
            except Exception:
                pass
    return last_processed_info


# ─── Schedules / Fixtures ───

def save_schedule_entry(match_info: Dict[str, Any]):
    """Saves a single schedule entry."""
    match_info['last_updated'] = dt.now().isoformat()
    # Map schedule CSV column names to fixture table columns
    mapped = {
        'fixture_id': match_info.get('fixture_id'),
        'date': match_info.get('date'),
        'time': match_info.get('match_time', match_info.get('time')),
        'league_id': match_info.get('league_id'),
        'home_team_name': match_info.get('home_team', match_info.get('home_team_name')),
        'away_team_name': match_info.get('away_team', match_info.get('away_team_name')),
        'home_team_id': match_info.get('home_team_id'),
        'away_team_id': match_info.get('away_team_id'),
        'home_score': match_info.get('home_score'),
        'away_score': match_info.get('away_score'),
        'match_status': match_info.get('match_status'),
        'region_league': match_info.get('region_league'),
        'match_link': match_info.get('match_link'),
        'league_stage': match_info.get('league_stage'),
    }
    upsert_fixture(_get_conn(), mapped)


def transform_streamer_match_to_schedule(m: Dict[str, Any]) -> Dict[str, Any]:
    """Transforms a raw match dictionary from the streamer into a standard Schedule entry."""
    now = dt.now()

    date_str = m.get('date')
    if not date_str:
        ts = m.get('timestamp')
        if ts:
            try:
                date_str = dt.fromisoformat(ts.replace('Z', '+00:00')).strftime("%Y-%m-%d")
            except Exception:
                date_str = now.strftime("%Y-%m-%d")
        else:
            date_str = now.strftime("%Y-%m-%d")

    league_id = m.get('league_id', '')
    if not league_id and m.get('region_league'):
        league_id = m['region_league'].replace(' - ', '_').replace(' ', '_').upper()

    return {
        'fixture_id': m.get('fixture_id'),
        'date': date_str,
        'match_time': m.get('match_time', '00:00'),
        'region_league': m.get('region_league', 'Unknown'),
        'league_id': league_id,
        'home_team': m.get('home_team', 'Unknown'),
        'away_team': m.get('away_team', 'Unknown'),
        'home_team_id': m.get('home_team_id', 'unknown'),
        'away_team_id': m.get('away_team_id', 'unknown'),
        'home_score': m.get('home_score', ''),
        'away_score': m.get('away_score', ''),
        'match_status': m.get('status', 'scheduled'),
        'match_link': m.get('match_link', ''),
        'league_stage': m.get('league_stage', ''),
        'last_updated': now.isoformat(),
    }


def save_schedule_batch(entries: List[Dict[str, Any]]):
    """Batch UPSERTs multiple schedule entries."""
    if not entries:
        return
    mapped = []
    for e in entries:
        mapped.append({
            'fixture_id': e.get('fixture_id'),
            'date': e.get('date'),
            'time': e.get('match_time', e.get('time')),
            'league_id': e.get('league_id'),
            'home_team_name': e.get('home_team', e.get('home_team_name')),
            'away_team_name': e.get('away_team', e.get('away_team_name')),
            'home_team_id': e.get('home_team_id'),
            'away_team_id': e.get('away_team_id'),
            'home_score': e.get('home_score'),
            'away_score': e.get('away_score'),
            'match_status': e.get('match_status'),
            'region_league': e.get('region_league'),
            'match_link': e.get('match_link'),
            'league_stage': e.get('league_stage'),
        })
    bulk_upsert_fixtures(_get_conn(), mapped)


def get_all_schedules() -> List[Dict[str, Any]]:
    """Loads all match schedules."""
    return query_all(_get_conn(), 'schedules')


# ─── Live Scores ───

def save_live_score_entry(match_info: Dict[str, Any]):
    """Saves or updates a live score entry."""
    match_info['last_updated'] = dt.now().isoformat()
    upsert_live_score(_get_conn(), match_info)


# ─── Standings ───

def save_standings(standings_data: List[Dict[str, Any]], region_league: str, league_id: str = ""):
    """UPSERTs standings data for a specific league."""
    if not standings_data:
        return

    last_updated = dt.now().isoformat()
    updated_count = 0

    for row in standings_data:
        row['region_league'] = region_league or row.get('region_league', 'Unknown')
        row['last_updated'] = last_updated

        t_id = row.get('team_id', '')
        l_id = league_id or row.get('league_id', '')
        if not l_id and region_league and " - " in region_league:
            l_id = region_league.split(" - ")[1].replace(' ', '_').upper()
        row['league_id'] = l_id

        if t_id and l_id:
            row['standings_key'] = f"{l_id}_{t_id}".upper()
            upsert_standing(_get_conn(), row)
            updated_count += 1

    if updated_count > 0:
        print(f"      [DB] UPSERTed {updated_count} standings entries for {region_league or league_id}")


def get_standings(region_league: str) -> List[Dict[str, Any]]:
    """Loads standings for a specific league."""
    return _get_standings_db(_get_conn(), region_league)


# ─── URL standardization ───

def _standardize_url(url: str, base_type: str = "flashscore") -> str:
    """Ensures URLs are absolute and follow standard patterns."""
    if not url or url == 'N/A' or url.startswith("data:"):
        return url

    if url.startswith("/"):
        url = f"https://www.flashscore.com{url}"

    if "/team/" in url and "https://www.flashscore.com/team/" not in url:
        clean_path = url.split("team/")[-1].strip("/")
        url = f"https://www.flashscore.com/team/{clean_path}/"
    elif "/team/" in url:
        if not url.endswith("/"):
            url += "/"

    if "flashscore.com" not in url and not url.startswith("http"):
        url = f"https://www.flashscore.com{url if url.startswith('/') else '/' + url}"

    return url


# ─── Region / League ───

def save_region_league_entry(info: Dict[str, Any]):
    """Saves or updates a single region-league entry."""
    league_id = info.get('league_id')
    region = info.get('region', 'Unknown')
    league = info.get('league', 'Unknown')
    if not league_id:
        league_id = f"{region}_{league}".replace(' ', '_').replace('-', '_').upper()

    upsert_league(_get_conn(), {
        'league_id': league_id,
        'name': info.get('league', info.get('name', league)), # Flexible name mapping
        'region': region,
        'region_flag': _standardize_url(info.get('region_flag', '')),
        'region_url': _standardize_url(info.get('region_url', '')),
        'crest': _standardize_url(info.get('league_crest', info.get('crest', ''))), # Flexible crest mapping
        'url': _standardize_url(info.get('league_url', info.get('url', ''))), # Flexible url mapping
        'date_updated': dt.now().isoformat(),
    })


# ─── Teams ───

def save_team_entry(team_info: Dict[str, Any]):
    """Saves or updates a single team entry with multi-league support."""
    team_id = team_info.get('team_id')
    if not team_id or team_id == 'unknown':
        return

    conn = _get_conn()

    # Check for existing entry to merge league_ids
    new_league_id = team_info.get('league_ids', team_info.get('region_league', ''))
    merged_league_ids = new_league_id

    row = conn.execute("SELECT league_ids FROM teams WHERE team_id = ?", (team_id,)).fetchone()
    if row and row['league_ids']:
        existing = row['league_ids'].split(';')
        if new_league_id and new_league_id not in existing:
            existing.append(new_league_id)
        merged_league_ids = ';'.join(filter(None, existing))

    upsert_team(conn, {
        'team_id': team_id,
        'name': team_info.get('name', team_info.get('team_name', 'Unknown')), # Flexible name mapping
        'league_ids': [merged_league_ids] if merged_league_ids else [],
        'crest': _standardize_url(team_info.get('team_crest', team_info.get('crest', ''))), # Flexible crest
        'url': _standardize_url(team_info.get('team_url', team_info.get('url', ''))), # Flexible url
        'country_code': team_info.get('country_code', team_info.get('country')), # Flex country
        'city': team_info.get('city'),
        'stadium': team_info.get('stadium'),
        'other_names': team_info.get('other_names'),
        'abbreviations': team_info.get('abbreviations'),
        'search_terms': team_info.get('search_terms'),
    })


def get_team_crest(team_id: str, team_name: str = "") -> str:
    """Retrieves the crest URL for a team."""
    if not team_id and not team_name:
        return ""

    conn = _get_conn()
    if team_id:
        row = conn.execute("SELECT crest FROM teams WHERE team_id = ?", (str(team_id),)).fetchone()
        if row and row['crest']:
            return row['crest']

    if team_name:
        row = conn.execute("SELECT crest FROM teams WHERE name = ?", (team_name,)).fetchone()
        if row and row['crest']:
            return row['crest']

    return ""


def propagate_crest_urls():
    """Propagates Supabase crest URLs from teams into schedules.
    Call after enrichment to ensure home_crest/away_crest in schedules
    point to Supabase-hosted URLs (not local file paths).
    """
    conn = _get_conn()
    h = conn.execute("""
        UPDATE schedules SET home_crest = (
            SELECT t.crest FROM teams t
            WHERE t.team_id = schedules.home_team_id AND t.crest LIKE 'http%'
        ) WHERE home_team_id IN (SELECT team_id FROM teams WHERE crest LIKE 'http%')
          AND (home_crest IS NULL OR home_crest NOT LIKE 'http%supabase%')
    """).rowcount
    a = conn.execute("""
        UPDATE schedules SET away_crest = (
            SELECT t.crest FROM teams t
            WHERE t.team_id = schedules.away_team_id AND t.crest LIKE 'http%'
        ) WHERE away_team_id IN (SELECT team_id FROM teams WHERE crest LIKE 'http%')
          AND (away_crest IS NULL OR away_crest NOT LIKE 'http%supabase%')
    """).rowcount
    conn.commit()
    if h + a > 0:
        print(f"    [Crest] Propagated Supabase URLs: {h} home + {a} away")


# ─── Football.com Registry ───

def get_site_match_id(date: str, home: str, away: str) -> str:
    """Generate a unique ID for a site match to prevent duplicates."""
    unique_str = f"{date}_{home}_{away}".lower().strip()
    return hashlib.md5(unique_str.encode()).hexdigest()


def save_site_matches(matches: List[Dict[str, Any]]):
    """UPSERTs a list of matches extracted from Football.com into the registry."""
    if not matches:
        return

    conn = _get_conn()
    last_extracted = dt.now().isoformat()

    for match in matches:
        site_id = get_site_match_id(match.get('date', ''), match.get('home', ''), match.get('away', ''))
        upsert_fb_match(conn, {
            'site_match_id': site_id,
            'date': match.get('date'),
            'time': match.get('time', 'N/A'),
            'home_team': match.get('home'),
            'away_team': match.get('away'),
            'league': match.get('league'),
            'url': match.get('url'),
            'last_extracted': last_extracted,
            'fixture_id': match.get('fixture_id', ''),
            'matched': match.get('matched', 'No_fs_match_found'),
            'booking_status': match.get('booking_status', 'pending'),
            'booking_details': match.get('booking_details', ''),
            'booking_code': match.get('booking_code', ''),
            'booking_url': match.get('booking_url', ''),
            'status': match.get('status', ''),
        })


def save_match_odds(odds_list: List[Dict[str, Any]]) -> int:
    """Persist match odds to SQLite immediately. Returns rows written."""
    return upsert_match_odds_batch(_get_conn(), odds_list)


def get_match_odds(fixture_id: str) -> List[Dict[str, Any]]:
    """Return all odds rows for a fixture ordered by rank."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM match_odds WHERE fixture_id = ? "
        "ORDER BY rank_in_list ASC",
        (fixture_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def load_site_matches(target_date: str) -> List[Dict[str, Any]]:
    """Loads all extracted site matches for a specific date."""
    return query_all(_get_conn(), 'fb_matches', 'date = ?', (target_date,))


def load_harvested_site_matches(target_date: str) -> List[Dict[str, Any]]:
    """Loads all harvested site matches for a specific date."""
    return query_all(_get_conn(), 'fb_matches',
                     "date = ? AND booking_status = 'harvested'", (target_date,))


def update_site_match_status(site_match_id: str, status: str,
                             fixture_id: Optional[str] = None,
                             details: Optional[str] = None,
                             booking_code: Optional[str] = None,
                             booking_url: Optional[str] = None,
                             matched: Optional[str] = None, **kwargs):
    """Updates the booking status, fixture_id, or booking details for a site match."""
    conn = _get_conn()
    updates = {'booking_status': status, 'status': status, 'last_updated': dt.now().isoformat()}
    if fixture_id:
        updates['fixture_id'] = fixture_id
    if details:
        updates['booking_details'] = details
    if booking_code:
        updates['booking_code'] = booking_code
    if booking_url:
        updates['booking_url'] = booking_url
    if matched:
        updates['matched'] = matched
    if 'odds' in kwargs:
        updates['odds'] = kwargs['odds']

    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
    updates['site_match_id'] = site_match_id
    conn.execute(f"UPDATE fb_matches SET {set_clause} WHERE site_match_id = :site_match_id", updates)
    conn.commit()


# ─── Market Outcome Evaluator (pure function, no I/O) ───

def evaluate_market_outcome(prediction: str, home_score: str, away_score: str,
                            home_team: str = "", away_team: str = "",
                            match_status: str = "") -> Optional[str]:
    """
    Unified First-Principles Outcome Evaluator (v5.0).
    Returns '1' (Correct), '0' (Incorrect), or '' (Unknown/Void).

    Settlement is based on 90min + stoppage time (regulation FT) ONLY.
    If match_status is 'aet'/'pen'/'after pen', the match was a DRAW at FT,
    so any draw-component prediction (1X, X2, draw) wins immediately.

    Handles: 1X2, Double Chance, DNB, Over/Under, BTTS, Team Over/Under,
             Winner & BTTS, Clean Sheet, and team-specific predictions.
    """
    import re
    try:
        h = int(home_score)
        a = int(away_score)
        total = h + a
    except (ValueError, TypeError):
        return ''

    p = (prediction or '').strip().lower()
    h_lower = (home_team or '').strip().lower()
    a_lower = (away_team or '').strip().lower()
    status = (match_status or '').strip().lower()

    # AET/Pen detection: match went beyond 90min = it was a DRAW at regulation FT.
    # Standard bookmaker rules: Double Chance / draw predictions settled on 90min only.
    is_regulation_draw = status in ('aet', 'pen', 'after pen', 'after extra time',
                                    'after penalties', 'ap', 'finished aet',
                                    'finished ap', 'finished pen')
    if is_regulation_draw:
        _draw_markets = ('draw', 'x', '1x', 'x2', 'home or draw', 'home_or_draw',
                         'away or draw', 'away_or_draw', 'draw or away',
                         'double chance 1x', 'double chance x2')
        if p in _draw_markets or ' or draw' in p or 'draw or ' in p:
            return '1'  # Draw at FT → any draw-component bet wins
        # Pure win bets lose at regulation time (match was a draw)
        if p in ('home win', 'home_win', '1', 'away win', 'away_win', '2'):
            return '0'
        if p.endswith(' to win') and 'btts' not in p:
            return '0'
        # DNB → void on draw
        if '(dnb)' in p:
            return ''

    def _team_matches(candidate: str, reference: str) -> bool:
        if not candidate or not reference:
            return False
        return candidate == reference or reference.startswith(candidate) or candidate.startswith(reference)

    def _is_home(team_str: str) -> bool:
        return _team_matches(team_str, h_lower)

    def _is_away(team_str: str) -> bool:
        return _team_matches(team_str, a_lower)

    # 0. Winner & BTTS
    btts_win_match = re.match(r'^(.+?)\s+to\s+win\s*&\s*btts\s+yes$', p)
    if btts_win_match:
        team = btts_win_match.group(1).strip()
        btts = h > 0 and a > 0
        if _is_home(team): return '1' if h > a and btts else '0'
        if _is_away(team): return '1' if a > h and btts else '0'

    # 1. Standard Markets
    if p in ("over 2.5", "over 2_5", "over_2.5", "over_2_5"): return '1' if total > 2.5 else '0'
    if p in ("under 2.5", "under 2_5", "under_2.5", "under_2_5"): return '1' if total < 2.5 else '0'
    if p in ("over 1.5", "over 1_5", "over_1.5", "over_1_5"): return '1' if total > 1.5 else '0'
    if p in ("under 1.5", "under 1_5", "under_1.5", "under_1_5"): return '1' if total < 1.5 else '0'
    if p in ("btts yes", "btts_yes", "both teams to score yes", "both teams to score"): return '1' if h > 0 and a > 0 else '0'
    if p in ("btts no", "btts_no", "both teams to score no"): return '1' if h == 0 or a == 0 else '0'
    if p in ("home win", "home_win", "1"): return '1' if h > a else '0'
    if p in ("away win", "away_win", "2"): return '1' if a > h else '0'
    if p in ("draw", "x"): return '1' if h == a else '0'

    # 1a. Double Chance — settled on 90min + stoppage time ONLY (regulation FT).
    #     A draw at FT triggers a win for "home or draw" / "away or draw" regardless
    #     of any extra time or penalties that may follow.
    if p in ("home or away", "12", "1 2", "double chance 12"): return '1' if h != a else '0'
    if p in ("1x", "home or draw", "home_or_draw", "double chance 1x"): return '1' if h >= a else '0'
    if p in ("x2", "away or draw", "away_or_draw", "draw or away", "double chance x2"): return '1' if a >= h else '0'

    # 2. "Team to win"
    if p.endswith(" to win"):
        team = p.replace(" to win", "").strip()
        if _is_home(team): return '1' if h > a else '0'
        if _is_away(team): return '1' if a > h else '0'

    # 3. "Team or Draw" / Double Chance (team-name based)
    #    Settled on 90min regulation time FT score only.
    if " or draw" in p:
        team = p.replace(" or draw", "").strip()
        if _is_home(team): return '1' if h >= a else '0'
        if _is_away(team): return '1' if a >= h else '0'
    if "draw or " in p:
        team = p.replace("draw or ", "").strip()
        if _is_home(team): return '1' if h >= a else '0'
        if _is_away(team): return '1' if a >= h else '0'

    or_match = re.match(r'^(.+?)\s+or\s+(.+?)$', p)
    if or_match and "draw" not in p:
        t1 = or_match.group(1).strip()
        t2 = or_match.group(2).strip()
        if (_is_home(t1) and _is_away(t2)) or (_is_away(t1) and _is_home(t2)):
            return '1' if h != a else '0'

    # 4. Draw No Bet
    if p.endswith(" (dnb)"):
        team = p.replace(" to win (dnb)", "").replace(" (dnb)", "").strip()
        if h == a: return ''
        if _is_home(team): return '1' if h > a else '0'
        if _is_away(team): return '1' if a > h else '0'

    # 5. Dynamic Over/Under
    over_match = re.search(r'over\s+([\d.]+)', p)
    if over_match:
        threshold = float(over_match.group(1))
        team_part = p[:over_match.start()].strip()
        if team_part:
            if _is_home(team_part): return '1' if h > threshold else '0'
            if _is_away(team_part): return '1' if a > threshold else '0'
        if "away" in p: return '1' if a > threshold else '0'
        if "home" in p: return '1' if h > threshold else '0'
        return '1' if total > threshold else '0'

    under_match = re.search(r'under\s+([\d.]+)', p)
    if under_match:
        threshold = float(under_match.group(1))
        team_part = p[:under_match.start()].strip()
        if team_part:
            if _is_home(team_part): return '1' if h < threshold else '0'
            if _is_away(team_part): return '1' if a < threshold else '0'
        if "away" in p: return '1' if a < threshold else '0'
        if "home" in p: return '1' if h < threshold else '0'
        return '1' if total < threshold else '0'

    # 6. Clean Sheet
    if "clean sheet" in p:
        team = p.replace(" clean sheet", "").strip()
        if _is_home(team): return '1' if a == 0 else '0'
        if _is_away(team): return '1' if h == 0 else '0'

    return ''


# ─── Legacy compatibility aliases ───
# Some modules import these directly. Point them at the DB path.
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_current_dir, "..", ".."))
DB_DIR = os.path.join(_project_root, "Data", "Store")

# These path constants are kept for any module that references them,
# but they now point to non-existent .bak files. Code should use
# the functions above instead.
PREDICTIONS_CSV = os.path.join(DB_DIR, "predictions.csv")
SCHEDULES_CSV = os.path.join(DB_DIR, "schedules.csv")
STANDINGS_CSV = os.path.join(DB_DIR, "standings.csv")
TEAMS_CSV = os.path.join(DB_DIR, "teams.csv")
REGION_LEAGUE_CSV = os.path.join(DB_DIR, "region_league.csv")
ACCURACY_REPORTS_CSV = os.path.join(DB_DIR, "accuracy_reports.csv")
FB_MATCHES_CSV = os.path.join(DB_DIR, "fb_matches.csv")
MATCH_REGISTRY_CSV = FB_MATCHES_CSV
AUDIT_LOG_CSV = os.path.join(DB_DIR, "audit_log.csv")
PROFILES_CSV = os.path.join(DB_DIR, "profiles.csv")
CUSTOM_RULES_CSV = os.path.join(DB_DIR, "custom_rules.csv")
RULE_EXECUTIONS_CSV = os.path.join(DB_DIR, "rule_executions.csv")
LIVE_SCORES_CSV = os.path.join(DB_DIR, "live_scores.csv")
COUNTRIES_CSV = os.path.join(DB_DIR, "countries.csv")

# Legacy low-level functions — kept as no-ops / thin wrappers for any
# external code still calling them. These will be removed once all
# consumers are updated.
def _read_csv(filepath: str) -> List[Dict[str, str]]:
    """Legacy: reads from SQLite instead of CSV."""
    table_map = {
        PREDICTIONS_CSV: 'predictions',
        SCHEDULES_CSV: 'schedules',
        STANDINGS_CSV: 'standings',
        TEAMS_CSV: 'teams',
        REGION_LEAGUE_CSV: 'leagues',
        FB_MATCHES_CSV: 'fb_matches',
        AUDIT_LOG_CSV: 'audit_log',
        LIVE_SCORES_CSV: 'live_scores',
        COUNTRIES_CSV: 'countries',
        ACCURACY_REPORTS_CSV: 'accuracy_reports',
    }
    table = table_map.get(filepath)
    if table:
        return query_all(_get_conn(), table)
    return []

def _write_csv(filepath: str, data: List[Dict], fieldnames: List[str]):
    """Legacy no-op: writes go through SQLite now."""
    pass

def _append_to_csv(filepath: str, data_row: Dict, fieldnames: List[str]):
    """Legacy no-op."""
    pass

def upsert_entry(filepath: str, data_row: Dict, fieldnames: List[str], unique_key: str):
    """Legacy: routes to appropriate SQLite upsert."""
    pass

def batch_upsert(filepath: str, data_rows: List[Dict], fieldnames: List[str], unique_key: str):
    """Legacy: routes to appropriate SQLite batch upsert."""
    pass

append_to_csv = _append_to_csv

# Legacy CSV_LOCK — no longer needed, WAL handles concurrency
import asyncio
CSV_LOCK = asyncio.Lock()

# Legacy headers dict — kept for any external code referencing it
files_and_headers = {}
