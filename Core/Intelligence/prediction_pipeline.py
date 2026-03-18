# prediction_pipeline.py: Pure DB-driven prediction pipeline for Chapter 1 Page 2.
# Part of LeoBook Core — Intelligence
#
# Functions: get_weekly_fixtures(), compute_team_form(), compute_h2h(),
#            build_rule_engine_input(), run_predictions(), apply_smart_scheduling()
# Called by: Leo.py (Chapter 1 Page 2)

"""
V7 Prediction Pipeline — Zero browser, pure computation.
All data sourced from the schedules table (populated by weekly enrichment).
"""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

from Data.Access.league_db import init_db, computed_standings
from Data.Access.db_helpers import save_prediction
from Core.Intelligence.rule_engine import RuleEngine
from Core.Intelligence.rl.inference import RLPredictor
from Core.Intelligence.ensemble import EnsembleEngine
from Core.Utils.constants import now_ng

logger = logging.getLogger(__name__)
NIGERIA_TZ = ZoneInfo("Africa/Lagos")


def _schedule_to_match_dict(row: Dict) -> Dict:
    """Convert a schedules table row into the match dict format RuleEngine/TagGenerator expects.

    TagGenerator._parse_match_result expects:
        home: str, away: str, score: str (e.g. "2-1"), winner: str ("Home"/"Away"/"Draw"),
        date: str, fixture_id: str
    """
    home_score = row.get("home_score")
    away_score = row.get("away_score")

    # Build score string
    if home_score is not None and away_score is not None:
        try:
            hs, as_ = int(home_score), int(away_score)
            score = f"{hs}-{as_}"
            if hs > as_:
                winner = "Home"
            elif as_ > hs:
                winner = "Away"
            else:
                winner = "Draw"
        except (ValueError, TypeError):
            score = "0-0"
            winner = "Draw"
    else:
        score = "0-0"
        winner = "Draw"

    return {
        "home": row.get("home_team_name", ""),
        "away": row.get("away_team_name", ""),
        "score": score,
        "winner": winner,
        "date": row.get("date", ""),
        "fixture_id": row.get("fixture_id", ""),
    }


def get_weekly_fixtures(conn=None, days: int = 7) -> List[Dict]:
    """Query schedules for the next N days of scheduled (unplayed) matches.

    Returns list of schedule row dicts for matches that haven't been played yet.
    """
    conn = conn or init_db()
    now = now_ng()
    today_str = now.strftime("%d.%m.%Y")

    # Scan next 7 days (including today)
    date_strings = []
    for i in range(8):
        target = now + timedelta(days=i)
        date_strings.append(target.strftime("%Y-%m-%d")) # Unified v7 Format: YYYY-MM-DD

    placeholders = ",".join(["?"] * len(date_strings))
    rows = conn.execute(
        f"""SELECT
               -- ROOT CAUSE 2 FIX: Prefer fixture-specific name over global teams.name.
               -- teams.name is polluted by multi-league upserts (wrong transliteration / alias).
               -- schedules.home_team_name is stored at scrape time for THAT fixture, so it is
               -- the most accurate name for this specific matchup.
               COALESCE(NULLIF(s.home_team_name, ''), h.name) AS home_team_name,
               COALESCE(NULLIF(s.away_team_name, ''), a.name) AS away_team_name,
               s.*
            FROM schedules s
            LEFT JOIN teams h ON s.home_team_id = h.team_id
            LEFT JOIN teams a ON s.away_team_id = a.team_id
            WHERE s.date IN ({placeholders})
              AND (s.match_status IS NULL OR s.match_status = 'scheduled' OR s.match_status = '')
            ORDER BY s.date, s.time""",
        date_strings,
    ).fetchall()

    return [dict(r) for r in rows]


def compute_team_form(conn, team_id: str, limit: int = 10) -> List[Dict]:
    """Get last N completed matches for a team from the schedules table.

    Searches where team is either home or away and a result exists.
    Returns in RuleEngine-compatible format.
    """
    rows = conn.execute(
        """SELECT * FROM schedules
           WHERE (home_team_id = ? OR away_team_id = ?)
             AND home_score IS NOT NULL AND away_score IS NOT NULL
             AND home_score != '' AND away_score != ''
           ORDER BY date DESC
           LIMIT ?""",
        (team_id, team_id, limit),
    ).fetchall()

    return [_schedule_to_match_dict(dict(r)) for r in rows]


def compute_h2h(conn, home_team_id: str, away_team_id: str, limit: int = 10) -> List[Dict]:
    """Get direct head-to-head matches between two teams from schedules.

    Either team can be home or away in historical matchups.
    Returns in RuleEngine-compatible format.
    """
    rows = conn.execute(
        """SELECT * FROM schedules
           WHERE ((home_team_id = ? AND away_team_id = ?)
               OR (home_team_id = ? AND away_team_id = ?))
             AND home_score IS NOT NULL AND away_score IS NOT NULL
             AND home_score != '' AND away_score != ''
           ORDER BY date DESC
           LIMIT ?""",
        (home_team_id, away_team_id, away_team_id, home_team_id, limit),
    ).fetchall()

    return [_schedule_to_match_dict(dict(r)) for r in rows]


def build_rule_engine_input(conn, fixture: Dict) -> Dict[str, Any]:
    """Assemble the h2h_data + standings dict that RuleEngine.analyze expects.

    Args:
        conn: SQLite connection
        fixture: A schedule row dict for the fixture to predict

    Returns:
        Dict with {"h2h_data": {...}, "standings": [...]}
    """
    home_team_id = fixture.get("home_team_id", "")
    away_team_id = fixture.get("away_team_id", "")
    home_team = fixture.get("home_team_name", "")
    away_team = fixture.get("away_team_name", "")
    league_id = fixture.get("league_id", "")
    region_league = fixture.get("region_league", "")
    season = fixture.get("season", "")

    # 1. Team form (last 10 completed matches per team)
    home_form = compute_team_form(conn, home_team_id, limit=10)
    away_form = compute_team_form(conn, away_team_id, limit=10)

    # 2. Direct H2H
    h2h = compute_h2h(conn, home_team_id, away_team_id, limit=10)

    # 3. Standings (computed on-the-fly from schedules)
    standings = []
    if league_id:
        standings = computed_standings(conn=conn, league_id=league_id, season=season)

    h2h_data = {
        "home_team": home_team,
        "away_team": away_team,
        "region_league": region_league or "GLOBAL",
        "home_last_10_matches": home_form,
        "away_last_10_matches": away_form,
        "head_to_head": h2h,
    }

    return {"h2h_data": h2h_data, "standings": standings}


def _get_existing_prediction_ids(conn) -> set:
    """Get fixture_ids of already-predicted matches to avoid duplicates."""
    try:
        rows = conn.execute("SELECT fixture_id FROM predictions WHERE fixture_id IS NOT NULL").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


async def run_predictions(conn=None, fixtures: List[Dict] = None, scheduler=None) -> List[Dict]:
    """Main prediction loop — pure DB computation, zero browser.

    Args:
        conn: SQLite connection (optional)
        fixtures: Pre-fetched list of fixture dicts (optional, fetched if None)
        scheduler: TaskScheduler for smart scheduling (optional)

    Returns:
        List of generated prediction dicts
    """
    conn = conn or init_db()

    if fixtures is None:
        fixtures = get_weekly_fixtures(conn)

    if not fixtures:
        print("    [Predictions] No scheduled fixtures found for the next 7 days.")
        return []

    # Filter out already-predicted
    existing_ids = _get_existing_prediction_ids(conn)
    new_fixtures = [f for f in fixtures if f.get("fixture_id") not in existing_ids]

    if not new_fixtures:
        print("    [Predictions] All fixtures already predicted.")
        return []

    # Filter out past matches (already started today)
    now = now_ng()
    today_str = now.strftime("%d.%m.%Y")
    now_time = now.time()

    eligible = []
    for f in new_fixtures:
        if f.get("date") == today_str:
            time_str = f.get("time", "")
            try:
                match_time = datetime.strptime(time_str, "%H:%M").time()
                if match_time > now_time:
                    eligible.append(f)
            except (ValueError, TypeError):
                eligible.append(f)  # Keep if time unparseable
        else:
            eligible.append(f)

    if not eligible:
        print("    [Predictions] No eligible fixtures (all already started or predicted).")
        return []

    print(f"    [Predictions] Processing {len(eligible)} fixtures (pure DB computation)...")

    predictions_made = []
    skipped = 0

    for fixture in eligible:
        fixture_id = fixture.get("fixture_id", "unknown")
        home = fixture.get("home_team_name", "?")
        away = fixture.get("away_team_name", "?")

        try:
            # Build input from DB
            vision_data = build_rule_engine_input(conn, fixture)

            # Data quality gate: need at least 3 form matches per team
            home_form_n = len(vision_data["h2h_data"]["home_last_10_matches"])
            away_form_n = len(vision_data["h2h_data"]["away_last_10_matches"])

            if home_form_n < 3 or away_form_n < 3:
                skipped += 1
                continue

            # Run symbolic prediction
            rule_prediction = RuleEngine.analyze(vision_data)

            # Run neural prediction
            rl_predictor = RLPredictor.get_instance()
            rl_prediction = rl_predictor.predict(
                vision_data,
                fs_league_id=fixture.get("league_id", "GLOBAL"),
                home_team_id=fixture.get("home_team_id", ""),
                away_team_id=fixture.get("away_team_id", "")
            )

            # Ensemble Merge — scale W_neural by data richness for this league
            richness = EnsembleEngine.get_richness_score(
                fixture.get("league_id", "GLOBAL"),
                current_season=fixture.get("season", ""),
            )
            merged = EnsembleEngine.merge(
                rule_logits=rule_prediction.get("raw_scores", {"home": 1.0, "draw": 1.0, "away": 1.0}),
                rule_conf=rule_prediction.get("market_reliability", 50) / 100.0,
                rl_logits=rl_prediction.get("rl_action_probs"),
                rl_conf=rl_prediction.get("ml_confidence"),
                league_id=fixture.get("league_id", "GLOBAL"),
                data_richness_score=richness,
            )

            # Integrate merged data into final prediction
            # We keep Rule Engine's structural fields but update confidence and add ensemble metadata
            prediction = rule_prediction.copy()
            prediction["ensemble_path"] = merged["path"]
            prediction["ensemble_weights"] = merged["weights"]
            prediction["market_reliability"] = round(merged["confidence"] * 100, 1)

            # Update confidence label based on merged confidence
            conf = merged["confidence"]
            if conf > 0.75: prediction["confidence"] = "Very High"
            elif conf > 0.60: prediction["confidence"] = "High"
            elif conf > 0.45: prediction["confidence"] = "Medium"
            else: prediction["confidence"] = "Low"

            p_type = prediction.get("type", "SKIP")
            if p_type == "SKIP":
                skipped += 1
                continue

            # Record reference data
            h2h_ids = [m.get("fixture_id", "") for m in vision_data["h2h_data"]["head_to_head"] if m.get("fixture_id")]
            home_form_ids = [m.get("fixture_id", "") for m in vision_data["h2h_data"]["home_last_10_matches"] if m.get("fixture_id")]
            away_form_ids = [m.get("fixture_id", "") for m in vision_data["h2h_data"]["away_last_10_matches"] if m.get("fixture_id")]

            prediction["h2h_fixture_ids"] = h2h_ids
            prediction["form_fixture_ids"] = home_form_ids + away_form_ids
            prediction["standings_snapshot"] = vision_data["standings"]

            # Build match_data for save_prediction
            match_data = {
                "fixture_id": fixture_id,
                "date": fixture.get("date", ""),
                "match_time": fixture.get("time", ""),
                "region_league": fixture.get("region_league", ""),
                "home_team": home,
                "away_team": away,
                "home_team_id": fixture.get("home_team_id", ""),
                "away_team_id": fixture.get("away_team_id", ""),
                "match_link": fixture.get("match_link", ""),
            }

            save_prediction(match_data, prediction)
            predictions_made.append({**match_data, **prediction})
            print(f"      [✓] {home} vs {away} → {p_type} ({prediction.get('confidence', '?')})")

            # Paper trade logging (never blocks pipeline)
            # Controlled by DISABLE_PAPER_TRADES flag in Leo.py
            try:
                from Leo import DISABLE_PAPER_TRADES
            except ImportError:
                DISABLE_PAPER_TRADES = False

            if DISABLE_PAPER_TRADES:
                if fixture_id == fixtures[0].get("fixture_id"):  # Log once per batch
                    print("      [CH1] Paper trades disabled by flag (DISABLE_PAPER_TRADES=True)")
            else:
                try:
                    from Core.Intelligence.ensemble import log_paper_trade
                    # Derive picks
                    rl_probs = rl_prediction.get("rl_action_probs", {})
                    rl_pick_key = max(rl_probs, key=rl_probs.get) if rl_probs else "no_bet"

                    # 30-dim rule pick (upgraded from 1X2-only)
                    best_30 = rule_prediction.get("best_30dim")
                    rule_pick_key = best_30["market_key"] if best_30 else "no_bet"

                    # Ensemble: use RL pick if RL is active, otherwise 30-dim rule pick
                    is_symbolic = prediction.get("ensemble_path") == "symbolic_fallback"
                    ensemble_pick_key = rule_pick_key if is_symbolic else rl_pick_key

                    log_paper_trade(
                        fixture_id=fixture_id,
                        home_team=home,
                        away_team=away,
                        league_id=fixture.get("league_id"),
                        match_date=fixture.get("date", ""),
                        rl_pick=rl_pick_key,
                        rule_pick=rule_pick_key,
                        ensemble_pick=ensemble_pick_key,
                        model_prob=best_30["prob"] if best_30 else 0.0,
                        rl_confidence=rl_prediction.get("ml_confidence"),
                        rule_confidence=rule_prediction.get("market_reliability"),
                    )
                except Exception:
                    pass  # Paper trade logging must never block

        except Exception as e:
            logger.error(f"      [✗] Prediction failed for {home} vs {away}: {e}")
            skipped += 1

    # Apply smart scheduling if scheduler provided
    if scheduler and predictions_made:
        predictions_made = apply_smart_scheduling(predictions_made, scheduler, conn)

    print(f"\n    [Predictions] Done: {len(predictions_made)} predictions, {skipped} skipped.")
    return predictions_made


def apply_smart_scheduling(predictions: List[Dict], scheduler, conn=None) -> List[Dict]:
    """Enforce max 1 prediction per team per week.

    If a team appears in multiple fixtures, keep the earliest and schedule the rest
    as 'day_before_predict' tasks in the scheduler.
    """
    from Core.Utils.constants import now_ng

    team_seen = {}  # team_id -> earliest fixture date
    keep = []
    deferred = 0

    # Sort by date to ensure earliest is processed first
    sorted_preds = sorted(predictions, key=lambda p: p.get("date", ""))

    for pred in sorted_preds:
        home_id = pred.get("home_team_id", "")
        away_id = pred.get("away_team_id", "")

        # Check if either team already has a prediction this week
        home_conflict = home_id in team_seen
        away_conflict = away_id in team_seen

        if home_conflict or away_conflict:
            # Defer this prediction — schedule as day_before_predict
            try:
                fixture_date = pred.get("date", "")
                scheduler.schedule_day_before_predict(
                    fixture_id=pred.get("fixture_id", ""),
                    match_date=fixture_date,
                )
                deferred += 1
            except Exception as e:
                logger.warning(f"    [SmartSched] Failed to schedule deferred prediction: {e}")
            continue

        # Mark teams as seen
        if home_id:
            team_seen[home_id] = pred.get("date", "")
        if away_id:
            team_seen[away_id] = pred.get("date", "")
        keep.append(pred)

    if deferred > 0:
        print(f"    [SmartSched] Kept {len(keep)}, deferred {deferred} to scheduler (1-per-team-per-week).")

    return keep
