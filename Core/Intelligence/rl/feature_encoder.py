# feature_encoder.py: Converts raw match data into fixed-size tensors for the RL model.
# Part of LeoBook Core — Intelligence (RL Engine)
#
# Classes: FeatureEncoder
# Called by: trainer.py, inference.py

"""
Feature Encoder Module
Transforms vision_data dicts (same input as RuleEngine.analyze()) into
fixed-size tensors with recency weighting and 2-season window enforcement.
"""

import math
import torch
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

FEATURE_DIM = 192  # Fixed feature vector size

# Recency decay: match at index 0 (most recent) = weight 1.0, index 9 = weight ~0.37
_RECENCY_WEIGHTS = [math.exp(-0.1 * i) for i in range(10)]
_RECENCY_SUM = sum(_RECENCY_WEIGHTS)


class FeatureEncoder:
    """Encodes raw match context into a 192-dim tensor for the RL model."""

    @staticmethod
    def encode(vision_data: Dict[str, Any],
               league_meta: Optional[Dict[str, Any]] = None) -> torch.Tensor:
        """
        Convert vision_data to a [1, FEATURE_DIM] tensor.

        Args:
            vision_data: Same dict that RuleEngine.analyze() receives.
            league_meta: Optional league-level metadata (avg_goals, home_adv, level).

        Returns:
            torch.Tensor of shape [1, FEATURE_DIM].
        """
        features = []

        h2h_data = vision_data.get("h2h_data", {})
        standings = vision_data.get("standings", [])
        home_team = h2h_data.get("home_team", "")
        away_team = h2h_data.get("away_team", "")

        home_form = [m for m in h2h_data.get("home_last_10_matches", []) if m][:10]
        away_form = [m for m in h2h_data.get("away_last_10_matches", []) if m][:10]
        h2h_matches = [m for m in h2h_data.get("head_to_head", []) if m]

        # --- 1. xG Features (4 floats) ---
        home_xg = FeatureEncoder._compute_xg(home_form, home_team, is_home=True)
        away_xg = FeatureEncoder._compute_xg(away_form, away_team, is_home=False)
        features.extend([
            home_xg,
            away_xg,
            home_xg - away_xg,  # xG difference
            home_xg + away_xg,  # Total xG
        ])

        # --- 2. Home Form (recency-weighted W/D/L) (30 floats) ---
        features.extend(FeatureEncoder._encode_form(home_form, home_team))

        # --- 3. Away Form (recency-weighted W/D/L) (30 floats) ---
        features.extend(FeatureEncoder._encode_form(away_form, away_team))

        # --- 4. Home Goal Stats (scored/conceded per match) (20 floats) ---
        features.extend(FeatureEncoder._encode_goal_stats(home_form, home_team))

        # --- 5. Away Goal Stats (20 floats) ---
        features.extend(FeatureEncoder._encode_goal_stats(away_form, away_team))

        # --- 6. H2H Summary (8 floats) ---
        features.extend(FeatureEncoder._encode_h2h(h2h_matches, home_team, away_team))

        # --- 7. Standings Features (10 floats) ---
        features.extend(FeatureEncoder._encode_standings(standings, home_team, away_team))

        # --- 8. Schedule Context (6 floats) ---
        features.extend(FeatureEncoder._encode_schedule_context(home_form, away_form))

        # --- 9. League Metadata (4 floats) ---
        features.extend(FeatureEncoder._encode_league_meta(league_meta))

        # --- 10. Padding to FEATURE_DIM ---
        current_len = len(features)
        if current_len < FEATURE_DIM:
            features.extend([0.0] * (FEATURE_DIM - current_len))
        elif current_len > FEATURE_DIM:
            features = features[:FEATURE_DIM]

        tensor = torch.tensor([features], dtype=torch.float32)
        return tensor

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _compute_xg(form: List[Dict], team_name: str, is_home: bool) -> float:
        """Compute expected goals from form data with home/away adjustment."""
        if not form:
            return 1.2  # League average default

        goals = []
        for m in form:
            score = m.get("score", "0-0")
            try:
                gf, ga = map(int, score.replace(" ", "").split("-"))
            except (ValueError, AttributeError):
                continue

            is_home_match = m.get("home", "") == team_name
            scored = gf if is_home_match else ga

            # Home/away adjustment
            if is_home and not is_home_match:
                scored = scored * 1.15  # Boost for playing at home
            elif not is_home and is_home_match:
                scored = scored * 0.85  # Penalty for playing away

            goals.append(scored)

        return sum(goals) / max(len(goals), 1)

    @staticmethod
    def _encode_form(form: List[Dict], team_name: str) -> List[float]:
        """Encode last-10 form as recency-weighted one-hot W/D/L (30 floats)."""
        result = [0.0] * 30  # 10 matches × 3 (W, D, L)

        for i, m in enumerate(form[:10]):
            score = m.get("score", "0-0")
            try:
                gf, ga = map(int, score.replace(" ", "").split("-"))
            except (ValueError, AttributeError):
                continue

            is_home_match = m.get("home", "") == team_name
            team_goals = gf if is_home_match else ga
            opp_goals = ga if is_home_match else gf

            weight = _RECENCY_WEIGHTS[i] / _RECENCY_SUM
            if team_goals > opp_goals:
                result[i * 3 + 0] = weight  # Win
            elif team_goals == opp_goals:
                result[i * 3 + 1] = weight  # Draw
            else:
                result[i * 3 + 2] = weight  # Loss

        return result

    @staticmethod
    def _encode_goal_stats(form: List[Dict], team_name: str) -> List[float]:
        """Encode goal-scoring and conceding patterns (20 floats)."""
        scored = []
        conceded = []

        for m in form[:10]:
            score = m.get("score", "0-0")
            try:
                gf, ga = map(int, score.replace(" ", "").split("-"))
            except (ValueError, AttributeError):
                continue

            is_home_match = m.get("home", "") == team_name
            scored.append(gf if is_home_match else ga)
            conceded.append(ga if is_home_match else gf)

        if not scored:
            return [0.0] * 20

        result = [
            np.mean(scored),                          # Avg scored
            np.std(scored) if len(scored) > 1 else 0, # Scored variance
            max(scored),                               # Max scored
            min(scored),                               # Min scored
            sum(1 for s in scored if s >= 2) / len(scored),  # % scoring 2+
            sum(1 for s in scored if s == 0) / len(scored),  # % failing to score
            np.mean(conceded),                         # Avg conceded
            np.std(conceded) if len(conceded) > 1 else 0,
            max(conceded),
            min(conceded),
            sum(1 for c in conceded if c >= 2) / len(conceded),  # % conceding 2+
            sum(1 for c in conceded if c == 0) / len(conceded),  # % clean sheets
            np.mean(scored) + np.mean(conceded),       # Avg total goals
            sum(1 for s, c in zip(scored, conceded) if s > 0 and c > 0) / len(scored),  # BTTS %
            sum(1 for s, c in zip(scored, conceded) if s + c > 2.5) / len(scored),  # O2.5 %
        ]

        # Pad to 20
        result.extend([0.0] * (20 - len(result)))
        return result[:20]

    @staticmethod
    def _encode_h2h(h2h: List[Dict], home_team: str, away_team: str) -> List[float]:
        """Encode H2H history (8 floats)."""
        if not h2h:
            return [0.0] * 8

        home_wins = 0
        away_wins = 0
        draws = 0
        total_goals = 0

        for m in h2h[:10]:  # Last 10 H2H matches
            score = m.get("score", "0-0")
            try:
                gf, ga = map(int, score.replace(" ", "").split("-"))
            except (ValueError, AttributeError):
                continue

            total_goals += gf + ga
            is_home = m.get("home", "") == home_team
            home_g = gf if is_home else ga
            away_g = ga if is_home else gf

            if home_g > away_g:
                home_wins += 1
            elif away_g > home_g:
                away_wins += 1
            else:
                draws += 1

        n = max(home_wins + away_wins + draws, 1)
        avg_goals = total_goals / n

        return [
            home_wins / n,    # Home win rate
            away_wins / n,    # Away win rate
            draws / n,        # Draw rate
            avg_goals,        # Avg total goals in H2H
            float(n),         # Number of H2H matches (signal strength)
            float(home_wins > away_wins),   # Home dominance flag
            float(away_wins > home_wins),   # Away dominance flag
            float(avg_goals > 2.5),         # High-scoring H2H flag
        ]

    @staticmethod
    def _encode_standings(standings: List[Dict],
                          home_team: str, away_team: str) -> List[float]:
        """Encode standings context (10 floats)."""
        if not standings:
            return [0.0] * 10

        home_pos = away_pos = 0
        home_pts = away_pts = 0
        home_gd = away_gd = 0
        league_size = len(standings)

        for row in standings:
            name = row.get("team", row.get("team_name", ""))
            pos = row.get("position", row.get("rank", 0))
            pts = row.get("points", 0)
            gd = row.get("goal_difference", row.get("gd", 0))

            if name == home_team:
                home_pos = pos
                home_pts = pts
                home_gd = gd
            elif name == away_team:
                away_pos = pos
                away_pts = pts
                away_gd = gd

        ls = max(league_size, 1)
        return [
            home_pos / ls,                # Normalized home position
            away_pos / ls,                # Normalized away position
            (away_pos - home_pos) / ls,   # Position gap (positive = home better)
            float(home_pts),              # Home points
            float(away_pts),              # Away points
            float(home_gd),               # Home GD
            float(away_gd),               # Away GD
            float(home_pos <= 3),         # Home in top 3
            float(away_pos <= 3),         # Away in top 3
            float(abs(home_pos - away_pos) <= 3),  # Close in table
        ]

    @staticmethod
    def _encode_schedule_context(home_form: List[Dict],
                                  away_form: List[Dict]) -> List[float]:
        """Encode schedule/rest context (6 floats)."""
        home_rest = FeatureEncoder._estimate_rest_days(home_form)
        away_rest = FeatureEncoder._estimate_rest_days(away_form)

        return [
            min(home_rest, 14) / 14.0,  # Normalized home rest
            min(away_rest, 14) / 14.0,  # Normalized away rest
            float(home_rest < 3),       # Home fatigued
            float(away_rest < 3),       # Away fatigued
            float(home_rest > 7),       # Home well rested
            float(away_rest > 7),       # Away well rested
        ]

    @staticmethod
    def _estimate_rest_days(form: List[Dict]) -> float:
        """Estimate days since last match from form dates."""
        if len(form) < 2:
            return 7.0  # Default weekly schedule

        dates = []
        for m in form[:2]:
            d = m.get("date", "")
            try:
                if "-" in d and len(d.split("-")[0]) == 4:
                    dates.append(datetime.strptime(d, "%Y-%m-%d"))
                elif "." in d:
                    dates.append(datetime.strptime(d, "%d.%m.%Y"))
            except (ValueError, AttributeError):
                continue

        if len(dates) >= 2:
            return abs((dates[0] - dates[1]).days)
        return 7.0

    @staticmethod
    def _encode_league_meta(meta: Optional[Dict[str, Any]]) -> List[float]:
        """Encode league-level metadata (4 floats)."""
        if not meta:
            return [0.5, 2.5, 0.45, 0.0]  # Defaults: mid-level, avg goals, home adv

        return [
            meta.get("league_level", 0.5),           # 0=top, 1=amateur
            meta.get("avg_goals_per_match", 2.5),    # League avg goals
            meta.get("home_advantage_factor", 0.45), # Home win %
            meta.get("draw_rate", 0.25),             # League draw rate
        ]
