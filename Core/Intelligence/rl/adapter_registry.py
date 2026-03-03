# adapter_registry.py: Manages league and team adapter lookup with cold-start fallback.
# Part of LeoBook Core — Intelligence (RL Engine)
#
# Classes: AdapterRegistry
# Called by: trainer.py, inference.py

"""
Adapter Registry
Maps Flashscore string IDs (fs_league_id, team_id) to integer indices
for the embedding/adapter layers. Handles cold-start registration.
"""

import json
import os
from typing import Dict, Optional, Tuple
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
REGISTRY_PATH = PROJECT_ROOT / "Data" / "Store" / "models" / "adapter_registry.json"

# Reserved indices
GLOBAL_LEAGUE_IDX = 0
GLOBAL_TEAM_IDX = 0


class AdapterRegistry:
    """
    Bidirectional mapping between Flashscore string IDs and integer indices.

    Cold-start strategy:
    - Unknown league → returns GLOBAL_LEAGUE_IDX (0)
    - Unknown team → returns GLOBAL_TEAM_IDX (0)
    - Registers new entities on first encounter for future fine-tuning
    """

    def __init__(self):
        self.league_to_idx: Dict[str, int] = {"GLOBAL": GLOBAL_LEAGUE_IDX}
        self.idx_to_league: Dict[int, str] = {GLOBAL_LEAGUE_IDX: "GLOBAL"}

        self.team_to_idx: Dict[str, int] = {"GLOBAL": GLOBAL_TEAM_IDX}
        self.idx_to_team: Dict[int, str] = {GLOBAL_TEAM_IDX: "GLOBAL"}

        # Track match counts for fine-tuning thresholds
        self.league_match_counts: Dict[str, int] = {}
        self.team_league_match_counts: Dict[str, int] = {}  # "team_id:league_id" → count

        self._load()

    # -------------------------------------------------------------------
    # League operations
    # -------------------------------------------------------------------

    def get_league_idx(self, fs_league_id: str) -> int:
        """Get integer index for a league. Auto-registers if unknown."""
        if not fs_league_id or fs_league_id == "GLOBAL":
            return GLOBAL_LEAGUE_IDX

        if fs_league_id not in self.league_to_idx:
            self._register_league(fs_league_id)

        return self.league_to_idx[fs_league_id]

    def _register_league(self, fs_league_id: str) -> int:
        """Register a new league and return its index."""
        idx = len(self.league_to_idx)
        self.league_to_idx[fs_league_id] = idx
        self.idx_to_league[idx] = fs_league_id
        return idx

    # -------------------------------------------------------------------
    # Team operations
    # -------------------------------------------------------------------

    def get_team_idx(self, team_id: str) -> int:
        """Get integer index for a team. Auto-registers if unknown."""
        if not team_id or team_id == "GLOBAL":
            return GLOBAL_TEAM_IDX

        if team_id not in self.team_to_idx:
            self._register_team(team_id)

        return self.team_to_idx[team_id]

    def _register_team(self, team_id: str) -> int:
        """Register a new team and return its index."""
        idx = len(self.team_to_idx)
        self.team_to_idx[team_id] = idx
        self.idx_to_team[idx] = team_id
        return idx

    # -------------------------------------------------------------------
    # Match counting (for fine-tune thresholds)
    # -------------------------------------------------------------------

    def record_match(self, fs_league_id: str, home_team_id: str, away_team_id: str):
        """Record a match for adapter fine-tuning threshold tracking."""
        self.league_match_counts[fs_league_id] = \
            self.league_match_counts.get(fs_league_id, 0) + 1

        for tid in [home_team_id, away_team_id]:
            key = f"{tid}:{fs_league_id}"
            self.team_league_match_counts[key] = \
                self.team_league_match_counts.get(key, 0) + 1

    def should_finetune_league(self, fs_league_id: str, threshold: int = 20) -> bool:
        """Whether a league has enough matches to warrant fine-tuning."""
        return self.league_match_counts.get(fs_league_id, 0) >= threshold

    def should_finetune_team(self, team_id: str, fs_league_id: str,
                              threshold: int = 5) -> bool:
        """Whether a team-in-league has enough matches for fine-tuning."""
        key = f"{team_id}:{fs_league_id}"
        return self.team_league_match_counts.get(key, 0) >= threshold

    # -------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------

    def save(self):
        """Save registry to disk."""
        os.makedirs(REGISTRY_PATH.parent, exist_ok=True)
        data = {
            "league_to_idx": self.league_to_idx,
            "team_to_idx": self.team_to_idx,
            "league_match_counts": self.league_match_counts,
            "team_league_match_counts": self.team_league_match_counts,
        }
        with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _load(self):
        """Load registry from disk if exists."""
        if not REGISTRY_PATH.exists():
            return

        try:
            with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.league_to_idx = data.get("league_to_idx", self.league_to_idx)
            self.team_to_idx = data.get("team_to_idx", self.team_to_idx)
            self.league_match_counts = data.get("league_match_counts", {})
            self.team_league_match_counts = data.get("team_league_match_counts", {})

            # Rebuild reverse maps
            self.idx_to_league = {v: k for k, v in self.league_to_idx.items()}
            self.idx_to_team = {v: k for k, v in self.team_to_idx.items()}
        except Exception:
            pass  # Start fresh if corrupted

    # -------------------------------------------------------------------
    # Stats
    # -------------------------------------------------------------------

    def stats(self) -> Dict[str, int]:
        """Return registry statistics."""
        return {
            "num_leagues": len(self.league_to_idx),
            "num_teams": len(self.team_to_idx),
            "leagues_with_matches": len(self.league_match_counts),
            "team_league_pairs": len(self.team_league_match_counts),
        }
