# model.py: Neural RL model with shared base, LoRA league adapters,
#            and league-conditioned team adapters.
# Part of LeoBook Core — Intelligence (RL Engine)
#
# Classes: LeagueAdapter, ConditionedTeamAdapter, LeoBookRLModel
# Called by: trainer.py, inference.py

"""
LeoBook RL Model
Implements the shared-base + adapter architecture for context-aware predictions.
Same team produces different behaviour in different competitions.
"""

import torch
import torch.nn as nn
from typing import Dict, Optional, Tuple


class LeagueAdapter(nn.Module):
    """
    LoRA-style low-rank adapter for league-specific behaviour.
    Each league gets its own small adapter (~8K params at rank=16).
    Residual connection: output = x + up(down(x))
    """

    def __init__(self, dim: int = 128, rank: int = 16):
        super().__init__()
        self.down = nn.Linear(dim, rank, bias=False)
        self.up = nn.Linear(rank, dim, bias=False)
        # Initialize up weights to zero so adapter starts as identity
        nn.init.zeros_(self.up.weight)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return x + self.up(self.down(x))


class ConditionedTeamAdapter(nn.Module):
    """
    Team adaptation that is explicitly conditioned on league context.
    Same team will have completely different effective behaviour in
    Premier League vs Champions League.

    Architecture: small MLP conditioner + LoRA delta (rank=8).
    Input: (team_features [B, dim], league_embedding [1, league_dim])
    Output: adapted features [B, dim]
    """

    def __init__(self, dim: int = 128, league_dim: int = 32, rank: int = 8):
        super().__init__()
        self.conditioner = nn.Sequential(
            nn.Linear(dim + league_dim, dim),
            nn.ReLU(),
            nn.Linear(dim, dim),
        )
        self.lora_down = nn.Linear(dim, rank, bias=False)
        self.lora_up = nn.Linear(rank, dim, bias=False)
        nn.init.zeros_(self.lora_up.weight)

    def forward(self, x: torch.Tensor, league_emb: torch.Tensor) -> torch.Tensor:
        # Expand league embedding to match batch size
        league_expanded = league_emb.expand(x.size(0), -1)
        combined = torch.cat([x, league_expanded], dim=-1)
        conditioned = self.conditioner(combined)
        lora_delta = self.lora_up(self.lora_down(conditioned))
        return x + conditioned + lora_delta


class LeoBookRLModel(nn.Module):
    """
    Full RL model for LeoBook predictions.

    Architecture:
        FeatureEncoder (192-dim) → SharedTrunk (192→256→256→128)
        → LeagueAdapter (LoRA rank=16)
        → ConditionedTeamAdapter (league-conditioned, LoRA rank=8)
        → PolicyHead (8-dim action distribution)
        → ValueHead (scalar EV)
        → StakeHead (Kelly fraction 0-5%)

    Policy actions (8 dims):
        [0] Home Win   [1] Draw   [2] Away Win
        [3] Over 2.5   [4] Under 2.5
        [5] BTTS Yes   [6] BTTS No
        [7] No Bet (abstain)
    """

    NUM_ACTIONS = 8
    ACTION_NAMES = [
        "home_win", "draw", "away_win",
        "over_2.5", "under_2.5",
        "btts_yes", "btts_no",
        "no_bet",
    ]

    def __init__(
        self,
        feature_dim: int = 192,
        hidden_dim: int = 256,
        trunk_out_dim: int = 128,
        league_emb_dim: int = 32,
        num_leagues: int = 2000,
        league_lora_rank: int = 16,
        team_lora_rank: int = 8,
    ):
        super().__init__()
        self.trunk_out_dim = trunk_out_dim
        self.league_emb_dim = league_emb_dim

        # --- Shared Trunk ---
        self.trunk = nn.Sequential(
            nn.Linear(feature_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.05),
            nn.Linear(hidden_dim, trunk_out_dim),
            nn.ReLU(),
        )

        # --- League Embedding (used for conditioning team adapters) ---
        self.league_embedding = nn.Embedding(num_leagues, league_emb_dim)

        # --- League Adapters (LoRA) ---
        # Stored as a dict keyed by league index
        self.league_adapters = nn.ModuleDict()
        # The GLOBAL adapter (index 0, used for cold-start)
        self.league_adapters["0"] = LeagueAdapter(trunk_out_dim, league_lora_rank)

        # --- Team Adapters (conditioned on league) ---
        self.team_adapters = nn.ModuleDict()
        # The GLOBAL team adapter (used for cold-start)
        self.team_adapters["0_0"] = ConditionedTeamAdapter(
            trunk_out_dim, league_emb_dim, team_lora_rank
        )

        # --- Output Heads ---
        self.policy_head = nn.Sequential(
            nn.Linear(trunk_out_dim, 64),
            nn.ReLU(),
            nn.Linear(64, self.NUM_ACTIONS),
        )
        self.value_head = nn.Sequential(
            nn.Linear(trunk_out_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
        )
        self.stake_head = nn.Sequential(
            nn.Linear(trunk_out_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid(),  # Output in [0, 1], scaled to [0, 0.05] externally
        )

        self._league_lora_rank = league_lora_rank
        self._team_lora_rank = team_lora_rank

    # -------------------------------------------------------------------
    # Adapter management
    # -------------------------------------------------------------------

    def ensure_league_adapter(self, league_idx: int) -> None:
        """Create a league adapter if it doesn't exist yet (cold-start)."""
        key = str(league_idx)
        if key not in self.league_adapters:
            self.league_adapters[key] = LeagueAdapter(
                self.trunk_out_dim, self._league_lora_rank
            ).to(next(self.parameters()).device)

    def ensure_team_adapter(self, league_idx: int, team_idx: int) -> None:
        """Create a team adapter if it doesn't exist yet (cold-start)."""
        key = f"{league_idx}_{team_idx}"
        if key not in self.team_adapters:
            self.team_adapters[key] = ConditionedTeamAdapter(
                self.trunk_out_dim, self.league_emb_dim, self._team_lora_rank
            ).to(next(self.parameters()).device)

    # -------------------------------------------------------------------
    # Forward pass
    # -------------------------------------------------------------------

    def forward(
        self,
        x: torch.Tensor,
        league_idx: int = 0,
        home_team_idx: int = 0,
        away_team_idx: int = 0,
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        """
        Forward pass with league and team conditioning.

        Args:
            x: Feature tensor [B, feature_dim]
            league_idx: Integer index for the league
            home_team_idx: Integer index for the home team
            away_team_idx: Integer index for the away team

        Returns:
            policy_logits: [B, NUM_ACTIONS]
            value: [B, 1]
            stake_fraction: [B, 1] in [0, 1] (multiply by 0.05 for Kelly %)
        """
        # 1. Shared trunk
        features = self.trunk(x)

        # 2. League adaptation
        self.ensure_league_adapter(league_idx)
        league_key = str(league_idx)
        features = self.league_adapters[league_key](features)

        # 3. League embedding (for conditioning team adapters)
        league_emb = self.league_embedding(
            torch.tensor([league_idx], device=x.device)
        )

        # 4. Team adaptation (home team perspective — primary prediction target)
        self.ensure_team_adapter(league_idx, home_team_idx)
        home_key = f"{league_idx}_{home_team_idx}"
        features_home = self.team_adapters[home_key](features, league_emb)

        # 5. Also condition on away team for richer context
        self.ensure_team_adapter(league_idx, away_team_idx)
        away_key = f"{league_idx}_{away_team_idx}"
        features_away = self.team_adapters[away_key](features, league_emb)

        # 6. Combine home + away team-adapted features
        combined = (features_home + features_away) / 2.0

        # 7. Output heads
        policy_logits = self.policy_head(combined)
        value = self.value_head(combined)
        stake = self.stake_head(combined)

        return policy_logits, value, stake

    def get_action_probs(
        self,
        x: torch.Tensor,
        league_idx: int = 0,
        home_team_idx: int = 0,
        away_team_idx: int = 0,
    ) -> torch.Tensor:
        """Get softmax action probabilities."""
        logits, _, _ = self.forward(x, league_idx, home_team_idx, away_team_idx)
        return torch.softmax(logits, dim=-1)

    def count_parameters(self) -> Dict[str, int]:
        """Count parameters by component."""
        trunk_params = sum(p.numel() for p in self.trunk.parameters())
        head_params = sum(
            p.numel()
            for head in [self.policy_head, self.value_head, self.stake_head]
            for p in head.parameters()
        )
        league_params = sum(
            p.numel() for p in self.league_adapters.parameters()
        )
        team_params = sum(
            p.numel() for p in self.team_adapters.parameters()
        )
        emb_params = sum(p.numel() for p in self.league_embedding.parameters())

        return {
            "trunk": trunk_params,
            "heads": head_params,
            "league_adapters": league_params,
            "team_adapters": team_params,
            "league_embeddings": emb_params,
            "total": trunk_params + head_params + league_params + team_params + emb_params,
        }
