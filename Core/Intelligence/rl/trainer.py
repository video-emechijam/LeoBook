# trainer.py: PPO-based RL trainer with chronological training and composite reward.
# Part of LeoBook Core — Intelligence (RL Engine)
#
# Classes: RLTrainer
# Called by: Leo.py (--train-rl)

"""
RL Trainer Module
Handles offline training from historical fixtures (chronological, day-by-day)
and online updates from new prediction outcomes.

Training constraints:
- Strict chronological order (no future data leakage)
- Max 2-season lookback window
- Last-10 matches prioritized via recency weighting
- Prediction accuracy is the primary reward signal
"""

import os
import json
import torch
import torch.nn as nn
import torch.optim as optim
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

from .model import LeoBookRLModel
from .feature_encoder import FeatureEncoder
from .adapter_registry import AdapterRegistry

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "Data" / "Store" / "models"

# Paths
BASE_MODEL_PATH = MODELS_DIR / "leobook_base.pth"
TRAINING_CONFIG_PATH = MODELS_DIR / "training_config.json"

# --- Market Likelihood Priors ---
_LIKELIHOOD_JSON_PATH = PROJECT_ROOT / "ranked_markets_likelihood_updated_with_team_ou.json"
_MARKET_LIKELIHOODS = {}  # populated lazily

def _load_market_likelihoods() -> dict:
    """Load market likelihood JSON once and build action-index lookup."""
    global _MARKET_LIKELIHOODS
    if _MARKET_LIKELIHOODS:
        return _MARKET_LIKELIHOODS
    try:
        with open(_LIKELIHOOD_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for entry in data.get("ranked_market_outcomes", []):
            key = entry.get("market_outcome", "")
            _MARKET_LIKELIHOODS[key] = entry.get("likelihood_percent", 50) / 100.0
    except Exception:
        pass
    return _MARKET_LIKELIHOODS

# Map RL action indices to their market likelihood keys
_ACTION_LIKELIHOOD_KEYS = {
    0: "1X2 - 1",            # home_win  ~46%
    1: "1X2 - X",            # draw      ~27%  (not in JSON directly, use default)
    2: "1X2 - 2",            # away_win  ~27%
    3: "Over/Under - Over (2.5 line)",   # over_2.5  ~55%
    4: "Over/Under - Under (2.5 line)",  # under_2.5 ~45%
    5: "GG/NG - GG",         # btts_yes  ~54%
    6: "GG/NG - NG",         # btts_no   ~46%
    7: None,                  # no_bet    N/A
}

# Default likelihoods for actions not found in JSON
_ACTION_DEFAULT_LIKELIHOODS = {
    0: 0.46, 1: 0.27, 2: 0.27,
    3: 0.55, 4: 0.45,
    5: 0.54, 6: 0.46,
    7: 0.0,
}

def get_action_likelihood(action_idx: int) -> float:
    """Get historical base likelihood for a given action index."""
    likelihoods = _load_market_likelihoods()
    key = _ACTION_LIKELIHOOD_KEYS.get(action_idx)
    if key and key in likelihoods:
        return likelihoods[key]
    return _ACTION_DEFAULT_LIKELIHOODS.get(action_idx, 0.5)


class RLTrainer:
    """
    PPO-based trainer for the LeoBook RL model.

    Training proceeds chronologically day-by-day:
    For each day D:
        1. Build features using ONLY data before D (max 2 seasons back)
        2. For each fixture on day D:
            a. Encode features
            b. Model predicts action (market + stake)
            c. After outcome: compute composite reward
            d. PPO gradient update
    """

    def __init__(
        self,
        lr_base: float = 5e-5,
        lr_league: float = 1e-4,
        lr_team: float = 5e-5,
        gamma: float = 0.99,
        clip_epsilon: float = 0.2,
        max_seasons_back: int = 2,
        device: str = "cpu",
    ):
        self.device = torch.device(device)
        self.max_seasons_back = max_seasons_back
        self.gamma = gamma
        self.clip_epsilon = clip_epsilon

        # Model & registry
        self.model = LeoBookRLModel().to(self.device)
        self.registry = AdapterRegistry()

        # Optimizer with per-component learning rates
        param_groups = [
            {"params": self.model.trunk.parameters(), "lr": lr_base},
            {"params": self.model.policy_head.parameters(), "lr": lr_base},
            {"params": self.model.value_head.parameters(), "lr": lr_base},
            {"params": self.model.stake_head.parameters(), "lr": lr_base},
            {"params": self.model.league_embedding.parameters(), "lr": lr_league},
            {"params": self.model.league_adapters.parameters(), "lr": lr_league},
            {"params": self.model.team_adapters.parameters(), "lr": lr_team},
        ]
        self.optimizer = optim.AdamW(param_groups, weight_decay=1e-4)
        self.scheduler = optim.lr_scheduler.CosineAnnealingWarmRestarts(
            self.optimizer, T_0=1000, T_mult=2
        )

        self._step_count = 0

    # -------------------------------------------------------------------
    # Composite Reward (prediction accuracy is primary)
    # -------------------------------------------------------------------

    @staticmethod
    def _get_correct_actions(outcome: Dict[str, Any]) -> set:
        """Map actual outcome to the set of correct action indices."""
        result = outcome.get("result", "")
        home_score = outcome.get("home_score", 0)
        away_score = outcome.get("away_score", 0)
        total_goals = home_score + away_score

        correct = set()
        if result == "home_win":
            correct.add(0)
        elif result == "draw":
            correct.add(1)
        elif result == "away_win":
            correct.add(2)

        if total_goals > 2:
            correct.add(3)  # over_2.5
        else:
            correct.add(4)  # under_2.5

        if home_score > 0 and away_score > 0:
            correct.add(5)  # btts_yes
        else:
            correct.add(6)  # btts_no

        return correct

    @staticmethod
    def compute_reward(
        predicted_action: int,
        actual_outcome: Dict[str, Any],
        pred_probs: Optional[torch.Tensor] = None,
    ) -> float:
        """
        Composite reward with prediction accuracy as the backbone.
        Weighted by market rarity (likelihood priors).
        """
        result = actual_outcome.get("result", "")
        home_score = actual_outcome.get("home_score", 0)
        away_score = actual_outcome.get("away_score", 0)
        total_goals = home_score + away_score

        correct_actions = RLTrainer._get_correct_actions(actual_outcome)
        prediction_correct = 1.0 if predicted_action in correct_actions else -0.5

        # --- 2. Calibration (weight: 0.6) ---
        calibration_score = 0.0
        if pred_probs is not None:
            actual_vec = torch.zeros(3)
            if result == "home_win":
                actual_vec[0] = 1.0
            elif result == "draw":
                actual_vec[1] = 1.0
            elif result == "away_win":
                actual_vec[2] = 1.0

            brier = ((pred_probs[:3] - actual_vec) ** 2).sum().item()
            calibration_score = 1.0 - brier

        # --- 3. ROI component (weight: 0.4) ---
        odds = actual_outcome.get("odds", {})
        roi_score = 0.0
        if predicted_action in correct_actions and predicted_action < 3:
            action_name = LeoBookRLModel.ACTION_NAMES[predicted_action]
            odd_val = odds.get(action_name, 2.0)
            roi_score = (odd_val - 1.0) / odd_val

        # --- 4. Abstention bonus/penalty ---
        if predicted_action == 7:  # no_bet
            if pred_probs is not None and pred_probs[:3].max().item() < 0.4:
                prediction_correct = 0.3
            else:
                prediction_correct = -0.1

        # --- 5. Market rarity multiplier ---
        likelihood = get_action_likelihood(predicted_action)
        if likelihood > 0 and likelihood < 0.20:
            rarity_mult = 2.0
        elif likelihood >= 0.50:
            rarity_mult = 0.5
        else:
            rarity_mult = 1.0

        # --- Composite ---
        raw_reward = (
            1.0 * prediction_correct
            + 0.6 * calibration_score
            + 0.4 * roi_score
        )

        return raw_reward * rarity_mult

    # -------------------------------------------------------------------
    # Training step (PPO)
    # -------------------------------------------------------------------

    # -------------------------------------------------------------------
    # KL Blending (Rule Engine -> Prob Distribution)
    # -------------------------------------------------------------------

    def _get_rule_engine_probs(self, vision_data: Dict[str, Any]) -> torch.Tensor:
        """
        Runs Rule Engine and builds an 8-dim probability distribution.
        Action indices from LeoBookRLModel:
        [0] Home Win   [1] Draw   [2] Away Win
        [3] Over 2.5   [4] Under 2.5
        [5] BTTS Yes   [6] BTTS No
        [7] No Bet
        """
        from ..rule_engine import RuleEngine
        analysis = RuleEngine.analyze(vision_data)
        
        if analysis.get("type") == "SKIP":
            probs = torch.zeros(LeoBookRLModel.NUM_ACTIONS)
            probs[7] = 1.0
            return probs.to(self.device)

        probs = torch.zeros(LeoBookRLModel.NUM_ACTIONS)
        
        # 1. 1X2 Probs (60% budget — dominant market)
        raw = analysis.get("raw_scores", {"home": 0, "draw": 0, "away": 0})
        scores = torch.tensor([float(raw["home"]), float(raw["draw"]), float(raw["away"])])
        # If all scores are 0 (no signal), use uniform
        if scores.sum() == 0:
            x12_probs = torch.tensor([1/3, 1/3, 1/3]) * 0.6
        else:
            x12_probs = torch.softmax(scores / 2.0, dim=0) * 0.6
        probs[0:3] = x12_probs

        # 2. Over/Under 2.5 (15% budget)
        o25_label = analysis.get("over_2.5", "50/50")
        if o25_label == "YES": o25, u25 = 0.8, 0.2
        elif o25_label == "NO": o25, u25 = 0.2, 0.8
        else: o25, u25 = 0.5, 0.5
        probs[3] = o25 * 0.15
        probs[4] = u25 * 0.15

        # 3. BTTS (15% budget)
        btts_label = analysis.get("btts", "50/50")
        if btts_label == "YES": b_yes, b_no = 0.8, 0.2
        elif btts_label == "NO": b_yes, b_no = 0.2, 0.8
        else: b_yes, b_no = 0.5, 0.5
        probs[5] = b_yes * 0.15
        probs[6] = b_no * 0.15

        # 4. No Bet (10% baseline — prevents overconfidence)
        probs[7] = 0.10

        return (probs / probs.sum()).to(self.device)

    def train_step(
        self,
        features: torch.Tensor,
        league_idx: int,
        home_team_idx: int,
        away_team_idx: int,
        outcome: Optional[Dict[str, Any]] = None,
        expert_probs: Optional[torch.Tensor] = None,
        use_kl: bool = False,
    ) -> Dict[str, float]:
        """
        Single training step for Phase 1 (Imitation) or Phase 2/3 (PPO).
        """
        self.model.train()
        features = features.to(self.device)

        # Forward pass
        policy_logits, value, stake = self.model(
            features, league_idx, home_team_idx, away_team_idx
        )
        action_probs = torch.softmax(policy_logits, dim=-1)

        total_loss = torch.tensor(0.0, device=self.device)
        metrics = {}

        if expert_probs is not None and outcome is None:
            # --- Phase 1: Imitation Learning ---
            if expert_probs.dim() == 1:
                expert_probs = expert_probs.unsqueeze(0)
            loss = nn.functional.cross_entropy(policy_logits, expert_probs)

            # KL divergence as monitoring metric + regulariser
            kl_div = torch.sum(
                expert_probs * (torch.log(expert_probs + 1e-10) - torch.log(action_probs + 1e-10))
            )
            total_loss = loss + 0.1 * kl_div

            rl_action = torch.argmax(action_probs, dim=-1).item()
            metrics["imitation_loss"] = loss.item()
            metrics["kl_div"] = kl_div.item()
            metrics["action"] = rl_action
            metrics["rule_engine_acc"] = 1.0 if rl_action == torch.argmax(expert_probs).item() else 0.0

        # --- B. RL / PPO (Phase 2 & 3) ---
        elif outcome is not None:
            dist = torch.distributions.Categorical(action_probs)
            action = dist.sample()
            log_prob = dist.log_prob(action)

            reward = self.compute_reward(action.item(), outcome, action_probs.detach().squeeze())
            reward_tensor = torch.tensor([reward], dtype=torch.float32, device=self.device)
            advantage = reward_tensor - value.squeeze(-1)

            ratio = torch.exp(log_prob - log_prob.detach())
            clipped = torch.clamp(ratio, 1.0 - self.clip_epsilon, 1.0 + self.clip_epsilon)
            policy_loss = -torch.min(ratio * advantage.detach(), clipped * advantage.detach()).mean()
            value_loss = nn.functional.mse_loss(value.squeeze(-1), reward_tensor)
            entropy_bonus = -0.01 * dist.entropy().mean()

            total_loss = policy_loss + 0.5 * value_loss + entropy_bonus

            if use_kl and expert_probs is not None:
                kl_div = torch.sum(expert_probs * (torch.log(expert_probs + 1e-10) - torch.log(action_probs + 1e-10)))
                total_loss += 0.1 * kl_div
                metrics["kl_div"] = kl_div.item()

            metrics.update({
                "policy_loss": policy_loss.item(),
                "value_loss": value_loss.item(),
                "reward": reward,
                "action": action.item(),
            })

        # Backward + optimize
        self.optimizer.zero_grad()
        total_loss.backward()
        torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=0.5)
        self.optimizer.step()
        self.scheduler.step()

        self._step_count += 1
        metrics.update({"total_loss": total_loss.item(), "step": self._step_count})
        return metrics

    # -------------------------------------------------------------------
    # Chronological training from fixtures
    # -------------------------------------------------------------------

    def train_from_fixtures(self, phase: int = 1, cold: bool = False, limit_days: Optional[int] = None, resume: bool = False):
        """
        3-Phase Chronological Training:
        Phase 1: Imitation Learning (Warm-start from Rule Engine)
        Phase 2: PPO with KL penalty (Fine-tune with constraints)
        Phase 3: Adapter Fine-tuning (League specialization, frozen trunk)
        """
        from Data.Access.db_helpers import _get_conn
        
        conn = _get_conn()
        os.makedirs(MODELS_DIR, exist_ok=True)

        print("\n  ============================================================")
        print(f"  RL TRAINING — PHASE {phase} {'(COLD START)' if cold else ''}")
        print("  ============================================================\n")

        if phase == 3:
            print("  [TRAIN] Freezing Shared Trunk... training Adapters only.")
            for param in self.model.trunk.parameters():
                param.requires_grad = False
        elif cold and phase == 1:
            print("  [TRAIN] Cold start requested. Skipping Imitation, proceeding to vanilla PPO.")

        # Get all fixture dates
        cursor = conn.execute("""
            SELECT DISTINCT date FROM schedules
            WHERE date IS NOT NULL AND home_score IS NOT NULL AND away_score IS NOT NULL
            ORDER BY date ASC
        """)
        all_dates = [row[0] for row in cursor.fetchall()]
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        cutoff = datetime.strptime(all_dates[-1], "%Y-%m-%d") - timedelta(days=self.max_seasons_back * 365)
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        all_dates = [d for d in all_dates if d >= cutoff_str and d <= today_str]

        if limit_days:
            all_dates = all_dates[:limit_days]

        print(f"  [TRAIN] Window: {all_dates[0]} → {all_dates[-1]} ({len(all_dates)} days)")

        # --- Checkpoint setup ---
        CHECKPOINT_DIR = MODELS_DIR / "checkpoints"
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        latest_path = MODELS_DIR / f"phase{phase}_latest.pth"
        total_matches_global = 0
        total_correct_global = 0
        start_day_idx = 0

        # --- Resume from checkpoint ---
        if resume and latest_path.exists():
            try:
                ckpt = torch.load(latest_path, map_location=self.device, weights_only=False)
                self.model.load_state_dict(ckpt["model_state"], strict=False)
                self.optimizer.load_state_dict(ckpt["optimizer_state"])
                start_day_idx = ckpt["day"]
                total_matches_global = ckpt.get("total_matches", 0)
                total_correct_global = ckpt.get("correct_predictions", 0)
                print(f"  [RESUME] ✓ Loaded checkpoint from Day {start_day_idx}/{len(all_dates)} ({ckpt.get('match_date', '?')})")
                print(f"  [RESUME]   Matches so far: {total_matches_global} | Correct: {total_correct_global}")
                all_dates = all_dates[start_day_idx:]  # Skip completed days
                if not all_dates:
                    print(f"  [RESUME] All days already completed. Nothing to do.")
                    return
            except Exception as e:
                print(f"  [RESUME] Failed to load checkpoint: {e} — starting fresh")
                start_day_idx = 0

        # Phase 1 LR reduction: imitation needs 10x lower LR than PPO exploration
        original_lrs = []
        if phase == 1 and not cold:
            for pg in self.optimizer.param_groups:
                original_lrs.append(pg['lr'])
                pg['lr'] = pg['lr'] * 0.1
            print(f"  [TRAIN] Phase 1 LR reduced 10x for stable imitation (base → {self.optimizer.param_groups[0]['lr']:.2e})")

        for day_offset, match_date in enumerate(all_dates):
            day_idx = start_day_idx + day_offset
            day_matches = 0
            day_reward = 0.0
            day_imit_loss = 0.0
            day_kl = 0.0
            day_rl_acc = 0.0
            day_rule_acc = 0.0
            day_grad_norm = 0.0

            cursor = conn.execute("""
                SELECT 
                    s.fixture_id, s.league_id, s.home_team_id, 
                    COALESCE(NULLIF(s.home_team_name, ''), t1.name) as h_name,
                    s.away_team_id, 
                    COALESCE(NULLIF(s.away_team_name, ''), t2.name) as a_name,
                    s.home_score, s.away_score,
                    s.season
                FROM schedules s
                LEFT JOIN teams t1 ON s.home_team_id = t1.team_id
                LEFT JOIN teams t2 ON s.away_team_id = t2.team_id
                WHERE s.date = ? AND s.home_score IS NOT NULL AND s.away_score IS NOT NULL
            """, (match_date,))
            fixtures = cursor.fetchall()

            for fix in fixtures:
                fixture_id, league_id, home_tid, h_name, away_tid, a_name, h_score, a_score, season = fix
                outcome = {
                    "result": "home_win" if h_score > a_score else "away_win" if a_score > h_score else "draw",
                    "home_score": h_score, "away_score": a_score
                }
                
                l_idx = self.registry.get_league_idx(league_id)
                h_idx = self.registry.get_team_idx(home_tid)
                a_idx = self.registry.get_team_idx(away_tid)
                
                vision_data = self._build_training_vision_data(conn, match_date, league_id, home_tid, h_name, away_tid, a_name, season=season)
                features = FeatureEncoder.encode(vision_data)
                expert_probs = self._get_rule_engine_probs(vision_data)
                
                if phase == 1 and not cold:
                    metrics = self.train_step(features, l_idx, h_idx, a_idx, expert_probs=expert_probs)
                else:
                    use_kl = (phase == 2)
                    metrics = self.train_step(features, l_idx, h_idx, a_idx, outcome=outcome, expert_probs=expert_probs, use_kl=use_kl)

                day_matches += 1
                day_reward += metrics.get("reward", 0.0)
                day_imit_loss += metrics.get("imitation_loss", 0.0)
                day_kl += metrics.get("kl_div", 0.0)

                # Gradient norm tracking
                total_norm = 0.0
                for p in self.model.parameters():
                    if p.grad is not None:
                        total_norm += p.grad.data.norm(2).item() ** 2
                day_grad_norm += total_norm ** 0.5

                # RL Correctness (vs Actual Outcome — all 8 action types)
                action_idx = metrics.get("action", torch.argmax(
                    self.model.get_action_probs(features, l_idx, h_idx, a_idx)
                ).item())
                correct_actions = self._get_correct_actions(outcome)
                expert_pred_idx = torch.argmax(expert_probs).item()

                if day_idx == 0 and day_matches <= 5:
                    print(f"      [DEBUG] {h_name} vs {a_name}")
                    print(f"        Expert probs: {expert_probs.squeeze().detach().cpu().tolist()}")
                    print(f"        Expert pick: {LeoBookRLModel.ACTION_NAMES[expert_pred_idx]} | RL pick: {LeoBookRLModel.ACTION_NAMES[action_idx]}")
                    print(f"        Correct actions: {[LeoBookRLModel.ACTION_NAMES[a] for a in correct_actions]}")
                    print(f"        KL: {metrics.get('kl_div', 0.0):.4f} | Imitation loss: {metrics.get('imitation_loss', 0.0):.4f}")

                # Count RL as correct if its action is in the correct set
                if action_idx in correct_actions:
                    day_rl_acc += 1
                # Count Rule Engine as correct if its argmax is in the correct set
                if expert_pred_idx in correct_actions:
                    day_rule_acc += 1

                self.registry.record_match(league_id, home_tid, away_tid)

            if day_matches > 0:
                rl_acc = (day_rl_acc / day_matches) * 100
                rule_acc = (day_rule_acc / day_matches) * 100
                kl = day_kl / day_matches
                gn = day_grad_norm / day_matches
                if phase == 1 and not cold:
                    il = day_imit_loss / day_matches
                    print(f"  [Day {day_idx+1:2d}/{start_day_idx + len(all_dates)}] Rule Acc: {rule_acc:4.1f}% | RL Acc: {rl_acc:4.1f}% | KL: {kl:5.3f} | ImitLoss: {il:6.4f} | GradNorm: {gn:.4f} | Matches: {day_matches}")
                else:
                    rw = day_reward / day_matches
                    print(f"  [Day {day_idx+1:2d}/{start_day_idx + len(all_dates)}] Rule Acc: {rule_acc:4.1f}% | RL Acc: {rl_acc:4.1f}% | KL: {kl:5.3f} | Reward: {rw:6.3f} | GradNorm: {gn:.4f} | Matches: {day_matches}")

                # --- Save checkpoint after each day ---
                total_matches_global += day_matches
                total_correct_global += int(day_rl_acc)
                ckpt_data = {
                    "day": day_idx + 1,
                    "total_days": start_day_idx + len(all_dates),
                    "match_date": match_date,
                    "model_state": self.model.state_dict(),
                    "optimizer_state": self.optimizer.state_dict(),
                    "total_matches": total_matches_global,
                    "correct_predictions": total_correct_global,
                    "phase": phase,
                }
                torch.save(ckpt_data, CHECKPOINT_DIR / f"phase{phase}_day{day_idx+1:03d}.pth")
                torch.save(ckpt_data, latest_path)

                # Keep only last 5 daily checkpoints
                existing = sorted(CHECKPOINT_DIR.glob(f"phase{phase}_day*.pth"))
                while len(existing) > 5:
                    existing[0].unlink()
                    existing = existing[1:]

        # Restore LR after Phase 1  
        if original_lrs:
            for pg, lr in zip(self.optimizer.param_groups, original_lrs):
                pg['lr'] = lr

        self.save()
        print(f"\n  [TRAIN] Phase {phase} complete. Model saved.")

    def _build_training_vision_data(
        self, conn, match_date: str, league_id: str,
        home_team_id: str, home_team_name: str,
        away_team_id: str, away_team_name: str,
        season: str = None,
    ) -> Dict[str, Any]:
        """
        Build a vision_data dict from historical fixtures for training.
        Uses ONLY data before match_date (no future leakage).
        """
        from Data.Access.league_db import computed_standings

        # Get last 10 home team matches before this date
        home_form = self._get_team_form(conn, home_team_id, home_team_name, match_date)
        away_form = self._get_team_form(conn, away_team_id, away_team_name, match_date)
        h2h = self._get_h2h(conn, home_team_id, away_team_id, match_date)

        # P0 Fix 3: Reconstruct historical standings as of match_date
        standings = []
        if league_id:
            try:
                standings = computed_standings(
                    conn=conn, league_id=league_id,
                    season=season, before_date=match_date
                )
            except Exception:
                standings = []

        return {
            "h2h_data": {
                "home_team": home_team_name,
                "away_team": away_team_name,
                "home_last_10_matches": home_form,
                "away_last_10_matches": away_form,
                "head_to_head": h2h,
                "region_league": league_id,
            },
            "standings": standings,
        }

    def _get_team_form(self, conn, team_id: str, team_name: str,
                       before_date: str) -> List[Dict]:
        """Get last 10 matches for a team before a given date."""
        cursor = conn.execute("""
            SELECT date, home_team_name, away_team_name, home_score, away_score
            FROM schedules
            WHERE (home_team_id = ? OR away_team_id = ?)
              AND date < ?
              AND home_score IS NOT NULL AND away_score IS NOT NULL
              AND home_score != '' AND away_score != ''
              AND (match_status = 'finished' OR match_status IS NULL)
            ORDER BY date DESC
            LIMIT 10
        """, (team_id, team_id, before_date))

        matches = []
        for row in cursor.fetchall():
            hs, as_ = int(row[3] or 0), int(row[4] or 0)
            winner = "Home" if hs > as_ else "Away" if as_ > hs else "Draw"
            matches.append({
                "date": row[0],
                "home": row[1],
                "away": row[2],
                "score": f"{hs}-{as_}",
                "winner": winner,
            })
        return matches

    def _get_h2h(self, conn, home_id: str, away_id: str,
                 before_date: str) -> List[Dict]:
        """Get H2H matches between two teams before a given date (540-day window)."""
        # P0 Fix 2: Apply 18-month (540-day) cutoff matching live Rule Engine
        cutoff_date = (datetime.strptime(before_date, "%Y-%m-%d")
                       - timedelta(days=540)).strftime("%Y-%m-%d")

        cursor = conn.execute("""
            SELECT date, home_team_name, away_team_name, home_score, away_score
            FROM schedules
            WHERE ((home_team_id = ? AND away_team_id = ?)
                OR (home_team_id = ? AND away_team_id = ?))
              AND date < ?
              AND date >= ?
              AND home_score IS NOT NULL AND away_score IS NOT NULL
              AND home_score != '' AND away_score != ''
              AND (match_status = 'finished' OR match_status IS NULL)
            ORDER BY date DESC
            LIMIT 10
        """, (home_id, away_id, away_id, home_id, before_date, cutoff_date))

        matches = []
        for row in cursor.fetchall():
            hs, as_ = int(row[3] or 0), int(row[4] or 0)
            winner = "Home" if hs > as_ else "Away" if as_ > hs else "Draw"
            matches.append({
                "date": row[0],
                "home": row[1],
                "away": row[2],
                "score": f"{hs}-{as_}",
                "winner": winner,
            })
        return matches

    # -------------------------------------------------------------------
    # Online update (from new prediction outcomes)
    # -------------------------------------------------------------------

    def update_from_outcomes(self, reviewed_predictions: List[Dict[str, Any]]):
        """
        Online learning from new prediction outcomes.
        Called after outcome_reviewer completes a batch.
        """
        if not reviewed_predictions:
            return

        self.load()  # Load latest model
        updated = 0

        for pred in reviewed_predictions:
            if pred.get("outcome_correct") not in ("True", "False", "1", "0"):
                continue

            is_correct = pred.get("outcome_correct") in ("True", "1")

            # Build vision_data from prediction record
            vision_data = {
                "h2h_data": {
                    "home_team": pred.get("home_team", ""),
                    "away_team": pred.get("away_team", ""),
                    "home_last_10_matches": [],
                    "away_last_10_matches": [],
                    "head_to_head": [],
                    "region_league": pred.get("region_league", "GLOBAL"),
                },
                "standings": [],
            }

            features = FeatureEncoder.encode(vision_data)

            league_id = pred.get("region_league", "GLOBAL")
            home_tid = pred.get("home_team_id", "GLOBAL")
            away_tid = pred.get("away_team_id", "GLOBAL")

            l_idx = self.registry.get_league_idx(league_id)
            h_idx = self.registry.get_team_idx(home_tid)
            a_idx = self.registry.get_team_idx(away_tid)

            # Simple reward from correctness
            outcome = {
                "result": "home_win" if is_correct else "draw",  # Simplified
                "home_score": int(pred.get("home_score", 0) or 0),
                "away_score": int(pred.get("away_score", 0) or 0),
            }

            self.train_step(features, l_idx, h_idx, a_idx, outcome)
            updated += 1

        if updated > 0:
            self.save()
            print(f"  [RL] Updated model from {updated} new outcomes")

    # -------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------

    def save(self):
        """Save model and registry."""
        os.makedirs(MODELS_DIR, exist_ok=True)
        torch.save(self.model.state_dict(), BASE_MODEL_PATH)
        self.registry.save()

    def load(self):
        """Load model and registry if they exist."""
        if BASE_MODEL_PATH.exists():
            try:
                state_dict = torch.load(BASE_MODEL_PATH, map_location=self.device, weights_only=True)
                self.model.load_state_dict(state_dict, strict=False)
            except Exception as e:
                print(f"  [RL] Could not load model: {e}")

        self.registry = AdapterRegistry()  # Reloads from disk
