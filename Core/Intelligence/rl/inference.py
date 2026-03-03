# inference.py: Fast inference wrapper for the RL model.
# Part of LeoBook Core — Intelligence (RL Engine)
#
# Classes: RLPredictor
# Called by: rule_engine_manager.py (as "rl_v1" engine)

"""
RL Predictor Module
Provides sub-millisecond inference that returns predictions in the same format
as RuleEngine.analyze() for drop-in compatibility with the existing pipeline.
"""

import torch
from typing import Dict, Any, Optional
from pathlib import Path

from .model import LeoBookRLModel
from .feature_encoder import FeatureEncoder
from .adapter_registry import AdapterRegistry

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "Data" / "Store" / "models"
BASE_MODEL_PATH = MODELS_DIR / "leobook_base.pth"


class RLPredictor:
    """
    Fast inference wrapper for the LeoBook RL model.

    Returns predictions in the EXACT same format as RuleEngine.analyze()
    so it's a drop-in replacement via RuleEngineManager.

    Usage:
        predictor = RLPredictor()
        result = predictor.predict(vision_data, fs_league_id, home_team_id, away_team_id)
    """

    _instance: Optional["RLPredictor"] = None

    def __init__(self):
        self.model: Optional[LeoBookRLModel] = None
        self.registry = AdapterRegistry()
        self.device = torch.device("cpu")
        self._loaded = False

    @classmethod
    def get_instance(cls) -> "RLPredictor":
        """Singleton access for cached model."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _ensure_loaded(self) -> bool:
        """Load model if not already loaded. Returns False if model doesn't exist."""
        if self._loaded:
            return True

        if not BASE_MODEL_PATH.exists():
            return False

        try:
            self.model = LeoBookRLModel().to(self.device)
            state_dict = torch.load(
                BASE_MODEL_PATH, map_location=self.device, weights_only=True
            )
            self.model.load_state_dict(state_dict, strict=False)
            self.model.eval()
            self._loaded = True
            return True
        except Exception as e:
            print(f"  [RL] Failed to load model: {e}")
            return False

    def predict(
        self,
        vision_data: Dict[str, Any],
        fs_league_id: str = "GLOBAL",
        home_team_id: str = "GLOBAL",
        away_team_id: str = "GLOBAL",
    ) -> Dict[str, Any]:
        """
        Generate predictions in the same format as RuleEngine.analyze().

        Args:
            vision_data: Same dict that RuleEngine.analyze() receives.
            fs_league_id: Flashscore league ID string.
            home_team_id: Flashscore home team ID string.
            away_team_id: Flashscore away team ID string.

        Returns:
            Prediction dict compatible with the existing pipeline.
        """
        if not self._ensure_loaded():
            return {
                "type": "SKIP",
                "confidence": "Low",
                "reason": ["RL model not trained yet — run: python Leo.py --train-rl"],
            }

        h2h_data = vision_data.get("h2h_data", {})
        home_team = h2h_data.get("home_team", "Unknown")
        away_team = h2h_data.get("away_team", "Unknown")

        # Encode features
        features = FeatureEncoder.encode(vision_data)
        features = features.to(self.device)

        # Get adapter indices
        l_idx = self.registry.get_league_idx(fs_league_id)
        h_idx = self.registry.get_team_idx(home_team_id)
        a_idx = self.registry.get_team_idx(away_team_id)

        # Inference (no gradient)
        with torch.no_grad():
            policy_logits, value, stake = self.model(features, l_idx, h_idx, a_idx)
            action_probs = torch.softmax(policy_logits, dim=-1).squeeze()
            predicted_action = action_probs.argmax().item()
            confidence_score = action_probs[predicted_action].item()

        action_name = LeoBookRLModel.ACTION_NAMES[predicted_action]
        ev = value.item()
        kelly = stake.item() * 0.05  # Scale to 0-5%

        # --- Map to existing pipeline format ---
        prediction_text = self._action_to_prediction_text(
            action_name, home_team, away_team
        )
        market_type = self._action_to_market_type(action_name)

        # Confidence label
        if confidence_score > 0.75:
            confidence_label = "Very High"
        elif confidence_score > 0.60:
            confidence_label = "High"
        elif confidence_score > 0.45:
            confidence_label = "Medium"
        else:
            confidence_label = "Low"

        # Handle abstention
        if action_name == "no_bet":
            return {
                "type": "SKIP",
                "confidence": "Low",
                "reason": [f"RL model recommends no bet (EV: {ev:.3f})"],
            }

        # xG from features
        home_form = [m for m in h2h_data.get("home_last_10_matches", []) if m][:10]
        away_form = [m for m in h2h_data.get("away_last_10_matches", []) if m][:10]
        home_xg = FeatureEncoder._compute_xg(home_form, home_team, is_home=True)
        away_xg = FeatureEncoder._compute_xg(away_form, away_team, is_home=False)

        # Recommendation score
        rec_score = int(confidence_score * 85) + (10 if ev > 0 else 0)
        rec_score = min(rec_score, 100)

        # Build reasoning
        reasoning = [
            f"RL model ({action_name}, conf={confidence_score:.2f})",
            f"EV: {ev:.3f}, Kelly: {kelly*100:.1f}%",
        ]

        # 1X2 probabilities for downstream
        p_home = action_probs[0].item()
        p_draw = action_probs[1].item()
        p_away = action_probs[2].item()

        return {
            "market_prediction": prediction_text,
            "type": prediction_text,
            "market_type": market_type,
            "confidence": confidence_label,
            "recommendation_score": rec_score,
            "market_reliability": round(confidence_score * 100, 1),
            "reason": reasoning,
            "xg_home": round(home_xg, 2),
            "xg_away": round(away_xg, 2),
            "btts": "YES" if action_probs[5].item() > action_probs[6].item() else "NO",
            "over_2.5": "YES" if action_probs[3].item() > action_probs[4].item() else "NO",
            "best_score": "1-0" if p_home > max(p_draw, p_away) else "0-1" if p_away > p_draw else "1-1",
            "top_scores": [],
            "home_tags": [],
            "away_tags": [],
            "h2h_tags": [],
            "standings_tags": [],
            "ml_confidence": confidence_score,
            "betting_markets": {},
            "h2h_n": 0,
            "home_form_n": len(home_form),
            "away_form_n": len(away_form),
            "total_xg": round(home_xg + away_xg, 2),
            # RL-specific fields
            "rl_action_probs": {
                name: round(action_probs[i].item(), 4)
                for i, name in enumerate(LeoBookRLModel.ACTION_NAMES)
            },
            "rl_expected_value": round(ev, 4),
            "rl_kelly_fraction": round(kelly, 4),
        }

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _action_to_prediction_text(action: str, home: str, away: str) -> str:
        """Convert RL action to human-readable prediction text."""
        mapping = {
            "home_win": f"{home} to Win",
            "draw": "Draw",
            "away_win": f"{away} to Win",
            "over_2.5": "Over 2.5 Goals",
            "under_2.5": "Under 2.5 Goals",
            "btts_yes": "Both Teams to Score - Yes",
            "btts_no": "Both Teams to Score - No",
            "no_bet": "No Bet",
        }
        return mapping.get(action, action)

    @staticmethod
    def _action_to_market_type(action: str) -> str:
        """Convert RL action to market type string."""
        mapping = {
            "home_win": "1X2",
            "draw": "1X2",
            "away_win": "1X2",
            "over_2.5": "Over/Under",
            "under_2.5": "Over/Under",
            "btts_yes": "BTTS",
            "btts_no": "BTTS",
            "no_bet": "ABSTAIN",
        }
        return mapping.get(action, "1X2")

    @staticmethod
    def is_available() -> bool:
        """Check if the RL model has been trained and is available."""
        return BASE_MODEL_PATH.exists()
