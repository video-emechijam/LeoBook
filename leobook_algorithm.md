# LeoBook Algorithm & Codebase Reference

> **Version**: 6.0 · **Last Updated**: 2026-03-03 · **Architecture**: High-Velocity Concurrent Architecture (Per-Match Pipeline + Adaptive Learning + Neural RL)

This document maps the **execution flow** of [Leo.py](Leo.py) to specific files and functions.

---

## System Architecture

Leo.py is a **pure orchestrator**. It runs an infinite `while True` loop, splitting each cycle into phases:

```
Leo.py (Orchestrator) v6.0
├── Prologue P1 (Sequential Prerequisite):
│   └── Cloud Sync → Outcome Review → Accuracy Report
├── Concurrent Execution:
│   ├── Prologue P2: Accuracy Generation & Final Sync
│   └── Chapter 1→2 Pipeline:
│       ├── Ch1 P1: [Match Worker Node] × MAX_CONCURRENCY
│       │   └── H2H/Standings → League Enrichment → Search Dict → Prediction
│       ├── Ch1 P2: Odds Harvesting & URL Resolution
│       ├── Ch1 P3: Final Sync & Recommendations
│       ├── Ch2 P1: Automated Booking (if session healthy)
│       └── Ch2 P2: Funds & Withdrawal Check
├── Chapter 3 (Sequential Finality):
│   └── Chief Engineer Oversight → Backtest → Final Sync
└── Live Streamer: Isolated parallel task (always-on)
```

---

## Live Streamer (Isolated Parallel Task)

**Objective**: Absolute real-time parity between Flashscore LIVE tab and the Flutter app.

Runs in parallel with the main cycle via `asyncio.create_task()` in its **own isolated Playwright instance** (separate temp data dir to prevent browser conflicts).

1. **Extraction**: [fs_live_streamer.py](Modules/Flashscore/fs_live_streamer.py) `live_score_streamer()`
   - Captures live scores, minutes, and statuses every 60s.
   - Uses `extrasaction='ignore'` in CSV writer to handle schema drift.
2. **Delta-Only Push**: Only rows with actual field changes are pushed to Supabase (prevents 0-propagation spam).
3. **Status Propagation**:
   - Marks fixtures as `live` in `predictions.csv` and `schedules.csv`.
   - **2.5hr Rule**: Matches exceeding `kickoff + 2.5h` are auto-transitioned to `finished`.
   - Supports DD.MM.YYYY and ISO date formats for kick-off time parsing.
4. **App Handshake**: Upserts to `live_scores` table via `SyncManager.batch_upsert()`.

---

## High-Velocity Concurrency

**Objective**: Maximize execution throughput via autonomous per-match worker nodes while maintaining absolute data integrity.

1. **Parallel Orchestration**: `Leo.py` uses `BatchProcessor` to spawn multiple `process_match_task` workers in parallel. Match sorting uses `match_time` field for chronological processing.
2. **Integrated Worker Node**: Each worker executes a strict sequential pipeline:
   - **H2H + Standings**: Core match data extraction.
   - **League Enrichment**: Inline navigation to league pages (deduped by `league_id` per cycle). Handles **Historical Season Extraction** via `/archive/` crawling and **Smart Year Detection** for match dates based on season context.
   - **Search Dict**: JIT metadata enrichment via LLMs (Gemini primary, Grok fallback) with enrichment gate capped at 100 teams max per cycle.
   - **Flashscore ID Integration**: Uses native string IDs (`fs_league_id`, `team_id`) as the spine of the database schema.
   - **Prediction**: Final rule engine analysis once all data is present.
3. **Shared Locking (CSV_LOCK)**: All persistent data access is protected by a global `asyncio.Lock` in [db_helpers.py](Data/Access/db_helpers.py).
4. **Resiliency**: If one match worker fails, other nodes continue processing. Data is saved incrementally per-match.

---

## Prediction Pipeline (Chapter 1 P1)

1. **Discovery**: [fs_schedule.py](Modules/Flashscore/fs_schedule.py) extracts fixture IDs.
   - Implements 2-tier header expansion retry (JS bulk + Locator fallback) to ensure 100% fixture visibility.
2. **Analysis**: [fs_processor.py](Modules/Flashscore/fs_processor.py) collects H2H and Standings data.
3. **Core Engine**: [rule_engine.py](Core/Intelligence/rule_engine.py) `analyze()`
   - **Rule Logic**: [rule_config.py](Core/Intelligence/rule_config.py) defines the logic constraints.
   - **Poisson Predictor**: [goal_predictor.py](Core/Intelligence/goal_predictor.py) handles O/U and BTTS probabilities.
4. **Rule Engine Registry**: [rule_engine_manager.py](Core/Intelligence/rule_engine_manager.py) supports multiple registered engines with `--rule-engine --list`, `--set-default`, and `--backtest` commands.

---

## Outcome Review & Accuracy

1. **Offline Review**: [outcome_reviewer.py](Data/Access/outcome_reviewer.py) first attempts offline resolution using schedule data.
2. **Browser Fallback**: Unresolved matches with kick-off ≥2h in the past are visited via a headless browser for score extraction. Future/in-progress matches are pre-filtered out.
3. **Accuracy Evaluation**: Supports all markets including team-name-based OR patterns for double chance (e.g., "Arsenal or Liverpool" → accurate if game outcome is not a draw).

---

## Adaptive Learning Intelligence

**Objective**: Continuous evolution of prediction rule weights based on historical accuracy.

1. **Feedback Loop**: [outcome_reviewer.py](Data/Access/outcome_reviewer.py) calls `LearningEngine.update_weights()` after every review batch.
2. **Analysis**: [learning_engine.py](Core/Intelligence/learning_engine.py) matches `predictions.csv` outcomes against the reasoning tags used.
3. **Weight Evolution**:
   - Success triggers **positive reinforcement** for specific weights.
   - Failure triggers **penalty** and weight reduction.
   - Updates `learning_weights.json` (per-league) and syncs to Supabase.
4. **Integration**: `RuleEngine.analyze()` loads these adaptive weights via `LearningEngine.load_weights(region_league)`.

---

## Neural RL Engine (`Core/Intelligence/rl/`)

**Objective**: Replace rule-based voting with a learned policy that adapts per-league and per-team, with prediction accuracy as the primary reward signal.

### Architecture

```
FeatureEncoder (vision_data → 192-dim tensor, recency-weighted)
    → SharedTrunk MLP (192→256→256→128, 148K params)
        → LeagueAdapter (LoRA rank=16, ~8K params/league)
            → ConditionedTeamAdapter (league-conditioned MLP + LoRA rank=8)
                → PolicyHead (8 actions: 1X2, O/U, BTTS, no_bet)
                → ValueHead (scalar expected value)
                → StakeHead (Kelly fraction 0-5%)
```

**Key design**: Same team produces different predictions in different competitions. The `ConditionedTeamAdapter` takes a league embedding as conditioning input, so Arsenal's behaviour in the Premier League differs from Arsenal in the Champions League.

### Feature Encoding (192 dims)

| Feature Group | Dims | Details |
|---------------|------|---------|
| xG features | 4 | Home/away/diff/total expected goals |
| Form vectors (×2 teams) | 60 | Last-10 W/D/L with exponential recency weighting |
| Goal stats (×2 teams) | 40 | Scored/conceded patterns, O2.5%, BTTS%, clean sheets |
| H2H summary | 8 | Win rates, avg goals, dominance flags |
| Standings | 10 | Normalized positions, points, GD, top-3 flags |
| Schedule context | 6 | Rest days, fatigue flags |
| League metadata | 4 | Level, avg goals, home advantage, draw rate |
| Reserved padding | 60 | Future-proof expansion |

### Composite Reward Function

```python
reward = (
    1.0 * prediction_accuracy      # Primary: did we predict correctly?
  + 0.6 * (1.0 - brier_score)      # Calibration quality
  + 0.4 * roi_normalized           # Return on investment
)
```

### Training Pipeline (Chronological, No Future Leakage)

1. **Phase 1 — Base Model**: All leagues, chronological day-by-day walk-through
2. **Phase 2 — League Adapters**: Freeze base, train per-league LoRA adapters
3. **Phase 3 — Team Adapters**: Freeze base+league, train league-conditioned team adapters
4. **Phase 4 — Online Learning**: Production updates from new outcome reviews

### Cold-Start Strategy

| Entity | Fallback | Fine-tune threshold |
|--------|----------|--------------------|
| New league | GLOBAL adapter | After 20 matches |
| New/promoted team | League-average adapter | After 5 matches |

### CLI

```bash
python Leo.py --train-rl               # Full chronological training
python Leo.py --train-rl --league ID   # Fine-tune specific league adapter
```

### Storage

| File | Contents |
|------|----------|
| `Data/Store/models/leobook_base.pth` | SharedTrunk + heads (~800KB) |
| `Data/Store/models/adapter_registry.json` | Flashscore ID → index mapping |

---

## LLM Health Management

**Module**: [llm_health_manager.py](Core/Intelligence/llm_health_manager.py)

- **Multi-Key Rotation**: 25+ Gemini API keys rotated round-robin across 5 models.
- **Dual Model Chains**: DESCENDING (pro→flash) for AIGO/predictions, ASCENDING (lite→flash) for SearchDict throughput.
- **Dead Key Persistence**: Keys returning 403 are permanently excluded via `_dead_keys` set that survives ping cycles.
- **Health Pings**: Samples 3 keys every 15 minutes to validate connectivity without exhausting quota.
- **Grok Optional**: Grok API key is optional; system routes to Gemini-only if unconfigured.

---

## UI Documentation (Flutter)

See [leobookapp/README.md](leobookapp/README.md) for the Liquid Glass design specification and widget architecture.

---

*Last updated: March 3, 2026 (v6.0 — Neural RL Architecture)*
*LeoBook Engineering Team*
