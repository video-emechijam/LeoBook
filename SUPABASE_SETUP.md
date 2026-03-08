# Supabase Setup Guide

> **Version**: 7.1 · **Last Updated**: 2026-03-07

## Quick Setup (5 minutes)

### Step 1: Create Supabase Account
1. Go to **https://supabase.com**
2. Click **"Start your project"**
3. Sign up with **GitHub** (recommended) or email

### Step 2: Create Project
1. Click **"New Project"**
2. Fill in details:
   - **Name**: `leobook-production`
   - **Region**: e.g., **Europe (Frankfurt)** for Aba, Nigeria
   - **Pricing Plan**: Free tier is sufficient

### Step 3: Run Database Schema
1. Go to **SQL Editor** (left sidebar)
2. Click **"New Query"**
3. Copy the contents of [`Data/Supabase/supabase_schema.sql`](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Data/Supabase/supabase_schema.sql)
4. Paste and click **"Run"**
5. **MANDATORY (v7.0)**: Create the `computed_standings` VIEW using the SQL found in [`Data/Access/league_db.py`](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Data/Access/league_db.py) under `_COMPUTED_STANDINGS_SQL`.

### Step 4: Get API Credentials
1. Go to **Project Settings** → **API**
2. Copy the **Project URL**, **Anon Key**, and **Service Role Key**.

### Step 5: Configure Environment Files
**Python Backend (`.env`):**
```env
SUPABASE_URL=https://xxxxxxxxxxxxx.supabase.co
SUPABASE_SERVICE_KEY=eyJhbGciOiJIUzI1...
```

---

## Autonomous Sync Architecture (v7.0)

LeoBook v7.0 transitions to an **autonomous, event-driven sync strategy** managed by the `TaskScheduler`. Standing tables are NO LONGER synced; they are computed on-the-fly.

### Sync Lifecycle
```
1. Startup Bootstrap: run_startup_sync() ensures DB parity before any other tasks start.
2. Data Readiness: Data Gates (P1-P3) verify coverage before predictions.
3. Pipeline Sync: run_full_sync() executes after significant pipeline milestones (Chapter 1 P3).
4. Live Updates: fs_live_streamer.py performs real-time delta upserts to live_scores.
```

### Tables Synced (11 tables)
*Notice: `standings` has been removed as it is now a computed VIEW.*

| Table                 | Unique Key                                        |
| --------------------- | ------------------------------------------------- |
| `predictions`         | `fixture_id`                                      |
| `schedules`           | `fixture_id`                                      |
| `teams`               | `team_id`                                         |
| `region_league`       | `league_id`                                       |
| `fb_matches`          | `site_match_id`                                   |
| `profiles`            | `id`                                              |
| `custom_rules`        | `id`                                              |
| `accuracy_reports`    | `report_id`                                       |
| `live_scores`         | `fixture_id`                                      |
| `match_odds`          | `fixture_id`, `market_id`, `outcome_name`, `line` |
| `scheduled_tasks`     | `task_id`                                         |
| `readiness_cache`     | `gate_id`                                         |
| `enrichment_queue`    | `id`                                              |
| `season_completeness` | `league_id`, `season`                             |

---

## Computed Standings VIEW
To ensure zero-latency data integrity, v7.0 uses a database VIEW instead of a persistent table.

**Verification**:
```sql
SELECT * FROM computed_standings WHERE league_id = 'YOUR_LEAGUE_ID' LIMIT 20;
```

---

## Security
1. ✅ **Never commit** `.env` files.
2. ✅ Use **Service Role Key** only for the Python backend.
3. ✅ Use **Anon Key** for the Flutter app.

---

---

*Last updated: March 7, 2026 (v7.1 — readiness_cache + enrichment_queue)*
*LeoBook Engineering Team*
