# league_db.py: SQLite database layer for league/fixture/team data.
# Part of LeoBook Data — Access Layer
#
# Functions: init_db(), upsert_league(), upsert_fixture(), upsert_team(),
#            mark_league_processed(), get_unprocessed_leagues()

import sqlite3
import json
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Store")
DB_PATH = os.path.join(DB_DIR, "leobook.db")


def get_connection() -> sqlite3.Connection:
    """Get a thread-safe SQLite connection with WAL mode."""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: Optional[sqlite3.Connection] = None) -> sqlite3.Connection:
    """Create all tables if they don't exist. Returns the connection."""
    if conn is None:
        conn = get_connection()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS leagues (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id           TEXT UNIQUE NOT NULL,
            country_code        TEXT,
            continent           TEXT,
            name                TEXT NOT NULL,
            crest               TEXT,
            current_season      TEXT,
            url                 TEXT,
            processed           INTEGER DEFAULT 0,
            last_updated        TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS teams (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            name                TEXT NOT NULL,
            league_ids          JSON,
            crest               TEXT,
            country_code        TEXT,
            url                 TEXT,
            last_updated        TEXT DEFAULT (datetime('now')),
            UNIQUE(name, country_code)
        );

        CREATE TABLE IF NOT EXISTS fixtures (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id          TEXT UNIQUE,
            date                TEXT,
            time                TEXT,
            league_id           INTEGER REFERENCES leagues(id),
            home_team_id        INTEGER REFERENCES teams(id),
            home_team_name      TEXT,
            away_team_id        INTEGER REFERENCES teams(id),
            away_team_name      TEXT,
            home_score          INTEGER,
            away_score          INTEGER,
            extra               JSON,
            league_stage        TEXT,
            match_status        TEXT,
            season              TEXT,
            home_crest          TEXT,
            away_crest          TEXT,
            url                 TEXT,
            last_updated        TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_fixtures_league ON fixtures(league_id);
        CREATE INDEX IF NOT EXISTS idx_fixtures_date ON fixtures(date);
        CREATE INDEX IF NOT EXISTS idx_fixtures_fixture_id ON fixtures(fixture_id);
        CREATE INDEX IF NOT EXISTS idx_leagues_league_id ON leagues(league_id);
    """)
    conn.commit()
    return conn


def upsert_league(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """Insert or update a league. Returns the row id."""
    now = datetime.now().isoformat()
    cur = conn.execute(
        """INSERT INTO leagues (league_id, country_code, continent, name, crest, current_season, url, last_updated)
           VALUES (:league_id, :country_code, :continent, :name, :crest, :current_season, :url, :last_updated)
           ON CONFLICT(league_id) DO UPDATE SET
               country_code = COALESCE(excluded.country_code, leagues.country_code),
               continent    = COALESCE(excluded.continent, leagues.continent),
               name         = COALESCE(excluded.name, leagues.name),
               crest        = COALESCE(excluded.crest, leagues.crest),
               current_season = COALESCE(excluded.current_season, leagues.current_season),
               url          = COALESCE(excluded.url, leagues.url),
               last_updated = excluded.last_updated
        """,
        {
            "league_id": data["league_id"],
            "country_code": data.get("country_code"),
            "continent": data.get("continent"),
            "name": data["name"],
            "crest": data.get("crest"),
            "current_season": data.get("current_season"),
            "url": data.get("url"),
            "last_updated": now,
        },
    )
    conn.commit()
    return cur.lastrowid


def upsert_team(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """Insert or update a team. Returns the row id."""
    now = datetime.now().isoformat()
    league_ids_json = json.dumps(data.get("league_ids", []))
    cur = conn.execute(
        """INSERT INTO teams (name, league_ids, crest, country_code, url, last_updated)
           VALUES (:name, :league_ids, :crest, :country_code, :url, :last_updated)
           ON CONFLICT(name, country_code) DO UPDATE SET
               league_ids   = :league_ids,
               crest        = COALESCE(excluded.crest, teams.crest),
               url          = COALESCE(excluded.url, teams.url),
               last_updated = excluded.last_updated
        """,
        {
            "name": data["name"],
            "league_ids": league_ids_json,
            "crest": data.get("crest"),
            "country_code": data.get("country_code"),
            "url": data.get("url"),
            "last_updated": now,
        },
    )
    conn.commit()
    return cur.lastrowid


def get_team_id(conn: sqlite3.Connection, name: str, country_code: str = None) -> Optional[int]:
    """Look up team id by name (and optionally country_code)."""
    if country_code:
        row = conn.execute(
            "SELECT id FROM teams WHERE name = ? AND country_code = ?", (name, country_code)
        ).fetchone()
    else:
        row = conn.execute("SELECT id FROM teams WHERE name = ?", (name,)).fetchone()
    return row["id"] if row else None


def upsert_fixture(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """Insert or update a fixture. Returns the row id."""
    now = datetime.now().isoformat()
    extra_json = json.dumps(data.get("extra")) if data.get("extra") else None
    fixture_id = data.get("fixture_id", "")

    cur = conn.execute(
        """INSERT INTO fixtures (
               fixture_id, date, time, league_id,
               home_team_id, home_team_name, away_team_id, away_team_name,
               home_score, away_score, extra, league_stage,
               match_status, season, home_crest, away_crest, url, last_updated
           ) VALUES (
               :fixture_id, :date, :time, :league_id,
               :home_team_id, :home_team_name, :away_team_id, :away_team_name,
               :home_score, :away_score, :extra, :league_stage,
               :match_status, :season, :home_crest, :away_crest, :url, :last_updated
           )
           ON CONFLICT(fixture_id) DO UPDATE SET
               date           = COALESCE(excluded.date, fixtures.date),
               time           = COALESCE(excluded.time, fixtures.time),
               home_score     = COALESCE(excluded.home_score, fixtures.home_score),
               away_score     = COALESCE(excluded.away_score, fixtures.away_score),
               extra          = COALESCE(excluded.extra, fixtures.extra),
               match_status   = COALESCE(excluded.match_status, fixtures.match_status),
               home_crest     = COALESCE(excluded.home_crest, fixtures.home_crest),
               away_crest     = COALESCE(excluded.away_crest, fixtures.away_crest),
               last_updated   = excluded.last_updated
        """,
        {
            "fixture_id": fixture_id,
            "date": data.get("date"),
            "time": data.get("time"),
            "league_id": data.get("league_id"),
            "home_team_id": data.get("home_team_id"),
            "home_team_name": data.get("home_team_name"),
            "away_team_id": data.get("away_team_id"),
            "away_team_name": data.get("away_team_name"),
            "home_score": data.get("home_score"),
            "away_score": data.get("away_score"),
            "extra": extra_json,
            "league_stage": data.get("league_stage"),
            "match_status": data.get("match_status"),
            "season": data.get("season"),
            "home_crest": data.get("home_crest"),
            "away_crest": data.get("away_crest"),
            "url": data.get("url"),
            "last_updated": now,
        },
    )
    conn.commit()
    return cur.lastrowid


def bulk_upsert_fixtures(conn: sqlite3.Connection, fixtures: List[Dict[str, Any]]):
    """Batch insert/update fixtures for performance."""
    now = datetime.now().isoformat()
    rows = []
    for f in fixtures:
        extra_json = json.dumps(f.get("extra")) if f.get("extra") else None
        rows.append((
            f.get("fixture_id", ""), f.get("date"), f.get("time"), f.get("league_id"),
            f.get("home_team_id"), f.get("home_team_name"),
            f.get("away_team_id"), f.get("away_team_name"),
            f.get("home_score"), f.get("away_score"),
            extra_json, f.get("league_stage"),
            f.get("match_status"), f.get("season"),
            f.get("home_crest"), f.get("away_crest"),
            f.get("url"), now,
        ))
    conn.executemany(
        """INSERT INTO fixtures (
               fixture_id, date, time, league_id,
               home_team_id, home_team_name, away_team_id, away_team_name,
               home_score, away_score, extra, league_stage,
               match_status, season, home_crest, away_crest, url, last_updated
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(fixture_id) DO UPDATE SET
               date           = COALESCE(excluded.date, fixtures.date),
               time           = COALESCE(excluded.time, fixtures.time),
               home_score     = COALESCE(excluded.home_score, fixtures.home_score),
               away_score     = COALESCE(excluded.away_score, fixtures.away_score),
               extra          = COALESCE(excluded.extra, fixtures.extra),
               match_status   = COALESCE(excluded.match_status, fixtures.match_status),
               home_crest     = COALESCE(excluded.home_crest, fixtures.home_crest),
               away_crest     = COALESCE(excluded.away_crest, fixtures.away_crest),
               last_updated   = excluded.last_updated
        """,
        rows,
    )
    conn.commit()


def mark_league_processed(conn: sqlite3.Connection, league_id: str):
    """Flag a league as fully scraped."""
    conn.execute(
        "UPDATE leagues SET processed = 1, last_updated = ? WHERE league_id = ?",
        (datetime.now().isoformat(), league_id),
    )
    conn.commit()


def get_unprocessed_leagues(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return all leagues not yet processed."""
    rows = conn.execute(
        "SELECT * FROM leagues WHERE processed = 0 ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_league_db_id(conn: sqlite3.Connection, league_id: str) -> Optional[int]:
    """Get the auto-increment id for a league by its league_id string."""
    row = conn.execute("SELECT id FROM leagues WHERE league_id = ?", (league_id,)).fetchone()
    return row["id"] if row else None
