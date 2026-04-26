# db.py
# Supports both PostgreSQL (production) and SQLite (local dev)
# Set DATABASE_URL env var on Render to use PostgreSQL automatically

import os
import threading
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")  # Set this on Render
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras
    print("🐘 Using PostgreSQL")
else:
    import sqlite3
    print("🗄️ Using SQLite (local dev)")

_local = threading.local()


def get_conn():
    """Return a connection — PostgreSQL or SQLite depending on env."""
    if USE_POSTGRES:
        # PostgreSQL: create a new connection per thread
        if not hasattr(_local, "pg_conn") or _local.pg_conn is None or _local.pg_conn.closed:
            try:
                _local.pg_conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=10)
            except Exception:
                _local.pg_conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        return _local.pg_conn
    else:
        # SQLite: thread-local connection
        if not hasattr(_local, "conn") or _local.conn is None:
            _local.conn = sqlite3.connect("intel.db", check_same_thread=False)
            _local.conn.row_factory = sqlite3.Row
        return _local.conn


def init_db():
    """Create tables if they don't exist. Handles both Postgres and SQLite."""
    conn = get_conn()
    cur = conn.cursor()

    if USE_POSTGRES:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id      SERIAL PRIMARY KEY,
                lat     DOUBLE PRECISION NOT NULL,
                lng     DOUBLE PRECISION NOT NULL,
                message  TEXT,
                type     TEXT,
                time     TEXT,
                source   TEXT DEFAULT 'Unknown',
                location TEXT DEFAULT 'Unknown'
            )
        """)
        # Safely add columns if they don't exist
        for col, typ in [("source", "TEXT DEFAULT 'Unknown'"), ("location", "TEXT DEFAULT 'Unknown'")]:
            try:
                cur.execute(f"ALTER TABLE events ADD COLUMN IF NOT EXISTS {col} {typ}")
            except Exception:
                pass
    else:
        # SQLite: detect and migrate old schema (lon → lng)
        cur.execute("PRAGMA table_info(events)")
        columns = [row[1] for row in cur.fetchall()]
        if columns and "lon" in columns and "lng" not in columns:
            print("⚠️ Old schema detected — migrating...")
            cur.execute("DROP TABLE IF EXISTS events")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                lat     REAL    NOT NULL,
                lng     REAL    NOT NULL,
                message  TEXT,
                type     TEXT,
                time     TEXT,
                source   TEXT DEFAULT 'Unknown',
                location TEXT DEFAULT 'Unknown'
            )
        """)

    conn.commit()
    print("✅ Database ready")


# Auto-initialize on import
init_db()


def save_event(event: dict):
    """Persist a single event."""
    try:
        conn = get_conn()
        cur = conn.cursor()

        if USE_POSTGRES:
            cur.execute(
                "INSERT INTO events (lat, lng, message, type, time, source, location) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    event.get("lat"),
                    event.get("lng"),
                    event.get("message", ""),
                    event.get("type", "info"),
                    event.get("time", datetime.utcnow().isoformat()),
                    event.get("source", "Unknown"),
                    event.get("location", "Unknown"),
                )
            )
        else:
            cur.execute(
                "INSERT INTO events (lat, lng, message, type, time, source, location) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    event.get("lat"),
                    event.get("lng"),
                    event.get("message", ""),
                    event.get("type", "info"),
                    event.get("time", datetime.utcnow().isoformat()),
                    event.get("source", "Unknown"),
                    event.get("location", "Unknown"),
                )
            )

        conn.commit()
    except Exception as e:
        print(f"⚠️ DB save_event error: {e}")
        # Reset broken connection
        if USE_POSTGRES:
            _local.pg_conn = None
        else:
            _local.conn = None


def save_events(events: list):
    """Persist a list of events."""
    for event in events:
        save_event(event)


def get_recent_events(limit: int = 100):
    """Return most recent events as list of tuples (lat, lng, message, type, time)."""
    try:
        conn = get_conn()
        cur = conn.cursor()

        if USE_POSTGRES:
            cur.execute(
                "SELECT lat, lng, message, type, time, source, location FROM events ORDER BY id DESC LIMIT %s",
                (limit,)
            )
        else:
            cur.execute(
                "SELECT lat, lng, message, type, time, source, location FROM events ORDER BY id DESC LIMIT ?",
                (limit,)
            )

        return cur.fetchall()
    except Exception as e:
        print(f"⚠️ DB get_recent_events error: {e}")
        return []


def get_events_since(hours: int = 24) -> list:
    """
    Return all events from the last N hours, ordered oldest-first.
    Used by the timeline slider API.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        if USE_POSTGRES:
            cur.execute("""
                SELECT lat, lng, message, type, time
                FROM events
                WHERE time::timestamp >= NOW() - INTERVAL '1 hour' * %s
                ORDER BY time ASC
            """, (hours,))
        else:
            cur.execute("""
                SELECT lat, lng, message, type, time
                FROM events
                WHERE time >= datetime('now', ? )
                ORDER BY time ASC
            """, (f'-{hours} hours',))

        rows = cur.fetchall()
        return [
            {"lat": r[0], "lng": r[1], "message": r[2], "type": r[3], "time": r[4]}
            for r in rows
        ]
    except Exception as e:
        print(f"⚠️ DB get_events_since error: {e}")
        return []


def cleanup_old_events(hours: int = 24):
    """Delete GDELT events and events older than N hours."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        if USE_POSTGRES:
            # Delete GDELT events
            cur.execute("DELETE FROM events WHERE source IN ('GDELT', 'gdelt')")
            gdelt_deleted = cur.rowcount
            # Delete old events safely
            try:
                cur.execute("""
                    DELETE FROM events 
                    WHERE time IS NOT NULL 
                    AND time != ''
                    AND time::timestamp < NOW() - INTERVAL '1 hour' * %s
                """, (hours,))
                old_deleted = cur.rowcount
            except Exception:
                old_deleted = 0
        else:
            cur.execute("DELETE FROM events WHERE source IN ('GDELT', 'gdelt')")
            gdelt_deleted = cur.rowcount
            try:
                cur.execute("""
                    DELETE FROM events 
                    WHERE time >= '' 
                    AND time < datetime('now', ?)
                """, (f'-{hours} hours',))
                old_deleted = cur.rowcount
            except Exception:
                old_deleted = 0
        conn.commit()
        total = gdelt_deleted + old_deleted
        if total > 0:
            print(f"🧹 Cleaned {gdelt_deleted} GDELT + {old_deleted} old events from DB")
        return total
    except Exception as e:
        print(f"⚠️ DB cleanup error: {e}")
        return 0


def get_seen_keys() -> set:
    """Load seen event keys from DB."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_keys (
                    key TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.commit()
            cur.execute("SELECT key FROM seen_keys WHERE created_at > NOW() - INTERVAL '24 hours'")
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_keys (
                    key TEXT PRIMARY KEY,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.commit()
            cur.execute("SELECT key FROM seen_keys WHERE created_at > datetime('now', '-24 hours')")
        rows = cur.fetchall()
        return set(r[0] for r in rows)
    except Exception as e:
        print(f"⚠️ get_seen_keys error: {e}")
        return set()

def add_seen_key(key: str):
    """Save a seen key to DB."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("INSERT INTO seen_keys (key) VALUES (%s) ON CONFLICT DO NOTHING", (key,))
        else:
            cur.execute("INSERT OR IGNORE INTO seen_keys (key) VALUES (?)", (key,))
        conn.commit()
    except Exception:
        pass

def cleanup_seen_keys():
    """Remove seen keys older than 24 hours."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("DELETE FROM seen_keys WHERE created_at < NOW() - INTERVAL '24 hours'")
        else:
            cur.execute("DELETE FROM seen_keys WHERE created_at < datetime('now', '-24 hours')")
        conn.commit()
    except Exception:
        pass
