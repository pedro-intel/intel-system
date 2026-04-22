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
            _local.pg_conn = psycopg2.connect(DATABASE_URL, sslmode="require")
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
                message TEXT,
                type    TEXT,
                time    TEXT
            )
        """)
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
                message TEXT,
                type    TEXT,
                time    TEXT
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
                "INSERT INTO events (lat, lng, message, type, time) VALUES (%s, %s, %s, %s, %s)",
                (
                    event.get("lat"),
                    event.get("lng"),
                    event.get("message", ""),
                    event.get("type", "info"),
                    event.get("time", datetime.utcnow().isoformat()),
                )
            )
        else:
            cur.execute(
                "INSERT INTO events (lat, lng, message, type, time) VALUES (?, ?, ?, ?, ?)",
                (
                    event.get("lat"),
                    event.get("lng"),
                    event.get("message", ""),
                    event.get("type", "info"),
                    event.get("time", datetime.utcnow().isoformat()),
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
                "SELECT lat, lng, message, type, time FROM events ORDER BY id DESC LIMIT %s",
                (limit,)
            )
        else:
            cur.execute(
                "SELECT lat, lng, message, type, time FROM events ORDER BY id DESC LIMIT ?",
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
                WHERE time >= NOW() - INTERVAL '%s hours'
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
