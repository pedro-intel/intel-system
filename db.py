# db.py

import sqlite3
import threading
from datetime import datetime

DB_PATH = "intel.db"

_local = threading.local()


def get_conn():
    """Return a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db():
    """Create tables, migrating schema if column names changed."""
    conn = get_conn()

    # Detect old schema: had 'lon' instead of 'lng'
    cursor = conn.execute("PRAGMA table_info(events)")
    columns = [row[1] for row in cursor.fetchall()]

    if columns and "lon" in columns and "lng" not in columns:
        print("⚠️ Old schema detected (lon vs lng) — dropping and recreating...")
        conn.execute("DROP TABLE IF EXISTS events")
        conn.commit()

    conn.execute("""
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
    """Persist a single event to the database."""
    try:
        conn = get_conn()
        conn.execute(
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


def save_events(events: list):
    """Persist a list of events."""
    for event in events:
        save_event(event)


def get_recent_events(limit: int = 100):
    """Return most recent events as list of tuples (lat, lng, message, type, time)."""
    try:
        conn = get_conn()
        cursor = conn.execute(
            "SELECT lat, lng, message, type, time FROM events ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        return cursor.fetchall()
    except Exception as e:
        print(f"⚠️ DB get_recent_events error: {e}")
        return []