# db.py

import sqlite3
import threading
from datetime import datetime

DB_PATH = "intel.db"

# Use thread-local storage for connections (safe under FastAPI/asyncio + threads)
_local = threading.local()


def get_conn():
    """Return a thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_conn()
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
    """Persist a list of events to the database."""
    for event in events:
        save_event(event)


def get_recent_events(limit: int = 100):
    """Return the most recent events as a list of tuples (lat, lng, message, type, time)."""
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