import sqlite3

conn = sqlite3.connect("intel.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lat REAL,
    lng REAL,
    message TEXT,
    type TEXT,
    time TEXT
)
""")

conn.commit()


def save_event(event):
    cursor.execute("""
    INSERT INTO events (lat, lng, message, type, time)
    VALUES (?, ?, ?, ?, ?)
    """, (
        event["lat"],
        event["lng"],
        event["message"],
        event["type"],
        event["time"]
    ))
    conn.commit()


def get_recent_events(limit=100):
    cursor.execute("""
    SELECT lat, lng, message, type, time
    FROM events
    ORDER BY id DESC
    LIMIT ?
    """, (limit,))
    return cursor.fetchall()