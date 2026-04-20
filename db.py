import sqlite3

DB = "intel.db"

def init_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        title TEXT,
        threat TEXT,
        lat REAL,
        lon REAL,
        time TEXT
    )
    """)

    conn.commit()
    conn.close()

def save_events(events):
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    for e in events:
        c.execute("INSERT INTO events (title, threat, lat, lon, time) VALUES (?, ?, ?, ?, ?)",
                  (e["title"], e["threat"], e["lat"], e["lon"], e["time"]))

    conn.commit()
    conn.close()

def load_history():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    c.execute("SELECT title, threat, lat, lon, time FROM events ORDER BY id DESC LIMIT 100")
    rows = c.fetchall()

    conn.close()

    return [{"title": r[0], "threat": r[1], "lat": r[2], "lon": r[3], "time": r[4]} for r in rows]