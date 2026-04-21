import feedparser
import asyncio
import json
import re
import requests

RSS_FEEDS = [
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "https://rss.cnn.com/rss/edition_world.rss"
]


# =========================
# GEOLOCATION (REAL)
# =========================
def geocode_location(place):
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={place}&format=json&limit=1"
        res = requests.get(url, headers={"User-Agent": "intel-system"})
        data = res.json()

        if data:
            return {
                "lat": float(data[0]["lat"]),
                "lng": float(data[0]["lon"])
            }
    except:
        pass
    return None


def extract_location(text):
    words = re.findall(r'\b[A-Z][a-z]+\b', text)

    for word in words:
        if len(word) > 3:
            coords = geocode_location(word)
            if coords:
                return coords

    return None


# =========================
# CLASSIFICATION
# =========================
def classify_event(text):
    text = text.lower()

    critical = ["war", "missile", "attack", "strike", "killed", "explosion"]
    warning = ["military", "tension", "threat", "conflict"]

    for w in critical:
        if w in text:
            return "critical"

    for w in warning:
        if w in text:
            return "warning"

    return "info"


def is_relevant(text):
    keywords = ["war", "military", "attack", "conflict", "missile", "strike"]
    return any(k in text.lower() for k in keywords)


# =========================
# MAIN LOOP
# =========================
async def fetch_news(events, clients):
    print("🔥 INTEL ENGINE STARTED")

    seen = set()

    while True:
        print("📡 scanning feeds...")

        for url in RSS_FEEDS:
            feed = feedparser.parse(url)

            for entry in feed.entries[:10]:

                if entry.title in seen:
                    continue

                if not is_relevant(entry.title):
                    continue

                seen.add(entry.title)

                coords = extract_location(entry.title)
                if not coords:
                    continue

                event = {
                    "lat": coords["lat"],
                    "lng": coords["lng"],
                    "message": entry.title,
                    "type": classify_event(entry.title)
                }

                print("🧠 EVENT:", event["message"])

                events.append(event)

                for client in clients:
                    try:
                        await client.send_text(json.dumps(event))
                    except:
                        pass

        await asyncio.sleep(20)