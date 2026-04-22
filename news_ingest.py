# news_ingest.py
# NOTE: RSS fetching is now handled directly in server.py's news_loop().
# This module is kept for any standalone use or future expansion.

import feedparser
import requests

RSS_FEEDS = [
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "https://rss.cnn.com/rss/edition_world.rss",
]

RELEVANT_KEYWORDS = [
    "war", "military", "attack", "conflict", "missile",
    "strike", "killed", "explosion", "troops", "crisis",
    "tension", "sanction", "protest", "nuclear", "coup"
]


def is_relevant(text: str) -> bool:
    return any(k in text.lower() for k in RELEVANT_KEYWORDS)


def get_news(max_per_feed: int = 10) -> list:
    """
    Fetch and return relevant news articles from RSS feeds.
    Returns list of dicts: [{title, summary}]
    """
    items = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:max_per_feed]:
                title = entry.get("title", "").strip()
                summary = entry.get("summary", "").strip()

                if not title:
                    continue

                if is_relevant(title + " " + summary):
                    items.append({"title": title, "summary": summary})
        except Exception as e:
            print(f"⚠️ RSS fetch error ({url}): {e}")

    return items


def geocode_location(place: str) -> dict | None:
    """Geocode a place name using Nominatim. Returns {lat, lng} or None."""
    try:
        url = "https://nominatim.openstreetmap.org/search"
        res = requests.get(
            url,
            params={"q": place, "format": "json", "limit": 1},
            headers={"User-Agent": "intel-system/1.0"},
            timeout=3
        )
        data = res.json()
        if data:
            return {"lat": float(data[0]["lat"]), "lng": float(data[0]["lon"])}
    except Exception:
        pass
    return None