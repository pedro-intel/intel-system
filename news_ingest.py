# news_ingest.py
# GDELT-powered news ingestion — replaces RSS feeds
# Uses GDELT DOC 2.0 API (articles) + GEO 2.0 API (coordinates)
# No API key required. Updated every 15 minutes by GDELT.

import requests
from datetime import datetime

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_GEO_URL = "https://api.gdeltproject.org/api/v2/geo/geo"

# Intelligence-focused query — conflict, disasters, military, political crises
INTEL_QUERY = (
    "war OR attack OR conflict OR military OR missile OR "
    "explosion OR protest OR coup OR invasion OR earthquake OR "
    "hurricane OR flood OR disaster OR sanction OR nuclear OR "
    "troops OR airstrike OR assassination OR hostage OR ceasefire"
)

HEADERS = {"User-Agent": "intel-system/1.0"}


def get_news(max_records: int = 50) -> list:
    """
    Fetch relevant articles from GDELT DOC 2.0 API.
    Returns list of {title, url, sourcecountry, seendate} dicts.
    """
    try:
        params = {
            "query": INTEL_QUERY,
            "mode": "artlist",
            "maxrecords": max_records,
            "timespan": "1h",         # Last 1 hour only — keeps it fresh
            "sort": "datedesc",
            "format": "json",
            "sourcelang": "english",  # English sources only
        }
        res = requests.get(GDELT_DOC_URL, params=params,
                           headers=HEADERS, timeout=10)
        data = res.json()
        articles = data.get("articles", [])

        return [
            {
                "title":         a.get("title", "").strip(),
                "url":           a.get("url", ""),
                "sourcecountry": a.get("sourcecountry", ""),
                "seendate":      a.get("seendate", ""),
            }
            for a in articles
            if a.get("title", "").strip()
        ]

    except Exception as e:
        print(f"⚠️ GDELT DOC API error: {e}")
        return []


def get_geo_events(timespan: str = "1h") -> list:
    """
    Fetch geo-tagged location points from GDELT GEO 2.0 API.
    Returns list of {name, lat, lng, count, context} dicts.
    These are locations mentioned in articles matching the query,
    with real coordinates from GDELT's NLP pipeline.
    """
    try:
        params = {
            "query":    INTEL_QUERY,
            "timespan": timespan,
            "format":   "json",
        }
        res = requests.get(GDELT_GEO_URL, params=params,
                           headers=HEADERS, timeout=10)
        data = res.json()
        features = data.get("features", [])

        points = []
        for f in features:
            props = f.get("properties", {})
            geom  = f.get("geometry", {})
            coords = geom.get("coordinates", [])

            if len(coords) >= 2:
                points.append({
                    "name":    props.get("name", "Unknown"),
                    "lat":     float(coords[1]),   # GeoJSON is [lng, lat]
                    "lng":     float(coords[0]),
                    "count":   props.get("count", 1),
                    "context": props.get("context", ""),
                })

        # Sort by article count — most-covered locations first
        points.sort(key=lambda x: x["count"], reverse=True)
        return points

    except Exception as e:
        print(f"⚠️ GDELT GEO API error: {e}")
        return []


def is_relevant(text: str) -> bool:
    """Quick pre-filter to skip clearly irrelevant articles."""
    keywords = [
        "war", "military", "attack", "conflict", "missile", "strike",
        "killed", "explosion", "troops", "crisis", "protest", "coup",
        "invasion", "earthquake", "flood", "hurricane", "disaster",
        "sanction", "nuclear", "hostage", "ceasefire", "airstrike"
    ]
    text_lower = text.lower()
    return any(k in text_lower for k in keywords)
