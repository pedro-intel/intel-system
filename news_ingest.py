# news_ingest.py
# ACLED API integration — human-verified conflict data
# Uses OAuth authentication with email/password

import os
import requests
from datetime import datetime, timedelta

ACLED_EMAIL    = os.getenv("ACLED_EMAIL")
ACLED_PASSWORD = os.getenv("ACLED_PASSWORD")
TOKEN_URL      = "https://acleddata.com/oauth/token"
API_URL        = "https://acleddata.com/api/acled/read"

HEADERS = {"User-Agent": "intel-system/1.0", "Content-Type": "application/json"}

# Cache token to avoid re-authenticating every cycle
_token_cache = {"token": None, "expires_at": None}


def get_access_token() -> str | None:
    """Get OAuth access token, using cache if still valid."""
    now = datetime.utcnow()

    # Return cached token if still valid (expires in 24h, refresh after 23h)
    if _token_cache["token"] and _token_cache["expires_at"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    if not ACLED_EMAIL or not ACLED_PASSWORD:
        print("⚠️ ACLED_EMAIL or ACLED_PASSWORD not set")
        return None

    try:
        print("🔑 Authenticating with ACLED...")
        res = requests.post(TOKEN_URL, headers={"Content-Type": "application/x-www-form-urlencoded"}, data={
            "username":   ACLED_EMAIL,
            "password":   ACLED_PASSWORD,
            "grant_type": "password",
            "client_id":  "acled",
            "scope":      "authenticated",
        }, timeout=15)

        if res.status_code == 200:
            data = res.json()
            token = data.get("access_token")
            _token_cache["token"]      = token
            _token_cache["expires_at"] = now + timedelta(hours=23)
            print("✅ ACLED authenticated successfully")
            return token
        else:
            print(f"⚠️ ACLED auth failed: {res.status_code} {res.text[:200]}")
            return None

    except Exception as e:
        print(f"⚠️ ACLED auth error: {e}")
        return None


def classify_event(event_type: str, sub_event_type: str) -> str:
    """Map ACLED event types to our severity levels."""
    event_type_lower = event_type.lower()
    sub_lower        = sub_event_type.lower()

    critical_types = {
        "battles", "explosions/remote violence", "violence against civilians"
    }
    critical_subs = {
        "armed clash", "attack", "air/drone strike", "shelling/artillery/missile attack",
        "suicide bomb", "grenade", "chemical weapon", "shooting", "abduction/forced disappearance",
        "mob violence", "mass killing"
    }
    warning_types = {
        "riots", "strategic developments"
    }
    warning_subs = {
        "violent demonstration", "looting/property destruction", "forceful seizure",
        "arrest", "siege", "disrupted weapons use", "non-violent transfer of territory"
    }

    if event_type_lower in critical_types or sub_lower in critical_subs:
        return "critical"
    if event_type_lower in warning_types or sub_lower in warning_subs:
        return "warning"
    return "info"


def build_message(event: dict) -> str:
    """Build a clean human-readable message from ACLED event fields."""
    actor1    = event.get("actor1", "").strip()
    actor2    = event.get("actor2", "").strip()
    sub_event = event.get("sub_event_type", "").strip()
    location  = event.get("location", "").strip()
    country   = event.get("country", "").strip()
    fatalities = int(event.get("fatalities", 0) or 0)

    place = f"{location}, {country}" if location and location != country else country

    # Build message
    if actor1 and actor2:
        msg = f"{actor1} — {sub_event.lower()} involving {actor2} in {place}"
    elif actor1:
        msg = f"{actor1} — {sub_event.lower()} in {place}"
    else:
        msg = f"{sub_event.capitalize()} in {place}"

    # Add fatalities if significant
    if fatalities > 0:
        msg += f" ({fatalities} {'fatality' if fatalities == 1 else 'fatalities'})"

    return msg


def get_gdelt_events() -> list:
    """
    Fetch recent conflict events from ACLED API.
    Returns list of {lat, lng, message, type, location, source} dicts.
    """
    token = get_access_token()
    if not token:
        print("⚠️ No ACLED token — skipping fetch")
        return []

    # Fetch last 7 days of events
    since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")

    params = {
        "_format":         "json",
        "event_date":      f"{since}|{today}",
        "event_date_where": "BETWEEN",
        "fields":          "event_id_cnty|event_date|event_type|sub_event_type|actor1|actor2|country|location|latitude|longitude|fatalities|disorder_type",
        "limit":           500,
    }

    try:
        print(f"🌍 Fetching ACLED events since {since}...")
        res = requests.get(
            API_URL,
            params=params,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=30,
        )

        if res.status_code != 200:
            print(f"⚠️ ACLED API error: {res.status_code} {res.text[:200]}")
            return []

        data = res.json()
        if data.get("status") != 200:
            print(f"⚠️ ACLED API returned status: {data.get('status')}")
            return []

        raw_events = data.get("data", [])
        print(f"✅ ACLED returned {len(raw_events)} events")

        processed = []
        seen = set()

        for e in raw_events:
            try:
                lat = float(e.get("latitude") or 0)
                lng = float(e.get("longitude") or 0)

                if lat == 0.0 and lng == 0.0:
                    continue
                if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                    continue

                event_type = e.get("event_type", "")
                sub_event  = e.get("sub_event_type", "")
                country    = e.get("country", "Unknown")
                location   = e.get("location", "")
                severity   = classify_event(event_type, sub_event)
                message    = build_message(e)

                # Deduplicate by location + event type
                key = f"{round(lat,2)},{round(lng,2)}:{event_type}"
                if key in seen:
                    continue
                seen.add(key)

                processed.append({
                    "lat":      lat,
                    "lng":      lng,
                    "message":  message,
                    "type":     severity,
                    "location": country,
                    "source":   "ACLED",
                })

            except (ValueError, TypeError):
                continue

        # Sort by severity — critical first
        order = {"critical": 0, "warning": 1, "info": 2}
        processed.sort(key=lambda x: order.get(x["type"], 2))

        print(f"✅ {len(processed)} quality ACLED events ready")
        return processed[:100]

    except Exception as e:
        print(f"⚠️ ACLED fetch error: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_news(max_records: int = 50) -> list:
    """Compatibility stub."""
    return []
