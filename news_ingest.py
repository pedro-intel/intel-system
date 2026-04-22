# news_ingest.py
# GDELT v2 CSV ingestion — downloads raw event files directly
# Published every 15 minutes at data.gdeltproject.org
# No API key required. Works on Render free tier.

import requests
import csv
import zipfile
import io
from datetime import datetime, timezone

GDELT_LASTUPDATE = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
HEADERS = {"User-Agent": "intel-system/1.0"}

# GDELT v2 CAMEO event codes we care about (conflict/crisis/disaster)
# Full list: https://www.gdeltproject.org/data/documentation/CAMEO.Manual.1.1b3.pdf
RELEVANT_CAMEO = {
    # Material conflict
    "18":  "critical",  # Assault
    "180": "critical",  # Use conventional military force
    "181": "critical",  # Impose blockade / restrict movement
    "182": "critical",  # Occupy territory
    "183": "critical",  # Fight
    "184": "critical",  # Engage in unconventional mass violence
    "185": "critical",  # Assassinate
    "186": "critical",  # Massacre
    "19":  "critical",  # Use unconventional mass violence
    "190": "critical",  # Use unconventional mass violence
    "195": "critical",  # Abduct / Kidnap
    "196": "critical",  # Hijack
    # Coercion
    "17":  "warning",   # Coerce
    "170": "warning",   # Coerce (general)
    "171": "warning",   # Seize / Confiscate
    "172": "warning",   # Arrest / Detain
    "173": "warning",   # Expel / Deport
    "174": "warning",   # Impose sanctions / embargo
    "175": "warning",   # Threaten
    # Protest / Demand
    "14":  "warning",   # Protest
    "140": "warning",   # Engage in political dissent
    "141": "warning",   # Demonstrate / Rally
    "145": "warning",   # Protest violently
    # Military posture
    "15":  "warning",   # Exhibit military posture
    "150": "warning",   # Exhibit military posture (general)
    "151": "warning",   # Increase military alert
    "152": "warning",   # Mobilise / increase troops
    "155": "warning",   # Halt military action
    # Appeal / diplomacy (info)
    "01":  "info",      # Make public statement
    "02":  "info",      # Appeal
    "03":  "info",      # Express intent to cooperate
    "04":  "info",      # Consult
    "05":  "info",      # Engage in diplomatic cooperation
}

# GDELT CSV column indices (v2 export format)
# Full schema: https://www.gdeltproject.org/data/documentation/GDELT-Event_Codebook-V2.0.pdf
COL_EVENTCODE    = 26   # CAMEO event code
COL_ACTOR1NAME   = 6    # Actor 1 name
COL_ACTOR2NAME   = 16   # Actor 2 name
COL_ACTIONGEO_NAME = 53 # Action location name
COL_ACTIONGEO_LAT  = 56 # Action location latitude
COL_ACTIONGEO_LNG  = 57 # Action location longitude
COL_NUMARTICLES    = 33 # Number of articles
COL_AVGTONE        = 34 # Average tone
COL_SOURCEURL      = 60 # Source URL


def get_latest_gdelt_url() -> str | None:
    """
    Fetch the URL of the most recent GDELT v2 export file.
    GDELT publishes a 'lastupdate.txt' with URLs every 15 minutes.
    """
    try:
        res = requests.get(GDELT_LASTUPDATE, headers=HEADERS, timeout=10)
        lines = res.text.strip().split("\n")
        # lastupdate.txt has 3 lines: export, mentions, gkg
        # We want the first line (export CSV)
        for line in lines:
            if "export.CSV" in line:
                # Format: "size hash url"
                parts = line.strip().split(" ")
                if len(parts) >= 3:
                    return parts[2]
    except Exception as e:
        print(f"⚠️ Failed to get GDELT lastupdate: {e}")
    return None


def download_gdelt_events(url: str) -> list:
    """
    Download and parse a GDELT v2 export CSV zip file.
    Returns list of raw event dicts with coordinates and metadata.
    """
    try:
        print(f"⬇️  Downloading GDELT: {url.split('/')[-1]}")
        res = requests.get(url, headers=HEADERS, timeout=30)
        res.raise_for_status()

        # Unzip in memory
        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            filename = z.namelist()[0]
            with z.open(filename) as f:
                content = f.read().decode("utf-8")

        events = []
        reader = csv.reader(io.StringIO(content), delimiter="\t")

        for row in reader:
            try:
                if len(row) < 61:
                    continue

                event_code = row[COL_EVENTCODE].strip()
                lat_str    = row[COL_ACTIONGEO_LAT].strip()
                lng_str    = row[COL_ACTIONGEO_LNG].strip()

                # Skip events without coordinates
                if not lat_str or not lng_str:
                    continue

                lat = float(lat_str)
                lng = float(lng_str)

                # Skip invalid coordinates
                if lat == 0.0 and lng == 0.0:
                    continue
                if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                    continue

                # Only keep relevant CAMEO codes
                severity = None
                for code, sev in RELEVANT_CAMEO.items():
                    if event_code.startswith(code):
                        severity = sev
                        break

                if not severity:
                    continue

                location  = row[COL_ACTIONGEO_NAME].strip()
                actor1    = row[COL_ACTOR1NAME].strip()
                actor2    = row[COL_ACTOR2NAME].strip()
                num_arts  = int(row[COL_NUMARTICLES]) if row[COL_NUMARTICLES].strip() else 1
                tone      = float(row[COL_AVGTONE]) if row[COL_AVGTONE].strip() else 0.0
                source    = row[COL_SOURCEURL].strip() if len(row) > COL_SOURCEURL else ""

                events.append({
                    "lat":        lat,
                    "lng":        lng,
                    "location":   location,
                    "actor1":     actor1,
                    "actor2":     actor2,
                    "event_code": event_code,
                    "severity":   severity,
                    "num_articles": num_arts,
                    "tone":       tone,
                    "source_url": source,
                })

            except (ValueError, IndexError):
                continue

        print(f"✅ Parsed {len(events)} relevant events from GDELT")
        return events

    except Exception as e:
        print(f"⚠️ GDELT download/parse error: {e}")
        return []


def build_message(event: dict) -> str:
    """
    Build a human-readable message from GDELT event fields.
    """
    location = event.get("location", "Unknown location")
    actor1   = event.get("actor1", "")
    actor2   = event.get("actor2", "")
    code     = event.get("event_code", "")
    severity = event.get("severity", "info")

    # Map CAMEO root codes to readable descriptions
    code_descriptions = {
        "18": "armed conflict", "180": "military action", "181": "blockade",
        "182": "territorial occupation", "183": "fighting", "184": "mass violence",
        "185": "assassination", "186": "massacre", "19": "mass violence",
        "190": "mass violence", "195": "kidnapping", "196": "hijacking",
        "17": "coercive action", "170": "coercion", "171": "seizure",
        "172": "arrest/detention", "173": "expulsion", "174": "sanctions",
        "175": "threat", "14": "protest", "140": "political dissent",
        "141": "demonstration", "145": "violent protest",
        "15": "military posture", "150": "military action",
        "151": "military alert", "152": "troop mobilization",
        "155": "military halt",
    }

    action = "activity"
    for c, desc in code_descriptions.items():
        if code.startswith(c):
            action = desc
            break

    if actor1 and actor2:
        return f"{actor1} — {action} involving {actor2} in {location}"
    elif actor1:
        return f"{actor1} — {action} in {location}"
    else:
        return f"Reported {action} in {location}"


def get_news(max_records: int = 50) -> list:
    """
    Compatibility function for server.py fallback path.
    Returns empty list — GDELT CSV mode doesn't use this.
    """
    return []


def get_gdelt_events() -> list:
    """
    Main function: fetch latest GDELT file and return processed events.
    Returns list of {lat, lng, message, type, location} dicts.
    """
    url = get_latest_gdelt_url()
    if not url:
        print("⚠️ Could not get GDELT file URL")
        return []

    raw_events = download_gdelt_events(url)
    if not raw_events:
        return []

    # Sort by number of articles (most-covered events first)
    raw_events.sort(key=lambda x: x["num_articles"], reverse=True)

    processed = []
    for e in raw_events[:max_records if max_records else 100]:
        processed.append({
            "lat":      e["lat"],
            "lng":      e["lng"],
            "message":  build_message(e),
            "type":     e["severity"],
            "location": e.get("location", ""),
            "source":   "GDELT",
        })

    return processed
