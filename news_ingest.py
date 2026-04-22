# news_ingest.py
# GDELT v2 CSV ingestion — downloads raw event files directly
# Published every 15 minutes at data.gdeltproject.org
# No API key required. Works on Render free tier.

import requests
import csv
import zipfile
import io

GDELT_LASTUPDATE = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
HEADERS = {"User-Agent": "intel-system/1.0"}

# GDELT uses FIPS country codes — map to readable names
COUNTRY_CODES = {
    "AF": "Afghanistan", "AL": "Albania", "AG": "Algeria", "AO": "Angola",
    "AR": "Argentina", "AM": "Armenia", "AS": "Australia", "AU": "Austria",
    "AJ": "Azerbaijan", "BA": "Bahrain", "BG": "Bangladesh", "BO": "Belarus",
    "BE": "Belgium", "BL": "Bolivia", "BK": "Bosnia", "BR": "Brazil",
    "BU": "Bulgaria", "UV": "Burkina Faso", "BM": "Burma", "CB": "Cambodia",
    "CM": "Cameroon", "CA": "Canada", "CI": "Chile", "CH": "China",
    "CO": "Colombia", "CF": "Congo", "HR": "Croatia", "CU": "Cuba",
    "EZ": "Czech Republic", "DA": "Denmark", "EC": "Ecuador", "EG": "Egypt",
    "ET": "Ethiopia", "FI": "Finland", "FR": "France", "GG": "Georgia",
    "GM": "Germany", "GH": "Ghana", "GR": "Greece", "GT": "Guatemala",
    "GV": "Guinea", "HA": "Haiti", "HO": "Honduras", "HU": "Hungary",
    "IN": "India", "ID": "Indonesia", "IR": "Iran", "IZ": "Iraq",
    "EI": "Ireland", "IS": "Israel", "IT": "Italy", "JA": "Japan",
    "JO": "Jordan", "KZ": "Kazakhstan", "KE": "Kenya", "KV": "Kosovo",
    "KU": "Kuwait", "KG": "Kyrgyzstan", "LA": "Laos", "LG": "Latvia",
    "LE": "Lebanon", "LY": "Libya", "LH": "Lithuania", "LU": "Luxembourg",
    "MY": "Malaysia", "ML": "Mali", "MX": "Mexico", "MD": "Moldova",
    "MG": "Mongolia", "MO": "Morocco", "MZ": "Mozambique", "WA": "Namibia",
    "NP": "Nepal", "NL": "Netherlands", "NU": "Nicaragua", "NG": "Niger",
    "NI": "Nigeria", "KN": "North Korea", "NO": "Norway", "PK": "Pakistan",
    "PS": "Palestine", "PM": "Panama", "PE": "Peru", "RP": "Philippines",
    "PL": "Poland", "PO": "Portugal", "QA": "Qatar", "RO": "Romania",
    "RS": "Russia", "RW": "Rwanda", "SA": "Saudi Arabia", "SG": "Senegal",
    "RB": "Serbia", "SL": "Sierra Leone", "SO": "Somalia", "SF": "South Africa",
    "KS": "South Korea", "OD": "South Sudan", "SP": "Spain", "CE": "Sri Lanka",
    "SU": "Sudan", "SW": "Sweden", "SZ": "Switzerland", "SY": "Syria",
    "TW": "Taiwan", "TI": "Tajikistan", "TZ": "Tanzania", "TH": "Thailand",
    "TS": "Tunisia", "TU": "Turkey", "TX": "Turkmenistan", "UG": "Uganda",
    "UP": "Ukraine", "AE": "UAE", "UK": "United Kingdom", "US": "United States",
    "UY": "Uruguay", "UZ": "Uzbekistan", "VE": "Venezuela", "VM": "Vietnam",
    "YM": "Yemen", "ZA": "Zambia", "ZI": "Zimbabwe", "GQ": "Equatorial Guinea",
    "BH": "Bahrain", "PG": "Papua New Guinea", "FJ": "Fiji",
}

# GDELT v2 CAMEO event codes we care about
RELEVANT_CAMEO = {
    "18":  "critical", "180": "critical", "181": "critical",
    "182": "critical", "183": "critical", "184": "critical",
    "185": "critical", "186": "critical", "19":  "critical",
    "190": "critical", "195": "critical", "196": "critical",
    "17":  "warning",  "170": "warning",  "171": "warning",
    "172": "warning",  "173": "warning",  "174": "warning",
    "175": "warning",  "14":  "warning",  "140": "warning",
    "141": "warning",  "145": "warning",  "15":  "warning",
    "150": "warning",  "151": "warning",  "152": "warning",
    "155": "warning",
}

# CAMEO root code → readable action description
CODE_DESCRIPTIONS = {
    "18": "armed conflict",    "180": "military action",
    "181": "blockade imposed", "182": "territory occupied",
    "183": "active fighting",  "184": "mass violence",
    "185": "assassination",    "186": "massacre",
    "19": "mass violence",     "190": "mass violence",
    "195": "kidnapping",       "196": "hijacking",
    "17": "coercive action",   "170": "coercion",
    "171": "seizure",          "172": "arrest/detention",
    "173": "expulsion",        "174": "sanctions imposed",
    "175": "threat issued",    "14": "protest",
    "140": "political dissent","141": "demonstration",
    "145": "violent protest",  "15": "military posture",
    "150": "military action",  "151": "military alert raised",
    "152": "troop mobilization","155": "military halt",
    "01": "statement issued",  "02": "appeal made",
    "03": "cooperation intent","04": "diplomatic talks",
    "05": "diplomatic cooperation",
}

# CSV column indices (GDELT v2 export format)
COL_EVENTCODE      = 26
COL_ACTOR1NAME     = 6
COL_ACTOR2NAME     = 16
COL_ACTIONGEO_NAME = 53
COL_ACTIONGEO_CC   = 55  # Country code
COL_ACTIONGEO_LAT  = 56
COL_ACTIONGEO_LNG  = 57
COL_NUMARTICLES    = 33
COL_AVGTONE        = 34
COL_SOURCEURL      = 60


def resolve_country(code: str, location_name: str) -> str:
    """Convert country code to full name, falling back to location_name."""
    if code and code.upper() in COUNTRY_CODES:
        return COUNTRY_CODES[code.upper()]
    return location_name or code or "Unknown location"


def get_action_description(event_code: str) -> str:
    """Get human-readable action from CAMEO code."""
    for code, desc in CODE_DESCRIPTIONS.items():
        if event_code.startswith(code):
            return desc
    return "activity"


def build_message(event: dict) -> str:
    """Build a clean human-readable message from GDELT event fields."""
    country  = event.get("country_name", "Unknown location")
    location = event.get("location", "")
    actor1   = event.get("actor1", "").title()
    actor2   = event.get("actor2", "").title()
    action   = event.get("action", "activity")

    # Use specific location if available and different from country
    place = location if location and location.lower() != country.lower() else country

    # Clean up generic actor names
    skip_actors = {"", "None", "Citizen", "People", "Individual"}
    a1 = actor1 if actor1 not in skip_actors else ""
    a2 = actor2 if actor2 not in skip_actors else ""

    if a1 and a2:
        return f"{a1} — {action} involving {a2} in {place}"
    elif a1:
        return f"{a1} — {action} in {place}"
    else:
        return f"{action.capitalize()} reported in {place}"


def get_latest_gdelt_url() -> str | None:
    """Fetch the URL of the most recent GDELT v2 export file."""
    try:
        res = requests.get(GDELT_LASTUPDATE, headers=HEADERS, timeout=10)
        for line in res.text.strip().split("\n"):
            if "export.CSV" in line:
                parts = line.strip().split(" ")
                if len(parts) >= 3:
                    return parts[2]
    except Exception as e:
        print(f"⚠️ Failed to get GDELT lastupdate: {e}")
    return None


def download_gdelt_events(url: str) -> list:
    """Download and parse a GDELT v2 export CSV zip file."""
    try:
        print(f"⬇️  Downloading GDELT: {url.split('/')[-1]}")
        res = requests.get(url, headers=HEADERS, timeout=30)
        res.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            with z.open(z.namelist()[0]) as f:
                content = f.read().decode("utf-8")

        events = []
        reader = csv.reader(io.StringIO(content), delimiter="\t")

        for row in reader:
            try:
                if len(row) < 58:
                    continue

                event_code = row[COL_EVENTCODE].strip()
                lat_str    = row[COL_ACTIONGEO_LAT].strip()
                lng_str    = row[COL_ACTIONGEO_LNG].strip()

                if not lat_str or not lng_str:
                    continue

                lat = float(lat_str)
                lng = float(lng_str)

                if lat == 0.0 and lng == 0.0:
                    continue
                if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                    continue

                severity = None
                for code, sev in RELEVANT_CAMEO.items():
                    if event_code.startswith(code):
                        severity = sev
                        break

                if not severity:
                    continue

                country_code  = row[COL_ACTIONGEO_CC].strip() if len(row) > COL_ACTIONGEO_CC else ""
                location_name = row[COL_ACTIONGEO_NAME].strip()
                country_name  = resolve_country(country_code, location_name)
                actor1        = row[COL_ACTOR1NAME].strip()
                actor2        = row[COL_ACTOR2NAME].strip()
                num_arts      = int(row[COL_NUMARTICLES]) if row[COL_NUMARTICLES].strip().isdigit() else 1
                action        = get_action_description(event_code)

                events.append({
                    "lat":          lat,
                    "lng":          lng,
                    "location":     location_name,
                    "country_name": country_name,
                    "actor1":       actor1,
                    "actor2":       actor2,
                    "event_code":   event_code,
                    "severity":     severity,
                    "action":       action,
                    "num_articles": num_arts,
                })

            except (ValueError, IndexError):
                continue

        print(f"✅ Parsed {len(events)} relevant events from GDELT")
        return events

    except Exception as e:
        print(f"⚠️ GDELT download/parse error: {e}")
        return []


def get_news(max_records: int = 50) -> list:
    """Compatibility stub — not used in GDELT CSV mode."""
    return []


def get_gdelt_events() -> list:
    """Fetch latest GDELT file and return processed events."""
    url = get_latest_gdelt_url()
    if not url:
        print("⚠️ Could not get GDELT file URL")
        return []

    raw_events = download_gdelt_events(url)
    if not raw_events:
        return []

    # Sort by coverage — most-reported events first
    raw_events.sort(key=lambda x: x["num_articles"], reverse=True)

    processed = []
    for e in raw_events[:100]:
        processed.append({
            "lat":      e["lat"],
            "lng":      e["lng"],
            "message":  build_message(e),
            "type":     e["severity"],
            "location": e.get("country_name", e.get("location", "")),
            "source":   "GDELT",
        })

    return processed
