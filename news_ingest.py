# news_ingest.py
# GDELT v2 CSV ingestion — high quality filtered events only

import requests
import csv
import zipfile
import io

GDELT_LASTUPDATE = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
HEADERS = {"User-Agent": "intel-system/1.0"}

COUNTRY_CODES = {
    "AF": "Afghanistan", "AL": "Albania", "AG": "Algeria", "AO": "Angola",
    "AR": "Argentina", "AM": "Armenia", "AS": "Australia", "AU": "Austria",
    "AJ": "Azerbaijan", "BH": "Bahrain", "BG": "Bangladesh", "BO": "Belarus",
    "BE": "Belgium", "BL": "Bolivia", "BK": "Bosnia", "BR": "Brazil",
    "BU": "Bulgaria", "UV": "Burkina Faso", "BM": "Burma", "CB": "Cambodia",
    "CM": "Cameroon", "CA": "Canada", "CI": "Chile", "CH": "China",
    "CO": "Colombia", "CF": "Congo", "CG": "DR Congo", "HR": "Croatia",
    "CU": "Cuba", "EZ": "Czech Republic", "DA": "Denmark", "EC": "Ecuador",
    "EG": "Egypt", "ET": "Ethiopia", "FI": "Finland", "FR": "France",
    "GG": "Georgia", "GM": "Germany", "GH": "Ghana", "GR": "Greece",
    "GT": "Guatemala", "GV": "Guinea", "GW": "Guinea-Bissau", "HA": "Haiti",
    "HO": "Honduras", "HU": "Hungary", "IN": "India", "ID": "Indonesia",
    "IR": "Iran", "IZ": "Iraq", "EI": "Ireland", "IS": "Israel",
    "IT": "Italy", "JA": "Japan", "JE": "Jersey", "JO": "Jordan",
    "KZ": "Kazakhstan", "KE": "Kenya", "KV": "Kosovo", "KU": "Kuwait",
    "KG": "Kyrgyzstan", "LA": "Laos", "LG": "Latvia", "LE": "Lebanon",
    "LY": "Libya", "LH": "Lithuania", "LU": "Luxembourg", "MY": "Malaysia",
    "ML": "Mali", "MX": "Mexico", "MD": "Moldova", "MG": "Mongolia",
    "MO": "Morocco", "MZ": "Mozambique", "WA": "Namibia", "NP": "Nepal",
    "NL": "Netherlands", "NZ": "New Zealand", "NU": "Nicaragua",
    "NG": "Niger", "NI": "Nigeria", "KN": "North Korea", "NO": "Norway",
    "PK": "Pakistan", "PA": "Paraguay", "PS": "Palestine", "PM": "Panama",
    "PE": "Peru", "RP": "Philippines", "PL": "Poland", "PO": "Portugal",
    "QA": "Qatar", "RO": "Romania", "RS": "Russia", "RW": "Rwanda",
    "SA": "Saudi Arabia", "SG": "Senegal", "RB": "Serbia", "SN": "Singapore",
    "SL": "Sierra Leone", "SO": "Somalia", "SF": "South Africa",
    "KS": "South Korea", "OD": "South Sudan", "SP": "Spain",
    "CE": "Sri Lanka", "SU": "Sudan", "SW": "Sweden", "SZ": "Switzerland",
    "SY": "Syria", "TW": "Taiwan", "TI": "Tajikistan", "TZ": "Tanzania",
    "TH": "Thailand", "TD": "Chad", "TS": "Tunisia", "TU": "Turkey",
    "TX": "Turkmenistan", "UG": "Uganda", "UP": "Ukraine", "AE": "UAE",
    "UK": "United Kingdom", "US": "United States", "UY": "Uruguay",
    "UZ": "Uzbekistan", "VE": "Venezuela", "VM": "Vietnam", "YM": "Yemen",
    "ZA": "Zambia", "ZI": "Zimbabwe", "GQ": "Equatorial Guinea",
    "PG": "Papua New Guinea", "FJ": "Fiji", "GY": "Guyana",
    "BP": "Solomon Islands", "MU": "Mauritius", "MV": "Maldives",
    "LI": "Liberia", "MR": "Mauritania", "SB": "Serbia",
}

# ── TIGHTENED CAMEO CODES ─────────────────────────────────────────────────────
# Only genuine conflict, military, and serious coercion events
# Removed: 14x (protest), 170/17 (generic coercion) — too noisy
RELEVANT_CAMEO = {
    # CRITICAL — direct violence and military action
    "180": "critical",  # Use conventional military force
    "181": "critical",  # Impose blockade
    "182": "critical",  # Occupy territory
    "183": "critical",  # Fight with small arms
    "184": "critical",  # Use unconventional mass violence
    "185": "critical",  # Assassinate
    "186": "critical",  # Massacre
    "190": "critical",  # Use unconventional mass violence (general)
    "195": "critical",  # Abduct / Kidnap
    "196": "critical",  # Hijack
    "18":  "critical",  # Assault (catch-all, after specifics)
    "19":  "critical",  # Mass violence (catch-all, after specifics)
    # WARNING — serious but not direct violence
    "172": "warning",   # Arrest / Detain
    "173": "warning",   # Expel / Deport
    "174": "warning",   # Impose sanctions / embargo
    "175": "warning",   # Threaten with force
    "145": "warning",   # Protest violently
    "151": "warning",   # Increase military alert
    "152": "warning",   # Mobilize / increase troops
    "171": "warning",   # Seize / Confiscate property
}

CODE_DESCRIPTIONS = {
    "180": "military strike",       "181": "blockade imposed",
    "182": "territory occupied",    "183": "armed clashes",
    "184": "mass violence",         "185": "assassination",
    "186": "massacre",              "190": "mass violence",
    "195": "kidnapping",            "196": "hijacking",
    "18":  "armed assault",         "19":  "mass violence",
    "172": "mass arrests",          "173": "mass expulsion",
    "174": "sanctions imposed",     "175": "military threat",
    "145": "violent protest",       "151": "military alert raised",
    "152": "troop mobilization",    "171": "assets seized",
}

# ── NOISE FILTERS ─────────────────────────────────────────────────────────────
# Actor names that indicate GDELT parsing artifacts, not real actors
JUNK_ACTORS = {
    "MEDIA", "NEWS OUTLET", "CRIMINAL", "COMPANY", "BUSINESS",
    "WEBSITE", "CARRIER", "AIRLINE", "SCHOOL", "UNIVERSITY",
    "COLLEGE", "CHURCH", "HOSPITAL", "COURT", "PRISON",
    "POLICE", "GUNMAN", "VOTER", "KING", "WORKER",
    "RESIDENT", "STUDENT", "JUDGE", "JURY", "LAWYER",
    "PROSECUTOR", "PRODUCER", "ACTOR", "MICROSOFT",
    "GOOGLE", "FACEBOOK", "TWITTER", "INTERNET",
}

# Minimum number of articles covering an event — filters single-source noise
MIN_ARTICLES = 3

# Countries that generate disproportionate domestic noise
HIGH_NOISE_COUNTRIES = {"US", "CA", "AS", "UK", "AU"}

# CSV column indices
COL_EVENTCODE      = 26
COL_ACTOR1NAME     = 6
COL_ACTOR2NAME     = 16
COL_ACTIONGEO_NAME = 53
COL_ACTIONGEO_CC   = 55
COL_ACTIONGEO_LAT  = 56
COL_ACTIONGEO_LNG  = 57
COL_NUMARTICLES    = 33
COL_AVGTONE        = 34


def resolve_country(code: str, location_name: str) -> str:
    if code and code.upper() in COUNTRY_CODES:
        return COUNTRY_CODES[code.upper()]
    return location_name or code or "Unknown location"


def get_action_description(event_code: str) -> str:
    # Match longest code first for specificity
    for length in [3, 2]:
        prefix = event_code[:length]
        if prefix in CODE_DESCRIPTIONS:
            return CODE_DESCRIPTIONS[prefix]
    return CODE_DESCRIPTIONS.get(event_code[:2], "military activity")


def is_junk_actor(actor: str) -> bool:
    return not actor or actor.upper() in JUNK_ACTORS


def build_message(event: dict) -> str:
    country  = event.get("country_name", "Unknown")
    location = event.get("location", "")
    actor1   = event.get("actor1", "").title()
    actor2   = event.get("actor2", "").title()
    action   = event.get("action", "military activity")

    # Use specific city/region if available
    place = location if (location and location.lower() != country.lower()
                         and len(location) > 2) else country

    a1 = "" if is_junk_actor(actor1.upper()) else actor1
    a2 = "" if is_junk_actor(actor2.upper()) else actor2

    if a1 and a2:
        return f"{a1} — {action} involving {a2} in {place}"
    elif a1:
        return f"{a1} — {action} in {place}"
    else:
        return f"{action.capitalize()} in {place}"


def get_latest_gdelt_url() -> str | None:
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

                # Parse core fields first
                event_code   = row[COL_EVENTCODE].strip()
                lat_str      = row[COL_ACTIONGEO_LAT].strip()
                lng_str      = row[COL_ACTIONGEO_LNG].strip()
                country_code = row[COL_ACTIONGEO_CC].strip() if len(row) > COL_ACTIONGEO_CC else ""
                actor1       = row[COL_ACTOR1NAME].strip()
                actor2       = row[COL_ACTOR2NAME].strip()
                num_arts_str = row[COL_NUMARTICLES].strip()
                num_arts     = int(num_arts_str) if num_arts_str.isdigit() else 0

                # ── FILTER 1: must have coordinates ──
                if not lat_str or not lng_str:
                    continue
                lat = float(lat_str)
                lng = float(lng_str)
                if lat == 0.0 and lng == 0.0:
                    continue
                if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                    continue

                # ── FILTER 2: minimum article coverage ──
                if num_arts < MIN_ARTICLES:
                    continue

                # ── FILTER 3: relevant CAMEO code ──
                severity = None
                for code, sev in RELEVANT_CAMEO.items():
                    if event_code.startswith(code):
                        severity = sev
                        break
                if not severity:
                    continue

                # ── FILTER 4: no junk actors from high-noise countries ──
                if country_code in HIGH_NOISE_COUNTRIES:
                    if is_junk_actor(actor1) and is_junk_actor(actor2):
                        continue

                location_name = row[COL_ACTIONGEO_NAME].strip()
                country_name  = resolve_country(country_code, location_name)
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

        print(f"✅ Parsed {len(events)} quality events from GDELT")
        return events

    except Exception as e:
        print(f"⚠️ GDELT download/parse error: {e}")
        return []


def get_news(max_records: int = 50) -> list:
    return []


def get_gdelt_events() -> list:
    url = get_latest_gdelt_url()
    if not url:
        print("⚠️ Could not get GDELT file URL")
        return []

    raw_events = download_gdelt_events(url)
    if not raw_events:
        return []

    # Sort by coverage volume — most-reported events first
    raw_events.sort(key=lambda x: x["num_articles"], reverse=True)

    processed = []
    seen_locations = set()

    for e in raw_events:
        if len(processed) >= 75:
            break

        # Deduplicate by country + action type to avoid map flooding
        dedup_key = f"{e['country_name']}:{e['event_code'][:2]}"
        if dedup_key in seen_locations:
            continue
        seen_locations.add(dedup_key)

        processed.append({
            "lat":      e["lat"],
            "lng":      e["lng"],
            "message":  build_message(e),
            "type":     e["severity"],
            "location": e["country_name"],
            "source":   "GDELT",
        })

    return processed
