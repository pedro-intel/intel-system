# news_ingest.py
# GDELT v2 CSV ingestion — high quality filtered events only

import requests
import csv
import zipfile
import io
import re

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
    "LI": "Liberia", "MR": "Mauritania", "MT": "Malta", "WE": "West Bank",
    "EK": "Ecuador", "BC": "Botswana", "SB": "Serbia",
    "CK": "Cayman Islands", "JM": "Jamaica",
    "BB": "Barbados", "LC": "Saint Lucia", "VC": "Saint Vincent",
    "DO": "Dominican Republic", "CJ": "Cayman Islands",
}

# Only genuine conflict/military CAMEO codes — no deportation, no territory
RELEVANT_CAMEO = {
    "183": "critical",  # Fight with small arms
    "184": "critical",  # Mass violence
    "185": "critical",  # Assassinate
    "186": "critical",  # Massacre
    "190": "critical",  # Mass violence (general)
    "195": "critical",  # Kidnap
    "196": "critical",  # Hijack
    "180": "critical",  # Military force
    "181": "critical",  # Blockade
    "18":  "critical",  # Armed assault (catch-all)
    "19":  "critical",  # Mass violence (catch-all)
    "172": "warning",   # Mass arrests
    "174": "warning",   # Sanctions
    "175": "warning",   # Military threat
    "145": "warning",   # Violent protest
    "151": "warning",   # Military alert
    "152": "warning",   # Troop mobilization
    "171": "warning",   # Assets seized
}

CODE_DESCRIPTIONS = {
    "183": "armed clashes",      "184": "mass violence",
    "185": "assassination",      "186": "massacre",
    "190": "mass violence",      "195": "kidnapping",
    "196": "hijacking",          "180": "military strike",
    "181": "blockade imposed",   "18":  "armed assault",
    "19":  "mass violence",      "172": "mass arrests",
    "174": "sanctions imposed",  "175": "military threat",
    "145": "violent protest",    "151": "military alert raised",
    "152": "troop mobilization", "171": "assets seized",
}

# All actor names that are not real actors
JUNK_ACTORS = {
    # Media/companies
    "MEDIA", "NEWS OUTLET", "COMPANY", "BUSINESS", "WEBSITE",
    "CARRIER", "AIRLINE", "RYANAIR", "MICROSOFT", "GOOGLE",
    "FACEBOOK", "TWITTER", "INTERNET", "NEWS AGENCY",
    # Generic people
    "CRIMINAL", "GUNMAN", "VOTER", "WORKER", "RESIDENT",
    "STUDENT", "ROBBER", "SERIAL KILLER", "ARSONIST",
    "CITIZEN", "PEOPLE", "INDIVIDUAL", "PERSON",
    # Institutions (too generic)
    "POLICE", "COURT", "PRISON", "SCHOOL", "UNIVERSITY",
    "COLLEGE", "CHURCH", "HOSPITAL", "PARLIAMENT",
    "COMMITTEE", "CHAMBER", "CONGRESS",
    # Legal
    "JUDGE", "JURY", "LAWYER", "PROSECUTOR", "MAGISTRATE",
    # Geographic terms used as actors
    "VILLAGE", "CITY", "TOWN", "REGION", "STATE",
    "PROVINCE", "DISTRICT", "COUNTY", "BOROUGH",
    # Other artifacts
    "ADMINISTRATION", "ARMED GROUP", "THE US",
    "EQUALITY STATE", "NEW BRUNSWICK", "HIGH COURT",
    # News agencies
    "REUTERS", "AP", "AFP", "BBC", "CNN", "STATE NEWS AGENCY",
    "NEWS AGENCY", "PRESS", "WIRE SERVICE",
    # Nationality adjectives (too generic)
    "CHINESE", "BRITISH", "AMERICAN", "EUROPEAN", "RUSSIAN",
    "IRANIAN", "ISRAELI", "PAKISTANI", "INDIAN", "FRENCH",
    # Other generic terms
    "SOVEREIGN", "AUTHORITIES", "NATO", "DEFECTOR",
    "DISTRICT COURT", "SUPREME COURT", "NATIONAL ELECTORAL COMMISSION",
    "KILLERS", "KILLER", "TANKER", "NAVAL UNIT",
}

# Country names — if actor = country name, it's redundant
COUNTRY_NAMES = set(COUNTRY_CODES.values())

MIN_ARTICLES = 5
HIGH_NOISE_COUNTRIES = {"US", "CA", "AS", "UK", "AU", "EI", "FR", "IT", "SP"}

COL_EVENTCODE      = 26
COL_ACTOR1NAME     = 6
COL_ACTOR2NAME     = 16
COL_ACTIONGEO_NAME = 53
COL_ACTIONGEO_CC   = 54  # FIPS 10-4 country code
COL_ACTIONGEO_ADM1 = 55  # ADM1 state/province code (first 2 chars = country code)
COL_ACTIONGEO_LAT  = 56  # Latitude (confirmed working)
COL_ACTIONGEO_LNG  = 57  # Longitude (confirmed working)
COL_NUMARTICLES    = 33

# Reverse lookup for location name matching
COUNTRY_NAME_TO_CODE = {v.lower(): k for k, v in COUNTRY_CODES.items()}


def resolve_country(code: str, location_name: str, adm1_code: str = "") -> str:
    """Try multiple strategies to resolve a full country name."""
    # Strategy 1: direct FIPS code
    if code and code.upper() in COUNTRY_CODES:
        return COUNTRY_CODES[code.upper()]
    # Strategy 2: first 2 chars of ADM1 code (e.g. "IR01" -> "IR" -> Iran)
    if adm1_code and len(adm1_code) >= 2:
        cc = adm1_code[:2].upper()
        if cc in COUNTRY_CODES:
            return COUNTRY_CODES[cc]
    # Strategy 3: location name matches a known country
    if location_name:
        for part in [location_name] + location_name.split(","):
            if part.strip().lower() in COUNTRY_NAME_TO_CODE:
                return part.strip().title()
    return location_name or code or "Unknown location"


def get_action_description(event_code: str) -> str:
    for length in [3, 2]:
        prefix = event_code[:length]
        if prefix in CODE_DESCRIPTIONS:
            return CODE_DESCRIPTIONS[prefix]
    return "military activity"


def is_junk_actor(actor: str, country_name: str) -> bool:
    """Return True if actor is a junk/artifact name."""
    if not actor:
        return True
    upper = actor.upper().strip()
    # In junk list
    if upper in JUNK_ACTORS:
        return True
    # Is just a country name (redundant)
    if actor.title() in COUNTRY_NAMES:
        return True
    # Is same as country (e.g. "Iran" acting in Iran)
    if actor.title() == country_name:
        return True
    # Very short (1-2 chars)
    if len(actor) <= 2:
        return True
    # Looks like a FIPS code (2 uppercase letters)
    if re.match(r'^[A-Z]{2}$', actor):
        return True
    return False


def clean_actor(actor: str) -> str:
    """Title-case and clean up actor name."""
    return actor.strip().title()


def build_message(event: dict) -> str:
    country  = event.get("country_name", "Unknown")
    location = event.get("location", "")
    actor1   = event.get("actor1", "")
    actor2   = event.get("actor2", "")
    action   = event.get("action", "military activity")

    # Locations that are known to be misassigned by GDELT
    # (city names from other countries, or overly generic terms)
    SUSPECT_LOCATIONS = {
        "Samara",    # Russian city often misassigned
        "Africa",    # Continent, not a city
        "Europe",    # Continent
        "Asia",      # Continent
        "America",   # Continent
        "New Brunswick",  # Canadian province misassigned to France
    }

    # Use specific city only if it's meaningful and not suspect
    place = (location if location
             and location.lower() != country.lower()
             and len(location) > 3
             and location.title() not in COUNTRY_NAMES
             and location not in SUSPECT_LOCATIONS
             else country)

    a1 = clean_actor(actor1) if not is_junk_actor(actor1, country) else ""
    a2 = clean_actor(actor2) if not is_junk_actor(actor2, country) else ""

    # Avoid redundant "X involving X" patterns
    if a1 and a2 and a1.lower() == a2.lower():
        a2 = ""

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

                event_code   = row[COL_EVENTCODE].strip()
                lat_str      = row[COL_ACTIONGEO_LAT].strip()
                lng_str      = row[COL_ACTIONGEO_LNG].strip()
                country_code = row[COL_ACTIONGEO_CC].strip() if len(row) > COL_ACTIONGEO_CC else ""
                adm1_code    = row[COL_ACTIONGEO_ADM1].strip() if len(row) > COL_ACTIONGEO_ADM1 else ""
                actor1       = row[COL_ACTOR1NAME].strip()
                actor2       = row[COL_ACTOR2NAME].strip()
                num_arts_str = row[COL_NUMARTICLES].strip()
                num_arts     = int(num_arts_str) if num_arts_str.isdigit() else 0

                # Filter 1: valid coordinates
                if not lat_str or not lng_str:
                    continue
                lat = float(lat_str)
                lng = float(lng_str)
                if lat == 0.0 and lng == 0.0:
                    continue
                if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                    continue

                # Filter 2: minimum article coverage
                if num_arts < MIN_ARTICLES:
                    continue

                # Filter 3: relevant CAMEO code
                severity = None
                for code, sev in RELEVANT_CAMEO.items():
                    if event_code.startswith(code):
                        severity = sev
                        break
                if not severity:
                    continue

                location_name = row[COL_ACTIONGEO_NAME].strip()
                country_name  = resolve_country(country_code, location_name, adm1_code)
                action        = get_action_description(event_code)

                # Filter 4: high-noise countries need at least one real actor
                if country_code in HIGH_NOISE_COUNTRIES:
                    if is_junk_actor(actor1, country_name) and is_junk_actor(actor2, country_name):
                        continue

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

    # Sort by coverage — most-reported first
    raw_events.sort(key=lambda x: x["num_articles"], reverse=True)

    processed = []
    seen_locations = set()

    for e in raw_events:
        if len(processed) >= 75:
            break

        # One event per country per CAMEO type to avoid flooding
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
