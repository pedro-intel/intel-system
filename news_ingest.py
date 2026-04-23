# news_ingest.py
# Multi-source: ACLED (primary) → GDELT CSV (secondary) → Google News + Nitter RSS (fast breaking news)

import os
import requests
import csv
import zipfile
import io
import re
import feedparser
from datetime import datetime, timedelta

# ── ACLED CONFIG ──────────────────────────────────────────────────────────────
ACLED_EMAIL    = os.getenv("ACLED_EMAIL")
ACLED_PASSWORD = os.getenv("ACLED_PASSWORD")
TOKEN_URL      = "https://acleddata.com/oauth/token"
API_URL        = "https://acleddata.com/api/acled/read"
_token_cache   = {"token": None, "expires_at": None}
_acled_ok      = None

HEADERS = {"User-Agent": "intel-system/1.0"}

# ── GOOGLE NEWS RSS FEEDS ─────────────────────────────────────────────────────
# These update every few minutes with breaking news
GOOGLE_NEWS_FEEDS = [
    # Breaking conflict/war news
    "https://news.google.com/rss/search?q=war+attack+conflict+military&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=airstrike+missile+bombing+explosion&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=killed+civilians+troops+battle&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=coup+protest+riot+sanction&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=earthquake+flood+disaster+hurricane&hl=en&gl=US&ceid=US:en",
]

# ── NITTER RSS (X/Twitter accounts as RSS) ────────────────────────────────────
# Breaking news accounts — multiple Nitter instances as fallbacks
NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.net",
    "https://nitter.cz",
]

# Major breaking news Twitter/X accounts
NEWS_ACCOUNTS = [
    # Major outlets
    "Reuters",
    "BBCBreaking",
    "AP",
    "AFP",
    "AJEnglish",        # Al Jazeera
    "BNONews",          # Breaking news aggregator
    "middleeasteye",
    # Fast breaking news
    "disclosetv",
    "sentdefender",
    # Conflict/OSINT
    "FaytuksNetworks",
    "Faytuks",
    "clashreport",
    "AMK_Mapping_",
    "Tammuz_Intel",
    "hey_itsmyturn",
    "lookner",
    "InsiderGeo",
    "AZ_Intel_",
    "Global_Mil_Info",
    "Osinttechnical",
    # Regional analysts
    "RALee85",
    "spectatorindex",
]

# ── GDELT CONFIG ──────────────────────────────────────────────────────────────
GDELT_LASTUPDATE = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

COUNTRY_CODES = {
    "AF":"Afghanistan","AL":"Albania","AG":"Algeria","AO":"Angola","AR":"Argentina",
    "AM":"Armenia","AS":"Australia","AU":"Austria","AJ":"Azerbaijan","BH":"Bahrain",
    "BG":"Bangladesh","BO":"Belarus","BE":"Belgium","BL":"Bolivia","BK":"Bosnia",
    "BR":"Brazil","BU":"Bulgaria","UV":"Burkina Faso","BM":"Burma","CB":"Cambodia",
    "CM":"Cameroon","CA":"Canada","CI":"Chile","CH":"China","CO":"Colombia",
    "CF":"Congo","CG":"DR Congo","HR":"Croatia","CU":"Cuba","EZ":"Czech Republic",
    "DA":"Denmark","EC":"Ecuador","EG":"Egypt","ET":"Ethiopia","FI":"Finland",
    "FR":"France","GG":"Georgia","GM":"Germany","GH":"Ghana","GR":"Greece",
    "GT":"Guatemala","GV":"Guinea","HA":"Haiti","HO":"Honduras","HU":"Hungary",
    "IN":"India","ID":"Indonesia","IR":"Iran","IZ":"Iraq","EI":"Ireland",
    "IS":"Israel","IT":"Italy","JA":"Japan","JO":"Jordan","KZ":"Kazakhstan",
    "KE":"Kenya","KV":"Kosovo","KU":"Kuwait","KG":"Kyrgyzstan","LA":"Laos",
    "LG":"Latvia","LE":"Lebanon","LY":"Libya","LH":"Lithuania","LU":"Luxembourg",
    "MY":"Malaysia","ML":"Mali","MX":"Mexico","MD":"Moldova","MG":"Mongolia",
    "MO":"Morocco","MZ":"Mozambique","NP":"Nepal","NL":"Netherlands",
    "NZ":"New Zealand","NU":"Nicaragua","NG":"Niger","NI":"Nigeria",
    "KN":"North Korea","NO":"Norway","PK":"Pakistan","PA":"Paraguay",
    "PS":"Palestine","PE":"Peru","RP":"Philippines","PL":"Poland","PO":"Portugal",
    "QA":"Qatar","RO":"Romania","RS":"Russia","RW":"Rwanda","SA":"Saudi Arabia",
    "SG":"Senegal","RB":"Serbia","SN":"Singapore","SL":"Sierra Leone",
    "SO":"Somalia","SF":"South Africa","KS":"South Korea","OD":"South Sudan",
    "SP":"Spain","CE":"Sri Lanka","SU":"Sudan","SW":"Sweden","SZ":"Switzerland",
    "SY":"Syria","TW":"Taiwan","TI":"Tajikistan","TZ":"Tanzania","TH":"Thailand",
    "TD":"Chad","TS":"Tunisia","TU":"Turkey","TX":"Turkmenistan","UG":"Uganda",
    "UP":"Ukraine","AE":"UAE","UK":"United Kingdom","US":"United States",
    "UY":"Uruguay","UZ":"Uzbekistan","VE":"Venezuela","VM":"Vietnam","YM":"Yemen",
    "ZA":"Zambia","ZI":"Zimbabwe","GQ":"Equatorial Guinea","PG":"Papua New Guinea",
    "FJ":"Fiji","GY":"Guyana","MU":"Mauritius","MT":"Malta","WE":"West Bank",
    "EK":"Ecuador","BC":"Botswana","LI":"Liberia","MR":"Mauritania",
}

COUNTRY_NAME_TO_CODE = {v.lower(): k for k, v in COUNTRY_CODES.items()}
COUNTRY_NAMES = set(COUNTRY_CODES.values())

RELEVANT_CAMEO = {
    "183":"critical","184":"critical","185":"critical","186":"critical",
    "190":"critical","195":"critical","196":"critical","180":"critical",
    "181":"critical","18":"critical","19":"critical",
    "172":"warning","174":"warning","175":"warning","145":"warning",
    "151":"warning","152":"warning","171":"warning",
}

CODE_DESCRIPTIONS = {
    "183":"armed clashes","184":"mass violence","185":"assassination",
    "186":"massacre","190":"mass violence","195":"kidnapping","196":"hijacking",
    "180":"military strike","181":"blockade imposed","18":"armed assault",
    "19":"mass violence","172":"mass arrests","174":"sanctions imposed",
    "175":"military threat","145":"violent protest","151":"military alert raised",
    "152":"troop mobilization","171":"assets seized",
}

JUNK_ACTORS = {
    "MEDIA","NEWS OUTLET","COMPANY","BUSINESS","WEBSITE","CARRIER","AIRLINE",
    "RYANAIR","MICROSOFT","GOOGLE","FACEBOOK","TWITTER","INTERNET","REUTERS",
    "AP","AFP","BBC","CNN","STATE NEWS AGENCY","NEWS AGENCY","PRESS",
    "CRIMINAL","GUNMAN","VOTER","WORKER","RESIDENT","STUDENT","ROBBER",
    "SERIAL KILLER","ARSONIST","CITIZEN","PEOPLE","INDIVIDUAL","PERSON",
    "POLICE","COURT","PRISON","SCHOOL","UNIVERSITY","COLLEGE","CHURCH",
    "HOSPITAL","PARLIAMENT","COMMITTEE","CHAMBER","CONGRESS",
    "JUDGE","JURY","LAWYER","PROSECUTOR","MAGISTRATE","DISTRICT COURT",
    "SUPREME COURT","NATIONAL ELECTORAL COMMISSION","APPELLATE COURT",
    "VILLAGE","CITY","TOWN","REGION","STATE","PROVINCE","DISTRICT",
    "ADMINISTRATION","ARMED GROUP","THE US","EQUALITY STATE","HIGH COURT",
    "SOVEREIGN","AUTHORITIES","NATO","DEFECTOR","TANKER","NAVAL UNIT",
    "KILLERS","KILLER","DETECTIVE","CAREGIVER","TELEVISION","PRODUCER",
    "HONDA","SAMSUNG","BANGKOK","LORRAINE","TENANTS","LIEUTENANT",
    "MANUFACTURER","EMPLOYEE","BELIEVER","GOVERNOR",
}

MIN_ARTICLES = 5
HIGH_NOISE   = {"US","CA","AS","UK","AU","EI","FR","IT","SP"}
SUSPECT_LOCS = {"Samara","Africa","Europe","Asia","America","New Brunswick","Odessa"}

COL_EVENTCODE=26; COL_ACTOR1=6; COL_ACTOR2=16; COL_GEO_NAME=53
COL_GEO_CC=54; COL_GEO_ADM1=55; COL_LAT=56; COL_LNG=57; COL_ARTS=33

# Keywords for RSS relevance filtering
CONFLICT_KEYWORDS = [
    "war","attack","conflict","military","missile","airstrike","bombing","explosion",
    "troops","killed","casualties","battle","fighting","offensive","ceasefire",
    "protest","riot","coup","sanction","nuclear","hostage","kidnap","massacre",
    "earthquake","flood","hurricane","tsunami","disaster","crisis","refugee",
]


# ── ACLED ─────────────────────────────────────────────────────────────────────

def get_acled_token() -> str | None:
    global _acled_ok
    if _acled_ok is False:
        return None
    now = datetime.utcnow()
    if _token_cache["token"] and _token_cache["expires_at"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]
    if not ACLED_EMAIL or not ACLED_PASSWORD:
        return None
    try:
        print("🔑 Authenticating with ACLED...")
        res = requests.post(TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"username":ACLED_EMAIL,"password":ACLED_PASSWORD,
                  "grant_type":"password","client_id":"acled","scope":"authenticated"},
            timeout=15)
        if res.status_code == 200:
            token = res.json().get("access_token")
            _token_cache["token"] = token
            _token_cache["expires_at"] = now + timedelta(hours=23)
            print("✅ ACLED authenticated")
            return token
        return None
    except Exception as e:
        print(f"⚠️ ACLED auth error: {e}")
        return None


def get_acled_events() -> list:
    global _acled_ok
    token = get_acled_token()
    if not token:
        return []
    since = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        res = requests.get(API_URL, params={
            "_format":"json","event_date":f"{since}|{today}",
            "event_date_where":"BETWEEN",
            "fields":"event_id_cnty|event_date|event_type|sub_event_type|actor1|actor2|country|location|latitude|longitude|fatalities",
            "limit":500,
        }, headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"}, timeout=30)
        if res.status_code == 403:
            print("⚠️ ACLED: Access denied — falling back")
            _acled_ok = False
            return []
        data = res.json()
        if data.get("status") != 200:
            return []
        _acled_ok = True
        raw = data.get("data",[])
        processed = []
        seen = set()
        for e in raw:
            try:
                lat = float(e.get("latitude") or 0)
                lng = float(e.get("longitude") or 0)
                if lat==0 and lng==0: continue
                if not (-90<=lat<=90) or not (-180<=lng<=180): continue
                country  = e.get("country","Unknown")
                location = e.get("location","")
                actor1   = e.get("actor1","").strip()
                actor2   = e.get("actor2","").strip()
                et       = e.get("event_type","")
                se       = e.get("sub_event_type","")
                fats     = int(e.get("fatalities",0) or 0)
                sev      = "critical" if et.lower() in {"battles","explosions/remote violence","violence against civilians"} else "warning" if et.lower() in {"riots","strategic developments"} else "info"
                place    = f"{location}, {country}" if location and location!=country else country
                if actor1 and actor2: msg = f"{actor1} — {se.lower()} involving {actor2} in {place}"
                elif actor1: msg = f"{actor1} — {se.lower()} in {place}"
                else: msg = f"{se.capitalize()} in {place}"
                if fats > 0: msg += f" ({fats} {'fatality' if fats==1 else 'fatalities'})"
                key = f"{round(lat,2)},{round(lng,2)}:{et}"
                if key in seen: continue
                seen.add(key)
                processed.append({"lat":lat,"lng":lng,"message":msg,"type":sev,"location":country,"source":"ACLED"})
            except: continue
        processed.sort(key=lambda x:{"critical":0,"warning":1,"info":2}.get(x["type"],2))
        return processed[:100]
    except Exception as e:
        print(f"⚠️ ACLED fetch error: {e}")
        return []


# ── GOOGLE NEWS RSS ───────────────────────────────────────────────────────────

def is_relevant(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in CONFLICT_KEYWORDS)


def classify_text(text: str) -> str:
    t = text.lower()
    critical = ["killed","dead","casualties","bombing","airstrike","missile","explosion",
                "massacre","attack","battle","fighting","war","nuclear","coup"]
    warning  = ["military","troops","conflict","sanction","protest","crisis",
                "threat","arrest","detained","ceasefire","refugee","displaced"]
    if any(w in t for w in critical): return "critical"
    if any(w in t for w in warning):  return "warning"
    return "info"


def extract_location_from_text(text: str) -> tuple | None:
    """Try to find a country/location in text and geocode it."""
    text_lower = text.lower()
    # Check against known country names (longest match first)
    for name in sorted(COUNTRY_NAMES, key=len, reverse=True):
        if name.lower() in text_lower:
            # Get approximate center coords from our lookup
            from ml_model import lookup_country_coords
            coords = lookup_country_coords(name)
            if coords:
                return coords, name
    return None


def fetch_google_news() -> list:
    """Fetch breaking news from Google News RSS feeds."""
    items = []
    seen_titles = set()

    for url in GOOGLE_NEWS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:15]:
                title   = entry.get("title", "").strip()
                summary = entry.get("summary", "").strip()
                text    = f"{title} {summary}"

                if not title or title in seen_titles:
                    continue
                if not is_relevant(text):
                    continue

                seen_titles.add(title)
                items.append({"title": title, "summary": summary, "source": "Google News"})

        except Exception as e:
            print(f"⚠️ Google News RSS error: {e}")

    print(f"📰 Google News: {len(items)} relevant articles")
    return items


def fetch_nitter_rss() -> list:
    """Fetch breaking news posts from major news X accounts via Nitter RSS."""
    items = []
    seen_titles = set()

    for account in NEWS_ACCOUNTS:
        fetched = False
        for instance in NITTER_INSTANCES:
            try:
                url  = f"{instance}/{account}/rss"
                feed = feedparser.parse(url)

                if not feed.entries:
                    continue

                for entry in feed.entries[:10]:
                    title = entry.get("title", "").strip()
                    # Remove RT prefixes and clean up
                    title = re.sub(r'^R to @\w+:\s*', '', title)
                    title = re.sub(r'https?://\S+', '', title).strip()

                    if not title or title in seen_titles:
                        continue
                    if not is_relevant(title):
                        continue

                    seen_titles.add(title)
                    items.append({"title": title, "summary": "", "source": f"@{account}"})

                fetched = True
                break  # Got data from this instance, move to next account

            except Exception:
                continue  # Try next Nitter instance

        if not fetched:
            pass  # All instances failed for this account, skip silently

    print(f"🐦 Nitter RSS: {len(items)} relevant posts")
    return items


def process_rss_items(items: list) -> list:
    """Convert RSS items to map events using NLP location extraction."""
    events = []
    seen   = set()

    for item in items:
        title = item["title"]
        text  = f"{title} {item.get('summary','')}"

        result = extract_location_from_text(text)
        if not result:
            continue

        coords, country = result
        lat, lng = coords

        if title in seen:
            continue
        seen.add(title)

        events.append({
            "lat":      lat,
            "lng":      lng,
            "message":  title[:200],
            "type":     classify_text(text),
            "location": country,
            "source":   item.get("source", "RSS"),
        })

    return events


# ── GDELT ─────────────────────────────────────────────────────────────────────

def resolve_country(code: str, location_name: str, adm1: str = "") -> str:
    if code and code.upper() in COUNTRY_CODES:
        return COUNTRY_CODES[code.upper()]
    if adm1 and len(adm1) >= 2:
        cc = adm1[:2].upper()
        if cc in COUNTRY_CODES:
            return COUNTRY_CODES[cc]
    if location_name:
        for part in [location_name] + location_name.split(","):
            if part.strip().lower() in COUNTRY_NAME_TO_CODE:
                return part.strip().title()
    return location_name or code or "Unknown"


def is_junk(actor: str, country: str) -> bool:
    if not actor: return True
    u = actor.upper().strip()
    if u in JUNK_ACTORS: return True
    if actor.title() in COUNTRY_NAMES: return True
    if actor.title() == country: return True
    if len(actor) <= 2: return True
    if re.match(r'^[A-Z]{2}$', actor): return True
    return False


def get_action(code: str) -> str:
    for l in [3, 2]:
        p = code[:l]
        if p in CODE_DESCRIPTIONS: return CODE_DESCRIPTIONS[p]
    return "military activity"


def build_gdelt_message(event: dict) -> str:
    country  = event.get("country_name","Unknown")
    location = event.get("location","")
    actor1   = event.get("actor1","").title()
    actor2   = event.get("actor2","").title()
    action   = event.get("action","military activity")
    place    = (location if location and location.lower()!=country.lower()
                and len(location)>3 and location.title() not in COUNTRY_NAMES
                and location not in SUSPECT_LOCS else country)
    a1 = "" if is_junk(actor1.upper(), country) else actor1
    a2 = "" if is_junk(actor2.upper(), country) else actor2
    if a1 and a2 and a1.lower()==a2.lower(): a2=""
    if a1 and a2: return f"{a1} — {action} involving {a2} in {place}"
    elif a1:      return f"{a1} — {action} in {place}"
    else:         return f"{action.capitalize()} in {place}"


def get_gdelt_url() -> str | None:
    try:
        res = requests.get(GDELT_LASTUPDATE, headers=HEADERS, timeout=10)
        for line in res.text.strip().split("\n"):
            if "export.CSV" in line:
                parts = line.strip().split(" ")
                if len(parts) >= 3: return parts[2]
    except Exception as e:
        print(f"⚠️ GDELT lastupdate error: {e}")
    return None


def download_gdelt(url: str) -> list:
    try:
        print(f"⬇️  Downloading GDELT: {url.split('/')[-1]}")
        res = requests.get(url, headers=HEADERS, timeout=30)
        res.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(res.content)) as z:
            with z.open(z.namelist()[0]) as f:
                content = f.read().decode("utf-8")
        events = []
        for row in csv.reader(io.StringIO(content), delimiter="\t"):
            try:
                if len(row) < 58: continue
                event_code   = row[COL_EVENTCODE].strip()
                lat_str      = row[COL_LAT].strip()
                lng_str      = row[COL_LNG].strip()
                country_code = row[COL_GEO_CC].strip() if len(row) > COL_GEO_CC else ""
                adm1_code    = row[COL_GEO_ADM1].strip() if len(row) > COL_GEO_ADM1 else ""
                actor1       = row[COL_ACTOR1].strip()
                actor2       = row[COL_ACTOR2].strip()
                num_arts_str = row[COL_ARTS].strip()
                num_arts     = int(num_arts_str) if num_arts_str.isdigit() else 0
                if not lat_str or not lng_str: continue
                lat = float(lat_str); lng = float(lng_str)
                if lat==0 and lng==0: continue
                if not (-90<=lat<=90) or not (-180<=lng<=180): continue
                if num_arts < MIN_ARTICLES: continue
                severity = None
                for code, sev in RELEVANT_CAMEO.items():
                    if event_code.startswith(code): severity=sev; break
                if not severity: continue
                location_name = row[COL_GEO_NAME].strip()
                country_name  = resolve_country(country_code, location_name, adm1_code)
                action        = get_action(event_code)
                if country_code in HIGH_NOISE:
                    if is_junk(actor1, country_name) and is_junk(actor2, country_name):
                        continue
                events.append({"lat":lat,"lng":lng,"location":location_name,
                    "country_name":country_name,"actor1":actor1,"actor2":actor2,
                    "event_code":event_code,"severity":severity,"action":action,
                    "num_articles":num_arts})
            except (ValueError, IndexError): continue
        print(f"✅ Parsed {len(events)} GDELT events")
        return events
    except Exception as e:
        print(f"⚠️ GDELT error: {e}")
        return []


def get_gdelt_fallback() -> list:
    url = get_gdelt_url()
    if not url: return []
    raw = download_gdelt(url)
    if not raw: return []
    raw.sort(key=lambda x: x["num_articles"], reverse=True)
    processed = []
    seen_locs = set()
    for e in raw:
        if len(processed) >= 60: break
        key = f"{e['country_name']}:{e['event_code'][:2]}"
        if key in seen_locs: continue
        seen_locs.add(key)
        processed.append({"lat":e["lat"],"lng":e["lng"],
            "message":build_gdelt_message(e),"type":e["severity"],
            "location":e["country_name"],"source":"GDELT"})
    return processed


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────

def get_gdelt_events() -> list:
    """
    Priority order:
    1. ACLED (human-verified, best quality) — if approved
    2. GDELT CSV (geo-tagged, 15-min updates) — always available
    3. Google News RSS (fastest, 2-5 min) — supplements GDELT
    4. Nitter RSS (X breaking news) — supplements when available
    """
    all_events = []

    # Try ACLED first
    if _acled_ok is not False:
        acled = get_acled_events()
        if acled:
            print(f"✅ Using ACLED ({len(acled)} events)")
            # Still add RSS for breaking news even with ACLED
            rss = _get_rss_events()
            return (acled + rss)[:150]

    # GDELT fallback
    print("🌐 Using GDELT + RSS sources")
    gdelt  = get_gdelt_fallback()
    rss    = _get_rss_events()
    all_events = gdelt + rss

    # Deduplicate by location proximity
    seen = set()
    final = []
    for e in all_events:
        key = f"{round(e['lat'],1)},{round(e['lng'],1)}"
        if key not in seen:
            seen.add(key)
            final.append(e)

    print(f"✅ Total: {len(final)} events ({len(gdelt)} GDELT + {len(rss)} RSS)")
    return final[:150]


def _get_rss_events() -> list:
    """Fetch and process all RSS sources."""
    items = []
    items += fetch_google_news()
    items += fetch_nitter_rss()
    return process_rss_items(items)


def get_news(max_records: int = 50) -> list:
    return []
