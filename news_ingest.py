# news_ingest.py
# Sources: Google News RSS + X/Twitter accounts via Nitter RSS
# Features: smart geocoding, deduplication, Nitter fallback handling

import re
import requests
import feedparser
import hashlib
from datetime import datetime

HEADERS = {"User-Agent": "intel-system/1.0"}

# ── GOOGLE NEWS RSS FEEDS ─────────────────────────────────────────────────────
GOOGLE_NEWS_FEEDS = [
    "https://news.google.com/rss/search?q=war+attack+conflict+military&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=airstrike+missile+bombing+explosion&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=killed+civilians+troops+battle&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=coup+protest+riot+sanction&hl=en&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=earthquake+flood+disaster+hurricane&hl=en&gl=US&ceid=US:en",
]

# ── NITTER INSTANCES ──────────────────────────────────────────────────────────
NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.net",
    "https://nitter.cz",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
]

NEWS_ACCOUNTS = [
    "Reuters", "BBCBreaking", "AP", "AFP", "AJEnglish", "BNONews",
    "middleeasteye", "disclosetv", "sentdefender", "FaytuksNetworks",
    "Faytuks", "clashreport", "AMK_Mapping_", "Tammuz_Intel",
    "hey_itsmyturn", "lookner", "InsiderGeo", "AZ_Intel_",
    "Global_Mil_Info", "Osinttechnical", "RALee85", "spectatorindex",
]

# ── COUNTRY DATA ──────────────────────────────────────────────────────────────
COUNTRY_COORDS = {
    "Afghanistan":(33.9,67.7),"Albania":(41.2,20.2),"Algeria":(28.0,1.7),
    "Angola":(-11.2,17.9),"Argentina":(-38.4,-63.6),"Armenia":(40.1,45.0),
    "Australia":(-25.3,133.8),"Austria":(47.5,14.5),"Azerbaijan":(40.1,47.6),
    "Bahrain":(26.0,50.6),"Bangladesh":(23.7,90.4),"Belarus":(53.7,27.9),
    "Belgium":(50.8,4.5),"Bolivia":(-16.3,-63.6),"Bosnia":(43.9,17.7),
    "Brazil":(-14.2,-51.9),"Bulgaria":(42.7,25.5),"Burkina Faso":(12.4,-1.6),
    "Burma":(21.9,95.9),"Cambodia":(12.6,104.9),"Cameroon":(3.8,11.5),
    "Canada":(56.1,-106.3),"Chile":(-35.7,-71.5),"China":(35.9,104.2),
    "Colombia":(4.6,-74.1),"Congo":(-0.2,15.8),"DR Congo":(-4.0,21.8),
    "Croatia":(45.1,15.2),"Cuba":(21.5,-79.5),"Czech Republic":(49.8,15.5),
    "Denmark":(56.3,9.5),"Ecuador":(-1.8,-78.2),"Egypt":(26.8,30.8),
    "Ethiopia":(9.1,40.5),"Finland":(64.0,26.0),"France":(46.2,2.2),
    "Georgia":(42.3,43.4),"Germany":(51.2,10.5),"Ghana":(7.9,-1.0),
    "Greece":(39.1,21.8),"Guatemala":(15.8,-90.2),"Guinea":(11.0,-10.9),
    "Haiti":(19.0,-72.3),"Honduras":(15.2,-86.2),"Hungary":(47.2,19.5),
    "India":(20.6,78.9),"Indonesia":(-0.8,113.9),"Iran":(32.4,53.7),
    "Iraq":(33.2,43.7),"Ireland":(53.4,-8.2),"Israel":(31.0,35.0),
    "Italy":(41.9,12.6),"Japan":(36.2,138.3),"Jordan":(31.0,36.0),
    "Kazakhstan":(48.0,68.0),"Kenya":(-0.0,37.9),"Kosovo":(42.6,20.9),
    "Kuwait":(29.3,47.5),"Kyrgyzstan":(41.2,74.8),"Laos":(19.9,102.5),
    "Latvia":(56.9,24.6),"Lebanon":(33.9,35.9),"Libya":(26.3,17.2),
    "Lithuania":(55.2,23.9),"Luxembourg":(49.8,6.1),"Malaysia":(4.2,108.0),
    "Mali":(17.6,-4.0),"Mexico":(23.6,-102.6),"Moldova":(47.4,28.4),
    "Mongolia":(46.9,103.8),"Morocco":(31.8,-7.1),"Mozambique":(-18.7,35.5),
    "Nepal":(28.4,84.1),"Netherlands":(52.1,5.3),"New Zealand":(-40.9,174.9),
    "Nicaragua":(12.9,-85.2),"Niger":(17.6,8.1),"Nigeria":(9.1,8.7),
    "North Korea":(40.0,127.0),
    "DPRK":(40.0,127.0),"Norway":(60.5,8.5),"Pakistan":(30.4,69.3),
    "Palestine":(31.9,35.2),"Paraguay":(-23.4,-58.4),"Peru":(-9.2,-75.0),
    "Philippines":(12.9,121.8),"Poland":(51.9,19.1),"Portugal":(39.4,-8.2),
    "Qatar":(25.4,51.2),"Romania":(45.9,24.9),"Russia":(61.5,105.3),
    "Rwanda":(-1.9,29.9),"Saudi Arabia":(23.9,45.1),"Senegal":(14.5,-14.5),
    "Serbia":(44.0,21.0),"Singapore":(1.4,103.8),"Sierra Leone":(8.5,-11.8),
    "Somalia":(5.2,46.2),"South Africa":(-30.6,22.9),"South Korea":(35.9,127.8),
    "South Sudan":(6.9,31.3),"Spain":(40.5,-3.7),"Sri Lanka":(7.9,80.8),
    "Sudan":(12.9,30.2),"Sweden":(63.1,18.6),"Switzerland":(46.8,8.2),
    "Syria":(34.8,38.9),"Taiwan":(23.7,120.9),"Tajikistan":(38.9,71.3),
    "Tanzania":(-6.4,34.9),"Thailand":(15.9,100.9),"Chad":(15.5,18.7),
    "Tunisia":(33.9,9.5),"Turkey":(38.9,35.2),"Turkmenistan":(38.9,59.6),
    "Uganda":(1.4,32.3),"Ukraine":(48.4,31.2),"UAE":(23.4,53.8),
    "United Kingdom":(55.4,-3.4),"United States":(37.1,-95.7),
    "Uruguay":(-32.5,-55.8),"Uzbekistan":(41.4,64.6),"Venezuela":(6.4,-66.6),
    "Vietnam":(14.1,108.3),"Yemen":(15.6,48.5),"Zambia":(-13.1,27.8),
    "Zimbabwe":(-20.0,30.0),"West Bank":(31.9,35.2),"Gaza":(31.4,34.3),
    "Botswana":(-22.3,24.7),"Liberia":(6.4,-9.4),"Mauritania":(21.0,-10.9),
    "Mauritius":(-20.3,57.6),"Crimea":(45.3,34.0),
}

COUNTRY_NAMES = sorted(COUNTRY_COORDS.keys(), key=len, reverse=True)

# Aliases for common alternate names
# City/region → country mapping (only unambiguous multi-char aliases)
COUNTRY_ALIASES = {
    # Conflict zones
    "zaporizhzhia": "Ukraine",
    "zaporizhia": "Ukraine",
    "donbas": "Ukraine",
    "donbass": "Ukraine",
    "donetsk": "Ukraine",
    "mariupol": "Ukraine",
    "bakhmut": "Ukraine",
    "kharkiv": "Ukraine",
    "kherson": "Ukraine",
    "kyiv": "Ukraine",
    "odesa": "Ukraine",
    "luhansk": "Ukraine",
    "gaza strip": "Gaza",
    "northern gaza": "Gaza",
    "beit lahiya": "Gaza",
    "rafah": "Gaza",
    "khan younis": "Gaza",
    "west bank": "West Bank",
    "ramallah": "West Bank",
    "jenin": "West Bank",
    "strait of hormuz": "Iran",
    "hormuz": "Iran",
    "tehran": "Iran",
    "isfahan": "Iran",
    "red sea": "Yemen",
    "hodeidah": "Yemen",
    "sanaa": "Yemen",
    "sahel": "Mali",
    "idlib": "Syria",
    "aleppo": "Syria",
    "damascus": "Syria",
    "deir ez-zor": "Syria",
    "mosul": "Iraq",
    "fallujah": "Iraq",
    "baghdad": "Iraq",
    "basra": "Iraq",
    "kabul": "Afghanistan",
    "kandahar": "Afghanistan",
    "helmand": "Afghanistan",
    "jerusalem": "Israel",
    "tel aviv": "Israel",
    "haifa": "Israel",
    "beirut": "Lebanon",
    "tripoli": "Libya",
    "benghazi": "Libya",
    "khartoum": "Sudan",
    "darfur": "Sudan",
    "taipei": "Taiwan",
    "pyongyang": "North Korea",
    "tuapse": "Russia",
    "vilnyansk": "Ukraine",
    "nagorny karabakh": "Azerbaijan",
    "nagorno-karabakh": "Azerbaijan",
}

# Aggressor countries — when these appear as subject, look for the target instead
# e.g. "Russia strikes Ukraine" → Ukraine, not Russia
AGGRESSOR_PATTERNS = {
    "Russia": ["Ukraine","Syria","Georgia","Poland","Baltic","Moldova"],
    "Israel": ["Gaza","Lebanon","Iran","Syria","West Bank","Palestine"],
    "Iran":   ["Israel","Iraq","Syria","Yemen","Pakistan"],
    "US":     ["Iraq","Syria","Afghanistan","Yemen","Somalia"],
    "United States": ["Iraq","Syria","Afghanistan","Yemen","Somalia"],
    "China":  ["Taiwan","India","Philippines","Japan"],
    "Turkey": ["Syria","Kurdistan","Iraq","Armenia","Greece"],
    "Saudi Arabia": ["Yemen"],
    "India":  ["Pakistan","China"],
    "Pakistan": ["India","Afghanistan"],
    "Myanmar": ["Thailand","Bangladesh"],
    "Ethiopia": ["Eritrea","Somalia","Sudan"],
    "Azerbaijan": ["Armenia"],
    "North Korea": ["South Korea","Japan","United States"],
}

# Action verbs indicating the following location is the TARGET
TARGET_VERBS = [
    "strikes?","attacks?","bombs?","bombing","invades?","invading","invasion",
    "shells?","shelling","launches? (?:missiles?|rockets?|drones?|strikes?)",
    "hits?","targets?","targeting","raids?","raiding","fires? (?:on|at|into|toward)",
    "enters?","entering","crosses? into","advances? (?:on|into|toward)",
    "kills? (?:in|near|at)","offensive (?:in|on|against|into)",
    "operations? in","deployed? (?:to|in|near)",
]

TARGET_VERB_RE = re.compile(
    r'(?:' + '|'.join(TARGET_VERBS) + r')\s+(?:the\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
    re.IGNORECASE
)

# Prepositions that usually introduce the location of the event
LOCATION_PREPS = ["in", "near", "at", "inside", "across", "throughout",
                  "within", "outside", "around", "from", "into", "toward"]
LOCATION_PREP_RE = re.compile(
    r'\b(?:' + '|'.join(LOCATION_PREPS) + r')\s+(?:the\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
    re.IGNORECASE
)

# ── CLASSIFICATION ────────────────────────────────────────────────────────────
CONFLICT_KEYWORDS = [
    "war","attack","conflict","military","missile","airstrike","bombing","explosion",
    "troops","killed","casualties","battle","fighting","offensive","ceasefire",
    "protest","riot","coup","sanction","nuclear","hostage","kidnap","massacre",
    "earthquake","flood","hurricane","tsunami","disaster","crisis","refugee",
    "shelling","drone","strike","wounded","detained","arrested","clash",
]

CRITICAL_WORDS = [
    "killed","dead","casualties","bombing","airstrike","missile","explosion",
    "massacre","attack","battle","fighting","war","nuclear","hostage","kidnap",
    "shelling","drone strike","wounded","clash","offensive","assault","fires",
]

WARNING_WORDS = [
    "military","troops","conflict","sanction","protest","crisis","threat",
    "arrest","detained","ceasefire","refugee","displaced","mobilization",
    "earthquake","flood","hurricane","tsunami","disaster",
]


# Patterns that indicate stale/non-breaking content to filter out
STALE_PATTERNS = [
    # Encyclopedia/reference articles
    r"\|\s*(history|facts|timeline|summary|casualties|combatants)",
    r"history of the", r"facts about", r"what you need to know",
    r"everything you need to know", r"explainer:", r"timeline of",
    r"global conflict tracker", r"instability in \w+",
    # Analysis/retrospective articles
    r"^why did\b", r"^how did\b", r"^how the\b", r"^what is\b",
    r"^explained[,:\s]", r"^explainer[,:\s]",
    r"\|\s*explained\b", r"\|\s*explainer\b",  # "X | Explained"
    r"\btimeline[:\-]", r"\bkey moments\b",
    r"in its first (four|two|three|five|six|eight|ten|\d+) weeks?",
    r"unfolded in (its|the)",
    r"\bwar maps?[:\-]", r"\bconflict maps?[:\-]",
    r"\bcollection[:\-]",
    r"\bq&a[:\-]", r"\bexpert q",
    r"\bprimer on\b", r"\btargeting primer\b",
    r"\boperational progress\b",
    r"^the war in \w+[:\-]",
    r"how long could", r"how long will",
    r"\b2025-2026\b",
    r"\bmap thread for\b",
    # Economic/financial war coverage (not military)
    r"\bwar.*boost.*pric", r"\bwar.*push.*pric",
    r"\bwar.*inflation\b", r"\bprice war\b", r"\btrade war\b",
    r"\bbusiness activity\b.*war", r"\bwar.*business activity\b",
    r"\boil.*recover\b", r"\bcrude output\b",
    r"\bfewer people will receive aid\b",
    r"higher fuel costs",
    # Denials/statements (handled in classify but also filter here)
    r"rules out (striking|using|nuclear)",
    # Crime/irrelevant
    r"drug sales", r"drug bust", r"arrested over alleged",
    r"charged with possession",
    # Thread fragments
    r"^\d+/",
    # Pope/religion unrelated
    r"\bpope\b.*photo", r"carries a photo",
    # Climate/unrelated political
    r"climate change off the agenda",
    r"not speaking for the",
]

STALE_RE = [re.compile(p, re.IGNORECASE) for p in STALE_PATTERNS]

def is_relevant(text: str) -> bool:
    t = text.lower()
    # Filter stale/encyclopedia content
    for pattern in STALE_RE:
        if pattern.search(text):
            return False
    return any(k in t for k in CONFLICT_KEYWORDS)


def classify_text(text: str) -> str:
    t = text.lower()
    if any(w in t for w in CRITICAL_WORDS): return "critical"
    if any(w in t for w in WARNING_WORDS):  return "warning"
    return "info"


def extract_country(text: str) -> tuple | None:
    """
    Smart geocoding: find the EVENT LOCATION, not just any country mention.
    Priority:
    1. Aliases (city names, region names → country)
    2. Target of action verbs (e.g. "strikes Ukraine" → Ukraine)
    3. Country after location prepositions (e.g. "in Gaza" → Gaza)
    4. Aggressor → known target mapping
    5. First country mentioned
    """
    import random

    def jitter(lat, lng):
        return lat + random.uniform(-1.2, 1.2), lng + random.uniform(-1.2, 1.2)

    def find_country_in(text_fragment: str) -> str | None:
        # Check aliases first
        frag_lower = text_fragment.lower()
        for alias, country in COUNTRY_ALIASES.items():
            if alias in frag_lower:
                return country
        for name in COUNTRY_NAMES:
            if re.search(r'\b' + re.escape(name) + r'\b', text_fragment, re.IGNORECASE):
                return name
        return None
    
    # Strategy 0: check aliases in full text first (city/region names)
    text_lower = text.lower()
    for alias, country in COUNTRY_ALIASES.items():
        # Use word boundary check to avoid partial matches
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, text_lower) and country in COUNTRY_COORDS:
            lat, lng = jitter(*COUNTRY_COORDS[country])
            return lat, lng, country

    # Strategy 1: target of action verb
    for m in TARGET_VERB_RE.finditer(text):
        candidate = m.group(1).strip()
        country = find_country_in(candidate)
        if country and country in COUNTRY_COORDS:
            lat, lng = jitter(*COUNTRY_COORDS[country])
            return lat, lng, country

    # Strategy 2: country after location preposition
    for m in LOCATION_PREP_RE.finditer(text):
        candidate = m.group(1).strip()
        country = find_country_in(candidate)
        if country and country in COUNTRY_COORDS:
            lat, lng = jitter(*COUNTRY_COORDS[country])
            return lat, lng, country

    # Strategy 3: aggressor → known target
    for aggressor, targets in AGGRESSOR_PATTERNS.items():
        if re.search(r'\b' + re.escape(aggressor) + r'\b', text, re.IGNORECASE):
            for target in targets:
                if re.search(r'\b' + re.escape(target) + r'\b', text, re.IGNORECASE):
                    if target in COUNTRY_COORDS:
                        lat, lng = jitter(*COUNTRY_COORDS[target])
                        return lat, lng, target

    # Strategy 4: first country mentioned
    for name in COUNTRY_NAMES:
        if re.search(r'\b' + re.escape(name) + r'\b', text, re.IGNORECASE):
            if name in COUNTRY_COORDS:
                lat, lng = jitter(*COUNTRY_COORDS[name])
                return lat, lng, name

    return None


def dedup_key(text: str) -> str:
    """Generate a deduplication key from headline text."""
    # Normalize: lowercase, remove punctuation, collapse spaces
    normalized = re.sub(r'[^\w\s]', '', text.lower())
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    # Use first 80 chars as key to catch slightly different versions of same story
    return normalized[:80]


# ── NITTER HEALTH CHECK ───────────────────────────────────────────────────────
_nitter_health = {}  # instance → {"ok": bool, "checked": datetime}

def check_nitter_instance(instance: str) -> bool:
    """Check if a Nitter instance is responding."""
    now = datetime.utcnow()
    health = _nitter_health.get(instance, {})

    # Re-check every 30 minutes
    if health.get("checked") and (now - health["checked"]).seconds < 1800:
        return health.get("ok", False)

    try:
        res = requests.get(f"{instance}/Reuters/rss", headers=HEADERS, timeout=8)
        ok = res.status_code == 200 and len(res.content) > 500
        _nitter_health[instance] = {"ok": ok, "checked": now}
        if ok: print(f"✅ Nitter instance working: {instance}")
        else:  print(f"⚠️ Nitter instance down: {instance} ({res.status_code})")
        return ok
    except Exception as e:
        _nitter_health[instance] = {"ok": False, "checked": now}
        print(f"⚠️ Nitter instance unreachable: {instance}")
        return False


def get_working_nitter() -> str | None:
    """Return first working Nitter instance."""
    for instance in NITTER_INSTANCES:
        if check_nitter_instance(instance):
            return instance
    print("⚠️ All Nitter instances down — skipping X RSS")
    return None


# ── GOOGLE NEWS RSS ───────────────────────────────────────────────────────────
def fetch_google_news() -> list:
    items = []
    seen = set()
    for url in GOOGLE_NEWS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:
                title = entry.get("title","").strip()
                title = re.sub(r'\s+-\s+[\w\s]+$', '', title).strip()
                if not title or not is_relevant(title): continue
                key = dedup_key(title)
                if key in seen: continue
                seen.add(key)
                items.append({"text": title, "source": "Google News"})
        except Exception as e:
            print(f"⚠️ Google News feed error: {e}")
    print(f"📰 Google News: {len(items)} relevant headlines")
    return items


# ── NITTER RSS ────────────────────────────────────────────────────────────────
def fetch_nitter_rss() -> list:
    items = []
    seen = set()

    instance = get_working_nitter()
    if not instance:
        return []

    for account in NEWS_ACCOUNTS:
        try:
            url  = f"{instance}/{account}/rss"
            feed = feedparser.parse(url)
            if not feed.entries: continue

            for entry in feed.entries[:8]:
                title = entry.get("title","").strip()
                title = re.sub(r'^RT by\s*:?\s*', '', title)
                title = re.sub(r'^R to @\w+:\s*', '', title)
                title = re.sub(r'https?://\S+', '', title).strip()
                title = re.sub(r'@\w+\s*', '', title).strip()
                title = re.sub(r'#\w+\s*', '', title).strip()
                # Trim at sentence break if too long
                if len(title) > 180:
                    for sep in ['. ', '! ', '? ']:
                        idx = title[:160].rfind(sep)
                        if idx > 60: title = title[:idx+1]; break
                    else:
                        title = title[:180].rsplit(' ', 1)[0] + '...'

                if not title or not is_relevant(title): continue
                key = dedup_key(title)
                if key in seen: continue
                seen.add(key)
                items.append({"text": title, "source": f"@{account}"})

        except Exception:
            continue

    print(f"🐦 Nitter (@{instance.split('//')[1]}): {len(items)} relevant posts")
    return items


# ── PROCESS TO MAP EVENTS ─────────────────────────────────────────────────────
def items_to_events(items: list) -> list:
    events = []
    seen_keys = set()

    for item in items:
        # Clean newlines and extra whitespace
        text = item["text"].replace("\n", " ").replace("\r", " ")
        text = re.sub(r'\s{2,}', ' ', text).strip()
        result = extract_country(text)
        if not result: continue

        lat, lng, country = result

        # Deduplicate by country + first 6 words of text
        words    = text.lower().split()[:6]
        dedup    = f"{country}:{''.join(words)}"
        if dedup in seen_keys: continue
        seen_keys.add(dedup)

        events.append({
            "lat":      round(lat, 4),
            "lng":      round(lng, 4),
            "message":  text[:200],
            "type":     classify_text(text),
            "location": country,
            "source":   item["source"],
        })

    events.sort(key=lambda x: {"critical":0,"warning":1,"info":2}.get(x["type"],2))
    return events


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────
def get_gdelt_events() -> list:
    print("📡 Fetching Google News + X RSS...")
    items  = fetch_google_news()
    items += fetch_nitter_rss()
    events = items_to_events(items)
    print(f"✅ {len(events)} geo-tagged events ready")
    return events[:150]


def get_news(max_records: int = 50) -> list:
    return []
