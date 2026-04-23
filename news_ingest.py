# news_ingest.py
# Sources: Google News RSS + X/Twitter accounts via Nitter RSS

import re
import requests
import feedparser
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

# ── NITTER INSTANCES (fallback chain) ─────────────────────────────────────────
NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.net",
    "https://nitter.cz",
]

# ── X ACCOUNTS TO FOLLOW ─────────────────────────────────────────────────────
NEWS_ACCOUNTS = [
    # Major outlets
    "Reuters",
    "BBCBreaking",
    "AP",
    "AFP",
    "AJEnglish",
    "BNONews",
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
    "North Korea":(40.3,127.5),"Norway":(60.5,8.5),"Pakistan":(30.4,69.3),
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
    "Zimbabwe":(-20.0,30.0),"West Bank":(31.9,35.2),"Botswana":(-22.3,24.7),
    "Liberia":(6.4,-9.4),"Mauritania":(21.0,-10.9),"Mauritius":(-20.3,57.6),
    "Gaza":(31.4,34.3),"Crimea":(45.3,34.0),"Kosovo":(42.6,20.9),
}

COUNTRY_NAMES = sorted(COUNTRY_COORDS.keys(), key=len, reverse=True)

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
    "shelling","drone strike","wounded","clash","offensive","assault",
]

WARNING_WORDS = [
    "military","troops","conflict","sanction","protest","crisis","threat",
    "arrest","detained","ceasefire","refugee","displaced","mobilization",
    "earthquake","flood","hurricane","tsunami","disaster",
]


def is_relevant(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in CONFLICT_KEYWORDS)


def classify_text(text: str) -> str:
    t = text.lower()
    if any(w in t for w in CRITICAL_WORDS): return "critical"
    if any(w in t for w in WARNING_WORDS):  return "warning"
    return "info"


def extract_country(text: str) -> tuple | None:
    """Find the first country name mentioned in text and return (lat, lng, name)."""
    text_lower = text.lower()
    for name in COUNTRY_NAMES:
        if name.lower() in text_lower:
            coords = COUNTRY_COORDS.get(name)
            if coords:
                # Add small random offset so markers don't all stack on country center
                import random
                lat = coords[0] + random.uniform(-1.5, 1.5)
                lng = coords[1] + random.uniform(-1.5, 1.5)
                return lat, lng, name
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
                # Clean Google News titles (remove " - Source" suffix)
                title = re.sub(r'\s+-\s+[\w\s]+$', '', title).strip()
                if not title or title in seen: continue
                if not is_relevant(title): continue
                seen.add(title)
                items.append({"text": title, "source": "Google News"})
        except Exception as e:
            print(f"⚠️ Google News feed error: {e}")
    print(f"📰 Google News: {len(items)} relevant headlines")
    return items


# ── NITTER RSS ────────────────────────────────────────────────────────────────
def fetch_nitter_rss() -> list:
    items = []
    seen = set()
    working_instance = None

    for account in NEWS_ACCOUNTS:
        fetched = False
        instances = ([working_instance] + NITTER_INSTANCES) if working_instance else NITTER_INSTANCES

        for instance in instances:
            if not instance: continue
            try:
                url  = f"{instance}/{account}/rss"
                feed = feedparser.parse(url)
                if not feed.entries: continue

                working_instance = instance  # Cache working instance
                for entry in feed.entries[:8]:
                    title = entry.get("title","").strip()
                    # Clean up RT/reply prefixes and URLs
                    title = re.sub(r'^R to @\w+:\s*', '', title)
                    title = re.sub(r'https?://\S+', '', title).strip()
                    title = re.sub(r'@\w+', '', title).strip()
                    if not title or title in seen: continue
                    if not is_relevant(title): continue
                    seen.add(title)
                    items.append({"text": title, "source": f"@{account}"})

                fetched = True
                break
            except Exception:
                continue

        if not fetched:
            pass  # Silent fail — try next account

    print(f"🐦 Nitter RSS: {len(items)} relevant posts")
    return items


# ── PROCESS TO MAP EVENTS ─────────────────────────────────────────────────────
def items_to_events(items: list) -> list:
    events = []
    seen_coords = set()

    for item in items:
        text   = item["text"]
        result = extract_country(text)
        if not result: continue

        lat, lng, country = result
        coord_key = f"{round(lat,1)},{round(lng,1)}"
        if coord_key in seen_coords: continue
        seen_coords.add(coord_key)

        events.append({
            "lat":      lat,
            "lng":      lng,
            "message":  text[:200],
            "type":     classify_text(text),
            "location": country,
            "source":   item["source"],
        })

    # Sort critical first
    events.sort(key=lambda x: {"critical":0,"warning":1,"info":2}.get(x["type"],2))
    return events


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────
def get_gdelt_events() -> list:
    """Fetch from Google News RSS + Nitter X accounts."""
    print("📡 Fetching Google News + X RSS...")
    items  = fetch_google_news()
    items += fetch_nitter_rss()
    events = items_to_events(items)
    print(f"✅ {len(events)} geo-tagged events from RSS")
    return events[:150]


def get_news(max_records: int = 50) -> list:
    return []
