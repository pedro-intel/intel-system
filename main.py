import os
import requests
import time
import json
from datetime import datetime
import spacy
from geopy.geocoders import Nominatim
import feedparser

from db import init_db, save_events
from ml_model import train_model, load_model, predict_hotspot

# 🔐 API KEY
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
if not NEWS_API_KEY:
    raise ValueError("Set NEWS_API_KEY using setx")

NEWS_URL = "https://newsapi.org/v2/everything?q=war OR military OR cyber OR attack&language=en&pageSize=50"

OUTPUT_FILE = "intel_data.json"

nlp = spacy.load("en_core_web_sm")
geolocator = Nominatim(user_agent="intel-system")

# 🧠 THREAT MODEL
THREAT_MODEL = {
    "CRITICAL": ["war", "nuclear", "missile", "explosion"],
    "HIGH": ["military", "attack", "conflict"],
    "MEDIUM": ["cyber", "protest", "policy"]
}

# =========================
# FETCH SOURCES
# =========================
def fetch_news():
    r = requests.get(NEWS_URL, params={"apiKey": NEWS_API_KEY})
    return r.json().get("articles", [])

def fetch_rss():
    feed = feedparser.parse("http://feeds.bbci.co.uk/news/world/rss.xml")
    return [{"title": e.title, "description": getattr(e, "summary", "")} for e in feed.entries[:20]]

def fetch_all():
    return fetch_news() + fetch_rss()

# =========================
# AI FUNCTIONS
# =========================
def classify(text):
    t = text.lower()
    for level, words in THREAT_MODEL.items():
        if any(w in t for w in words):
            return level
    return None

def extract_location(text):
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ in ["GPE", "LOC"]:
            try:
                loc = geolocator.geocode(ent.text, timeout=2)
                if loc:
                    return (loc.latitude, loc.longitude)
            except:
                pass
    return None

def summary(title, desc):
    return (title + ". " + desc)[:120]

# =========================
# PROCESS
# =========================
def process(articles):
    events = []

    for a in articles:
        title = a.get("title", "")
        desc = a.get("description", "")
        text = title + " " + desc

        threat = classify(text)
        loc = extract_location(text)

        if threat and loc:
            events.append({
                "title": title,
                "summary": summary(title, desc),
                "threat": threat,
                "lat": loc[0],
                "lon": loc[1],
                "time": datetime.now().strftime("%H:%M:%S")
            })

    return events

# =========================
# MAIN LOOP
# =========================
def main():
    print("🚀 FULL AI INTEL SYSTEM RUNNING")

    init_db()
    model = load_model()

    while True:
        articles = fetch_all()
        events = process(articles)

        save_events(events)

        model = train_model(events) or model
        prediction = predict_hotspot(model, events)

        if prediction:
            print("🔥 Predicted hotspot:", prediction)

        with open(OUTPUT_FILE, "w") as f:
            json.dump(events, f, indent=2)

        time.sleep(20)

if __name__ == "__main__":
    main()