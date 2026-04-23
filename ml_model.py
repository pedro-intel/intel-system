# ml_model.py

import re
import requests

nlp = None

# ── Country name → approximate center coordinates ────────────────────────────
# Used as fast fallback when Nominatim is too slow or fails
COUNTRY_COORDS = {
    "afghanistan": (33.93, 67.71), "albania": (41.15, 20.17),
    "algeria": (28.03, 1.66), "angola": (11.20, 17.87),
    "argentina": (-38.42, -63.62), "armenia": (40.07, 45.04),
    "australia": (-25.27, 133.78), "austria": (47.52, 14.55),
    "azerbaijan": (40.14, 47.58), "bahrain": (26.00, 50.55),
    "bangladesh": (23.68, 90.36), "belarus": (53.71, 27.95),
    "belgium": (50.50, 4.47), "bolivia": (-16.29, -63.59),
    "bosnia": (43.92, 17.68), "brazil": (-14.24, -51.93),
    "bulgaria": (42.73, 25.49), "burkina faso": (12.36, -1.53),
    "burma": (21.91, 95.96), "myanmar": (21.91, 95.96),
    "cambodia": (12.57, 104.99), "cameroon": (3.85, 11.50),
    "canada": (56.13, -106.35), "chile": (-35.68, -71.54),
    "china": (35.86, 104.19), "colombia": (4.57, -74.30),
    "congo": (-0.23, 15.83), "croatia": (45.10, 15.20),
    "cuba": (21.52, -77.78), "czech": (49.82, 15.47),
    "denmark": (56.26, 9.50), "ecuador": (-1.83, -78.18),
    "egypt": (26.82, 30.80), "ethiopia": (9.14, 40.49),
    "finland": (61.92, 25.75), "france": (46.23, 2.21),
    "georgia": (42.31, 43.36), "germany": (51.17, 10.45),
    "ghana": (7.95, -1.02), "greece": (39.07, 21.82),
    "guatemala": (15.78, -90.23), "guinea": (9.95, -11.61),
    "haiti": (18.97, -72.29), "honduras": (15.20, -86.24),
    "hungary": (47.16, 19.50), "india": (20.59, 78.96),
    "indonesia": (-0.79, 113.92), "iran": (32.43, 53.69),
    "iraq": (33.22, 43.68), "ireland": (53.41, -8.24),
    "israel": (31.05, 34.85), "italy": (41.87, 12.57),
    "japan": (36.20, 138.25), "jordan": (30.59, 36.24),
    "kazakhstan": (48.02, 66.92), "kenya": (-0.02, 37.91),
    "kosovo": (42.60, 20.90), "kuwait": (29.31, 47.48),
    "kyrgyzstan": (41.20, 74.77), "laos": (19.86, 102.50),
    "latvia": (56.88, 24.60), "lebanon": (33.85, 35.86),
    "libya": (26.34, 17.23), "lithuania": (55.17, 23.88),
    "luxembourg": (49.82, 6.13), "malaysia": (4.21, 108.96),
    "mali": (17.57, -3.99), "mexico": (23.63, -102.55),
    "moldova": (47.41, 28.37), "mongolia": (46.86, 103.85),
    "morocco": (31.79, -7.09), "mozambique": (-18.67, 35.53),
    "namibia": (-22.96, 18.49), "nepal": (28.39, 84.12),
    "netherlands": (52.13, 5.29), "nicaragua": (12.87, -85.21),
    "niger": (17.61, 8.08), "nigeria": (9.08, 8.68),
    "north korea": (40.34, 127.51), "norway": (60.47, 8.47),
    "pakistan": (30.38, 69.35), "palestine": (31.95, 35.23),
    "panama": (8.54, -80.78), "peru": (-9.19, -75.02),
    "philippines": (12.88, 121.77), "poland": (51.92, 19.15),
    "portugal": (39.40, -8.22), "qatar": (25.35, 51.18),
    "romania": (45.94, 24.97), "russia": (61.52, 105.32),
    "rwanda": (-1.94, 29.87), "saudi arabia": (23.89, 45.08),
    "senegal": (14.50, -14.45), "serbia": (44.02, 21.01),
    "sierra leone": (8.46, -11.78), "somalia": (5.15, 46.20),
    "south africa": (-30.56, 22.94), "south korea": (35.91, 127.77),
    "south sudan": (6.88, 31.31), "spain": (40.46, -3.75),
    "sri lanka": (7.87, 80.77), "sudan": (12.86, 30.22),
    "sweden": (60.13, 18.64), "switzerland": (46.82, 8.23),
    "syria": (34.80, 38.99), "taiwan": (23.70, 121.00),
    "tajikistan": (38.86, 71.28), "tanzania": (-6.37, 34.89),
    "thailand": (15.87, 100.99), "tunisia": (33.89, 9.54),
    "turkey": (38.96, 35.24), "turkiye": (38.96, 35.24),
    "uganda": (1.37, 32.29), "ukraine": (48.38, 31.17),
    "united arab emirates": (23.42, 53.85), "uae": (23.42, 53.85),
    "united kingdom": (55.38, -3.44), "uk": (55.38, -3.44),
    "united states": (37.09, -95.71), "usa": (37.09, -95.71),
    "us": (37.09, -95.71), "america": (37.09, -95.71),
    "uruguay": (-32.52, -55.77), "uzbekistan": (41.38, 64.59),
    "venezuela": (6.42, -66.59), "vietnam": (14.06, 108.28),
    "yemen": (15.55, 48.52), "zambia": (-13.13, 27.85),
    "zimbabwe": (-19.02, 29.15),
    # Major cities
    "moscow": (55.75, 37.62), "beijing": (39.91, 116.39),
    "washington": (38.91, -77.04), "london": (51.51, -0.13),
    "paris": (48.86, 2.35), "berlin": (52.52, 13.40),
    "tokyo": (35.69, 139.69), "kyiv": (50.45, 30.52),
    "kiev": (50.45, 30.52), "tehran": (35.69, 51.39),
    "gaza": (31.35, 34.31), "kabul": (34.53, 69.17),
    "baghdad": (33.34, 44.40), "damascus": (33.51, 36.29),
    "jerusalem": (31.77, 35.22), "ankara": (39.93, 32.86),
    "riyadh": (24.69, 46.72), "cairo": (30.06, 31.25),
    "islamabad": (33.72, 73.04), "new delhi": (28.61, 77.21),
    "delhi": (28.61, 77.21), "mumbai": (19.08, 72.88),
    "pyongyang": (39.02, 125.75), "seoul": (37.57, 126.98),
    "taipei": (25.03, 121.56), "bangkok": (13.75, 100.52),
    "jakarta": (-6.21, 106.85), "manila": (14.60, 120.98),
}


def load_model():
    """Load spaCy model. Called once at startup."""
    global nlp
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
        print("✅ spaCy model loaded")
    except Exception as e:
        print(f"⚠️ spaCy failed to load: {e}")
        nlp = None


def lookup_country_coords(text: str):
    """
    Fast O(n) scan of text for known country/city names.
    Returns (lat, lng) or None. No network call needed.
    """
    text_lower = text.lower()
    # Sort by length descending so "south korea" matches before "korea"
    for name, coords in sorted(COUNTRY_COORDS.items(), key=lambda x: -len(x[0])):
        if re.search(rf'\b{re.escape(name)}\b', text_lower):
            return coords
    return None


def geocode_place(place: str):
    """
    Geocode via Nominatim. Used only when spaCy finds a specific city/place
    not in our lookup table.
    Returns (lat, lng) or None.
    """
    try:
        res = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": place, "format": "json", "limit": 1},
            headers={"User-Agent": "intel-system/1.0"},
            timeout=3
        )
        data = res.json()
        if data:
            return (float(data[0]["lat"]), float(data[0]["lon"]))
    except Exception:
        pass
    return None


def extract_location(text: str):
    """
    Try to find a real location for the event using a 3-tier strategy:
    1. spaCy NER → Nominatim geocode (most accurate for specific cities)
    2. Country/city lookup table (fast, no network)
    3. Return None — event is skipped, no fake coords

    Returns (lat, lng) or None.
    """
    # Tier 1: spaCy NER + Nominatim
    if nlp:
        try:
            doc = nlp(text)
            candidates = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
            candidates += [ent.text for ent in doc.ents if ent.label_ == "LOC"]
            candidates = [c for c in candidates if len(c) > 2 and not c.isnumeric()]

            for place in candidates:
                # First try our fast lookup table
                coords = lookup_country_coords(place)
                if coords:
                    return coords
                # Then try Nominatim for specific places not in our table
                coords = geocode_place(place)
                if coords:
                    return coords
        except Exception as e:
            print(f"⚠️ spaCy extraction error: {e}")

    # Tier 2: direct text scan against lookup table
    coords = lookup_country_coords(text)
    if coords:
        return coords

    # Tier 3: no location found — caller should skip this event
    return None


def classify_event(text: str) -> str:
    """
    Classify event severity. Returns 'critical', 'warning', or 'info'.
    """
    text_lower = text.lower()

    def has_word(word):
        return bool(re.search(rf'\b{re.escape(word)}\b', text_lower))

    critical_keywords = [
        "war", "nuclear", "missile", "airstrike", "air strike",
        "bombing", "bombed", "explosion", "exploded",
        "coup", "massacre", "invasion", "invaded", "assassinated",
        "mass shooting", "killed in", "dead in", "casualties",
        "rockets fired", "troops advance", "offensive launched"
    ]

    warning_keywords = [
        "military", "troops", "conflict", "sanction", "sanctions",
        "hostage", "armed", "protest", "crisis", "threatens",
        "clashes", "clashed", "detained", "arrested", "sentenced",
        "gang", "cartel", "smuggling", "refugee", "displaced",
        "earthquake", "flood", "hurricane", "tsunami", "disaster"
    ]

    # Use substring match for multi-word critical phrases
    for w in critical_keywords:
        if w in text_lower:
            return "critical"

    # Use whole-word match for warning keywords to avoid false positives
    for w in warning_keywords:
        if has_word(w):
            return "warning"

    return "info"


# ── ML Hotspot Prediction ────────────────────────────────────────────────────

def train_model(events: list):
    """Train KMeans on event coordinates. Returns model or None."""
    if not events or len(events) < 3:
        return None
    try:
        from sklearn.cluster import KMeans
        import numpy as np
        coords = [[e["lat"], e["lng"]] for e in events if "lat" in e and "lng" in e]
        if len(coords) < 3:
            return None
        k = min(3, len(coords))
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        model.fit(np.array(coords))
        print(f"✅ KMeans trained: {len(coords)} events, {k} clusters")
        return model
    except Exception as e:
        print(f"⚠️ Model training failed: {e}")
        return None


def predict_hotspot(model, events: list):
    """Return most active cluster center as {lat, lng} or None."""
    if model is None or not events:
        return None
    try:
        import numpy as np
        coords = [[e["lat"], e["lng"]] for e in events if "lat" in e and "lng" in e]
        if not coords:
            return None
        X = np.array(coords)
        labels = model.predict(X).tolist()
        most_common = max(set(labels), key=labels.count)
        center = model.cluster_centers_[most_common]
        return {"lat": round(center[0], 4), "lng": round(center[1], 4)}
    except Exception as e:
        print(f"⚠️ Hotspot prediction failed: {e}")
        return None


# Approximate country center coordinates for RSS geocoding
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
}


def lookup_country_coords(country_name: str):
    """Return (lat, lng) tuple for a country name, or None if not found."""
    return COUNTRY_COORDS.get(country_name)
