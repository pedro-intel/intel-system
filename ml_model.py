# ml_model.py

import requests

# spaCy loaded lazily so it never crashes on import
nlp = None


def load_model():
    """Load spaCy model. Called once at startup."""
    global nlp
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
        print("✅ spaCy model loaded")
    except Exception as e:
        print(f"⚠️ spaCy failed to load: {e}. Location extraction disabled.")
        nlp = None


def geocode_place(place: str):
    """
    Geocode a place name via Nominatim (OpenStreetMap).
    Returns (lat, lng) tuple or None.
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
    Use spaCy NER to find GPE/LOC entities, then geocode the best one.
    Returns (lat, lng) tuple or None.
    """
    if not nlp:
        return None

    try:
        doc = nlp(text)

        # Prioritize GPE (countries, cities, states) over generic LOC
        candidates = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
        candidates += [ent.text for ent in doc.ents if ent.label_ == "LOC"]

        # Filter out very short or purely numeric tokens
        candidates = [c for c in candidates if len(c) > 2 and not c.isnumeric()]

        for place in candidates:
            coords = geocode_place(place)
            if coords:
                return coords

    except Exception as e:
        print(f"⚠️ Location extraction error: {e}")

    return None


def classify_event(text: str) -> str:
    """
    Classify event severity based on keyword matching.
    Uses whole-word matching to reduce false positives.
    Returns 'critical', 'warning', or 'info'.
    """
    import re
    text_lower = text.lower()

    def has_word(word):
        return bool(re.search(rf'\b{re.escape(word)}\b', text_lower))

    # CRITICAL: direct violence / major military events
    critical_keywords = [
        "war", "nuclear", "missile", "airstrike", "air strike",
        "bombing", "bombed", "explosion", "exploded", "attack",
        "coup", "massacre", "invasion", "invaded", "assassinated",
        "mass shooting", "killed in", "dead in", "casualties"
    ]

    # WARNING: tensions, military posturing, political crisis
    warning_keywords = [
        "military", "troops", "conflict", "sanction", "sanctions",
        "hostage", "armed", "protest", "crisis", "threat", "threatens",
        "clashes", "clashed", "detained", "arrested", "sentenced",
        "trial", "gang", "cartel", "smuggling", "refugee", "displaced"
    ]

    for w in critical_keywords:
        if w in text_lower:
            return "critical"

    for w in warning_keywords:
        if has_word(w):
            return "warning"

    return "info"


# ── ML Hotspot Prediction ────────────────────────────────────────────────────

def train_model(events: list):
    """
    Train KMeans clustering on event coordinates to identify hotspots.
    Returns trained model or None.
    """
    if not events or len(events) < 3:
        return None

    try:
        from sklearn.cluster import KMeans
        import numpy as np

        coords = [[e["lat"], e["lng"]] for e in events
                  if "lat" in e and "lng" in e]

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
    """
    Find the most active geographic cluster center.
    Returns dict {lat, lng} or None.
    """
    if model is None or not events:
        return None

    try:
        import numpy as np

        coords = [[e["lat"], e["lng"]] for e in events
                  if "lat" in e and "lng" in e]

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