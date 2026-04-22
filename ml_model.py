# ml_model.py

import random
import requests

# spaCy loaded lazily so it never crashes on import
nlp = None


def load_model():
    """Load spaCy model. Call once at startup."""
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
    Try to geocode a place name using Nominatim (OpenStreetMap).
    Returns (lat, lng) tuple or None.
    """
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": place, "format": "json", "limit": 1}
        headers = {"User-Agent": "intel-system/1.0"}
        res = requests.get(url, params=params, headers=headers, timeout=3)
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
        # Collect all location entities, prioritize GPE (countries/cities) over LOC
        candidates = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
        candidates += [ent.text for ent in doc.ents if ent.label_ == "LOC"]

        # Skip overly short/generic tokens
        candidates = [c for c in candidates if len(c) > 2]

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
    Returns 'critical', 'warning', or 'info'.
    """
    text = text.lower()

    critical_keywords = [
        "war", "nuclear", "missile", "explosion", "attack", "strike",
        "killed", "dead", "bombing", "invaded", "coup", "massacre"
    ]
    warning_keywords = [
        "military", "tension", "conflict", "threat", "protest",
        "sanction", "troops", "armed", "hostage", "crisis"
    ]

    if any(w in text for w in critical_keywords):
        return "critical"
    elif any(w in text for w in warning_keywords):
        return "warning"
    return "info"


# ─── ML Hotspot Prediction (scikit-learn) ───────────────────────────────────

def train_model(events: list):
    """
    Train a simple KMeans clustering model on event coordinates
    to identify geographic hotspots.
    Returns trained model or None if not enough data.
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

        X = numpy_array(coords)
        k = min(3, len(coords))
        model = KMeans(n_clusters=k, random_state=42, n_init=10)
        model.fit(X)
        print(f"✅ KMeans trained with {len(coords)} events, {k} clusters")
        return model

    except Exception as e:
        print(f"⚠️ Model training failed: {e}")
        return None


def numpy_array(data):
    """Helper to create numpy array without top-level numpy import."""
    import numpy as np
    return np.array(data)


def predict_hotspot(model, events: list):
    """
    Use trained KMeans model to find the most active geographic cluster.
    Returns dict with lat/lng of predicted hotspot, or None.
    """
    if model is None or not events:
        return None

    try:
        import numpy as np

        coords = [[e["lat"], e["lng"]] for e in events
                  if "lat" in e and "lng" in e]

        if not coords:
            return None

        # Find cluster with most points
        X = np.array(coords)
        labels = model.predict(X)
        most_common = max(set(labels.tolist()), key=labels.tolist().count)
        center = model.cluster_centers_[most_common]

        return {"lat": round(center[0], 4), "lng": round(center[1], 4)}

    except Exception as e:
        print(f"⚠️ Hotspot prediction failed: {e}")
        return None