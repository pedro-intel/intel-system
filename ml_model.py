# ml_model.py

import random

# ❌ DO NOT load spacy at import time (this caused your crash)
nlp = None

def load_model():
    global nlp
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
        print("✅ spaCy loaded")
    except Exception as e:
        print("⚠️ spaCy failed:", e)
        nlp = None


def extract_location(text):
    if not nlp:
        return None

    doc = nlp(text)

    for ent in doc.ents:
        if ent.label_ in ["GPE", "LOC"]:
            return fake_geocode(ent.text)

    return None


def fake_geocode(place):
    return (
        random.uniform(-60, 70),
        random.uniform(-180, 180)
    )


def classify_event(text):
    text = text.lower()

    if any(word in text for word in ["war", "attack", "military", "explosion"]):
        return "critical"
    elif any(word in text for word in ["protest", "tension"]):
        return "warning"
    else:
        return "info"