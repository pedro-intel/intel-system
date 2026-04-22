import spacy

# 🔒 SAFE LOAD (prevents crash)
try:
    nlp = spacy.load("en_core_web_sm")
except:
    nlp = None
    print("⚠️ spaCy model not loaded, using fallback")


def extract_location(text):
    if not nlp:
        return None

    doc = nlp(text)

    for ent in doc.ents:
        if ent.label_ in ["GPE", "LOC"]:
            return fake_geocode(ent.text)

    return None


def fake_geocode(place):
    # fallback coords
    import random
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