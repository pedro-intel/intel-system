import spacy

nlp = spacy.load("en_core_web_sm")

def extract_location(text):
    doc = nlp(text)

    for ent in doc.ents:
        if ent.label_ == "GPE":
            return ent.text

    return None


def classify_event(text):
    text = text.lower()

    if any(x in text for x in ["war","attack","missile","strike","explosion","conflict"]):
        return "critical"

    if any(x in text for x in ["president","election","government","policy"]):
        return "political"

    if any(x in text for x in ["economy","market","inflation","stocks"]):
        return "economic"

    return "info"