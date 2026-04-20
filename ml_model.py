import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib

FILE = "model.pkl"

def train_model(events):
    if len(events) < 5:
        return None

    df = pd.DataFrame(events)

    df["t"] = df["threat"].map({"CRITICAL":3,"HIGH":2,"MEDIUM":1})

    X = df[["lat","lon","t"]]
    y = df["t"]

    model = RandomForestClassifier()
    model.fit(X,y)

    joblib.dump(model, FILE)
    return model

def load_model():
    try:
        return joblib.load(FILE)
    except:
        return None

def predict_hotspot(model, events):
    if not model:
        return None

    df = pd.DataFrame(events)
    df["t"] = df["threat"].map({"CRITICAL":3,"HIGH":2,"MEDIUM":1})

    preds = model.predict(df[["lat","lon","t"]])
    df["p"] = preds

    top = df.sort_values("p", ascending=False).iloc[0]

    return {"lat": top["lat"], "lon": top["lon"], "score": top["p"]}