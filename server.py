from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
import json
from db import load_history

app = FastAPI()
security = HTTPBasic()

USER = "admin"
PASS = "intel123"

def auth(c: HTTPBasicCredentials = Depends(security)):
    if not (secrets.compare_digest(c.username, USER) and secrets.compare_digest(c.password, PASS)):
        raise HTTPException(status_code=401)
    return c.username

@app.get("/")
def home():
    return FileResponse("intel_map.html")

@app.get("/events")
def events(user: str = Depends(auth)):
    with open("intel_data.json") as f:
        return json.load(f)

@app.get("/history")
def history(user: str = Depends(auth)):
    return load_history()