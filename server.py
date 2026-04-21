from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
import feedparser
import requests

from ml_model import extract_location, classify_event

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

clients = []
seen_titles = set()


# 🌍 REAL geolocation
def get_coordinates(location):
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={location}&format=json&limit=1"
        res = requests.get(url, headers={"User-Agent": "intel-app"})
        data = res.json()

        if data and len(data) > 0:
            return {
                "lat": float(data[0]["lat"]),
                "lng": float(data[0]["lon"])
            }

    except Exception as e:
        print("Geo error:", e)

    return None


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)

    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        clients.remove(websocket)


# 📰 MAIN INTEL LOOP
async def news_loop():
    while True:
        feed = feedparser.parse("https://rss.cnn.com/rss/edition.rss")

        for entry in feed.entries[:10]:

            if entry.title in seen_titles:
                continue

            seen_titles.add(entry.title)

            location = extract_location(entry.title)

            if not location:
                continue

            coords = get_coordinates(location)

            if not coords:
                continue

            event = {
                "lat": coords["lat"],
                "lng": coords["lng"],
                "message": entry.title,
                "type": classify_event(entry.title)
            }

            print("EVENT:", event)  # debug

            for client in clients:
                try:
                    await client.send_text(json.dumps(event))
                except:
                    pass

        await asyncio.sleep(20)


@app.on_event("startup")
async def start():
    asyncio.create_task(news_loop())