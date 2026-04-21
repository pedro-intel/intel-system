from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import asyncio
import json
import feedparser
import requests
from datetime import datetime

from ml_model import extract_location, classify_event
from db import save_event, get_recent_events

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


# ✅ SERVE FRONTEND (FIXES YOUR 404)
@app.get("/")
async def serve_map():
    return FileResponse("intel_map.html")


# 🌍 Geocoding
def get_coordinates(location):
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={location}&format=json&limit=1"
        res = requests.get(url, headers={"User-Agent": "intel-system"})
        data = res.json()

        if data:
            return {
                "lat": float(data[0]["lat"]),
                "lng": float(data[0]["lon"])
            }

    except Exception as e:
        print("Geo error:", e)

    return None


# 🔌 WebSocket
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)

    print("🟢 Client connected")

    # 🔁 SEND OLD EVENTS (history)
    for row in get_recent_events():
        await websocket.send_text(json.dumps({
            "lat": row[0],
            "lng": row[1],
            "message": row[2],
            "type": row[3],
            "time": row[4]
        }))

    try:
        while True:
            await asyncio.sleep(1)

    except WebSocketDisconnect:
        clients.remove(websocket)
        print("🔴 Client disconnected")


# 📰 News ingestion loop
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
                "type": classify_event(entry.title),
                "time": datetime.utcnow().isoformat()
            }

            print("EVENT:", event["message"])

            # ✅ SAVE TO DB
            save_event(event)

            # ✅ SEND TO CLIENTS
            for client in clients:
                try:
                    await client.send_text(json.dumps(event))
                except:
                    pass

        await asyncio.sleep(20)


# 🚀 Start background task
@app.on_event("startup")
async def start():
    asyncio.create_task(news_loop())