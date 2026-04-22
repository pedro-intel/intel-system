# server.py

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import asyncio
import json
import random
from datetime import datetime

from ml_model import extract_location, classify_event, load_model
from news_ingest import get_news
from db import save_event

app = FastAPI()
clients = []


@app.get("/")
async def home():
    return FileResponse("intel_map.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    print("🟢 Client connected")

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.remove(websocket)
        print("🔴 Client disconnected")


def fake_coordinates():
    return {
        "lat": random.uniform(-60, 70),
        "lng": random.uniform(-180, 180)
    }


async def news_loop():
    while True:
        print("🔄 Fetching news...")

        # 🔥 ALWAYS SEND TEST EVENT
        test_event = {
            "lat": 18.4655,
            "lng": -66.1057,
            "message": "TEST EVENT: Puerto Rico Live",
            "type": "critical",
            "time": datetime.utcnow().isoformat()
        }

        save_event(test_event)

        for client in clients:
            try:
                await client.send_text(json.dumps(test_event))
            except:
                pass

        # 📰 REAL NEWS
        try:
            news_items = get_news()
        except:
            news_items = []

        for article in news_items[:5]:
            location = extract_location(article["title"])

            if location:
                lat, lng = location
            else:
                coords = fake_coordinates()
                lat, lng = coords["lat"], coords["lng"]

            event = {
                "lat": lat,
                "lng": lng,
                "message": article["title"],
                "type": classify_event(article["title"]),
                "time": datetime.utcnow().isoformat()
            }

            print("EVENT:", event["message"])

            save_event(event)

            for client in clients:
                try:
                    await client.send_text(json.dumps(event))
                except:
                    pass

        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    print("🚀 Starting server...")

    # ✅ SAFE MODEL LOAD (won’t crash app)
    load_model()

    asyncio.create_task(news_loop())