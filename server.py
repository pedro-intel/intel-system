# server.py

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import FileResponse
import asyncio
import json
import random
import feedparser
from datetime import datetime

from ml_model import extract_location, classify_event, load_model
from db import save_event

app = FastAPI()

# Thread-safe client list
clients: list[WebSocket] = []


@app.get("/")
async def home():
    return FileResponse("intel_map.html")


@app.head("/")
async def head_home():
    """Handle HEAD requests (used by Render health checks)."""
    return Response()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    print("🟢 Client connected")

    # Replay recent events from DB so map isn't empty on connect
    from db import get_recent_events
    recent = get_recent_events(limit=50)
    for row in recent:
        event = {
            "lat": row[0], "lng": row[1],
            "message": row[2], "type": row[3], "time": row[4]
        }
        try:
            await websocket.send_text(json.dumps(event))
        except Exception:
            pass

    try:
        while True:
            # Properly wait for client messages or disconnect signals
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=60)
            except asyncio.TimeoutError:
                # Send a ping to keep connection alive
                await websocket.send_text(json.dumps({"ping": True}))
    except (WebSocketDisconnect, Exception):
        if websocket in clients:
            clients.remove(websocket)
        print("🔴 Client disconnected")


def fake_coordinates():
    return {
        "lat": random.uniform(-60, 70),
        "lng": random.uniform(-180, 180)
    }


def fetch_rss_news():
    """Fetch news from RSS feeds. Returns list of {title, summary} dicts."""
    RSS_FEEDS = [
        "http://feeds.bbci.co.uk/news/world/rss.xml",
        "https://rss.cnn.com/rss/edition_world.rss",
    ]
    items = []
    for url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:10]:
                items.append({
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", "")
                })
        except Exception as e:
            print(f"⚠️ RSS error for {url}: {e}")
    return items


async def broadcast(event: dict):
    """Send event to all connected clients, remove dead ones."""
    dead = []
    for client in clients:
        try:
            await client.send_text(json.dumps(event))
        except Exception:
            dead.append(client)
    for c in dead:
        if c in clients:
            clients.remove(c)


async def news_loop():
    """Main loop: fetch RSS, classify, geolocate, broadcast."""
    seen_titles: set = set()

    while True:
        print(f"🔄 Fetching news... ({len(clients)} client(s) connected)")

        news_items = fetch_rss_news()

        for article in news_items:
            title = article["title"]

            # Skip already-seen headlines
            if title in seen_titles:
                continue
            seen_titles.add(title)

            # Keep seen set from growing unbounded
            if len(seen_titles) > 500:
                seen_titles.clear()

            text = title + " " + article.get("summary", "")
            location = extract_location(text)

            if location:
                lat, lng = location
            else:
                coords = fake_coordinates()
                lat, lng = coords["lat"], coords["lng"]

            event = {
                "lat": lat,
                "lng": lng,
                "message": title,
                "type": classify_event(text),
                "time": datetime.utcnow().isoformat()
            }

            print(f"📡 EVENT [{event['type'].upper()}]: {title[:80]}")

            save_event(event)
            await broadcast(event)

            # Small delay so frontend isn't flooded
            await asyncio.sleep(0.5)

        print(f"✅ Cycle done. Waiting 60s...")
        await asyncio.sleep(60)


@app.on_event("startup")
async def startup_event():
    print("🚀 Starting SENTINEL server...")
    load_model()
    asyncio.create_task(news_loop())