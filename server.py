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

clients: list[WebSocket] = []
_news_loop_running = False  # Guard against multiple loop instances


@app.get("/")
async def home():
    return FileResponse("intel_map.html")


@app.head("/")
async def head_home():
    """Handle HEAD requests (Render health checks)."""
    return Response()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    print(f"🟢 Client connected ({len(clients)} total)")

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
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                # Send keepalive ping every 30s
                await websocket.send_text(json.dumps({"ping": True}))
    except (WebSocketDisconnect, Exception):
        if websocket in clients:
            clients.remove(websocket)
        print(f"🔴 Client disconnected ({len(clients)} remaining)")


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
    """Send event to all connected clients, clean up dead connections."""
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
    global _news_loop_running

    # Prevent multiple instances of this loop
    if _news_loop_running:
        print("⚠️ news_loop already running — skipping duplicate")
        return

    _news_loop_running = True
    seen_titles: set = set()
    print("🚀 news_loop started")

    try:
        while True:
            print(f"🔄 Fetching news... ({len(clients)} client(s) connected)")

            news_items = fetch_rss_news()
            new_count = 0

            for article in news_items:
                title = article["title"]

                if title in seen_titles:
                    continue
                seen_titles.add(title)

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
                new_count += 1

                await asyncio.sleep(0.5)

            print(f"✅ Cycle done ({new_count} new events). Waiting 60s...")
            await asyncio.sleep(60)

    except Exception as e:
        print(f"❌ news_loop crashed: {e}")
    finally:
        _news_loop_running = False
        print("⚠️ news_loop exited — will not restart automatically")


@app.on_event("startup")
async def startup_event():
    print("🚀 Starting SENTINEL server...")
    load_model()
    asyncio.create_task(news_loop())