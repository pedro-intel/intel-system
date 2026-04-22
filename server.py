# server.py

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import FileResponse
import asyncio
import json
from datetime import datetime

from ml_model import extract_location, classify_event, load_model
from db import save_event
from news_ingest import get_news, get_geo_events, is_relevant

app = FastAPI()

clients: list[WebSocket] = []
_news_loop_running = False


@app.get("/")
async def home():
    return FileResponse("intel_map.html")


@app.head("/")
async def head_home():
    return Response()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    print(f"🟢 Client connected ({len(clients)} total)")

    # Replay recent events from DB on connect
    from db import get_recent_events
    recent = get_recent_events(limit=100)
    replayed = 0
    for row in recent:
        event = {
            "lat": row[0], "lng": row[1],
            "message": row[2], "type": row[3], "time": row[4]
        }
        try:
            await websocket.send_text(json.dumps(event))
            replayed += 1
        except Exception:
            pass
    print(f"📤 Replayed {replayed} historical events to new client")

    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30)
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"ping": True}))
    except (WebSocketDisconnect, Exception):
        if websocket in clients:
            clients.remove(websocket)
        print(f"🔴 Client disconnected ({len(clients)} remaining)")


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


async def gdelt_loop():
    """
    Main intelligence loop using GDELT APIs.
    Strategy:
      1. Fetch geo-tagged location points from GDELT GEO API
         → These have real lat/lng from GDELT's NLP pipeline
      2. Fetch article titles from GDELT DOC API
         → Match titles to locations by sourcecountry / NLP
      3. Broadcast combined events to all clients
    """
    global _news_loop_running

    if _news_loop_running:
        print("⚠️ gdelt_loop already running — skipping duplicate")
        return

    _news_loop_running = True
    seen_titles: set = set()
    print("🚀 GDELT intelligence loop started")

    try:
        while True:
            print(f"🌐 Fetching GDELT data... ({len(clients)} client(s) connected)")

            # ── Step 1: Get geo-tagged locations from GDELT GEO API ──
            geo_points = []
            try:
                geo_points = get_geo_events(timespan="1h")
                print(f"📍 GDELT GEO: {len(geo_points)} location points")
            except Exception as e:
                print(f"⚠️ GEO fetch error: {e}")

            # ── Step 2: Get article titles from GDELT DOC API ──
            articles = []
            try:
                articles = get_news(max_records=50)
                print(f"📰 GDELT DOC: {len(articles)} articles")
            except Exception as e:
                print(f"⚠️ DOC fetch error: {e}")

            new_count = 0
            skipped = 0

            # ── Step 3: Process geo points + match to article titles ──
            if geo_points and articles:
                # Build a quick title lookup by sourcecountry
                country_titles: dict = {}
                for a in articles:
                    sc = a.get("sourcecountry", "").upper()
                    if sc and sc not in country_titles:
                        if is_relevant(a.get("title", "")):
                            country_titles[sc] = a["title"]

                for point in geo_points[:30]:  # Top 30 hottest locations
                    lat = point["lat"]
                    lng = point["lng"]
                    name = point["name"]
                    context = point.get("context", "")

                    # Try to find a matching article title
                    # First try context snippet, then country match, then location name
                    title = context[:120] if context else None

                    if not title:
                        # Try to match by country code
                        title = country_titles.get(name.upper())

                    if not title:
                        # Fall back to first relevant article
                        for a in articles[:5]:
                            if is_relevant(a.get("title", "")):
                                title = a["title"]
                                break

                    if not title:
                        title = f"Intelligence activity detected near {name}"

                    # Deduplicate
                    dedup_key = f"{round(lat,1)},{round(lng,1)}:{title[:50]}"
                    if dedup_key in seen_titles:
                        skipped += 1
                        continue
                    seen_titles.add(dedup_key)

                    if len(seen_titles) > 1000:
                        seen_titles.clear()

                    event = {
                        "lat":     lat,
                        "lng":     lng,
                        "message": title,
                        "type":    classify_event(title + " " + context),
                        "time":    datetime.utcnow().isoformat(),
                        "source":  "GDELT",
                        "location": name,
                    }

                    print(f"📡 [{event['type'].upper()}] {name}: {title[:60]}")

                    save_event(event)
                    await broadcast(event)
                    new_count += 1

                    await asyncio.sleep(0.3)

            # ── Fallback: if GEO API returns nothing, use DOC + our NLP ──
            elif articles:
                print("⚠️ GEO API empty — falling back to NLP location extraction")
                for article in articles[:20]:
                    title = article["title"]

                    if title in seen_titles:
                        skipped += 1
                        continue

                    if not is_relevant(title):
                        skipped += 1
                        continue

                    seen_titles.add(title)

                    text = title
                    location = extract_location(text)

                    if not location:
                        skipped += 1
                        print(f"⏭️  Skipped (no location): {title[:60]}")
                        continue

                    lat, lng = location

                    event = {
                        "lat":     lat,
                        "lng":     lng,
                        "message": title,
                        "type":    classify_event(text),
                        "time":    datetime.utcnow().isoformat(),
                        "source":  "GDELT-DOC",
                    }

                    print(f"📡 [{event['type'].upper()}] {title[:60]}")

                    save_event(event)
                    await broadcast(event)
                    new_count += 1

                    await asyncio.sleep(0.3)

            print(f"✅ Cycle done — {new_count} events, {skipped} skipped. Waiting 60s...")
            await asyncio.sleep(60)

    except Exception as e:
        print(f"❌ gdelt_loop crashed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        _news_loop_running = False
        print("⚠️ gdelt_loop exited")


@app.on_event("startup")
async def startup_event():
    print("🚀 Starting SENTINEL server (GDELT mode)...")
    load_model()
    asyncio.create_task(gdelt_loop())
