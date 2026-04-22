# server.py

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response, Query
from fastapi.responses import FileResponse, JSONResponse
import asyncio
import json
from datetime import datetime

from ml_model import classify_event, load_model
from db import save_event
from news_ingest import get_gdelt_events

app = FastAPI()

clients: list[WebSocket] = []
_loop_running = False


@app.get("/")
async def home():
    return FileResponse("intel_map.html")


@app.head("/")
async def head_home():
    return Response()


@app.get("/api/events")
async def get_events(hours: int = Query(default=24, ge=1, le=72)):
    """
    Return all events from the last N hours for the timeline slider.
    Ordered oldest-first so the frontend can replay in sequence.
    """
    from db import get_events_since
    events = get_events_since(hours=hours)
    return JSONResponse(content={"events": events, "count": len(events)})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    print(f"🟢 Client connected ({len(clients)} total)")

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
    global _loop_running

    if _loop_running:
        print("⚠️ gdelt_loop already running — skipping duplicate")
        return

    _loop_running = True
    print("🚀 GDELT CSV intelligence loop started")

    seen_keys: set = set()

    try:
        while True:
            print(f"🌐 Fetching GDELT CSV... ({len(clients)} client(s) connected)")

            loop = asyncio.get_event_loop()
            try:
                events = await loop.run_in_executor(None, get_gdelt_events)
            except Exception as e:
                print(f"⚠️ GDELT fetch error: {e}")
                events = []

            new_count = 0
            skipped   = 0

            for event in events:
                key = f"{round(event['lat'],2)},{round(event['lng'],2)}:{event['message'][:40]}"
                if key in seen_keys:
                    skipped += 1
                    continue
                seen_keys.add(key)

                if len(seen_keys) > 2000:
                    seen_keys.clear()

                event["time"] = datetime.utcnow().isoformat()

                print(f"📡 [{event['type'].upper()}] {event.get('location','?')}: {event['message'][:70]}")

                save_event(event)
                await broadcast(event)
                new_count += 1

                await asyncio.sleep(0.2)

            print(f"✅ Cycle done — {new_count} new events, {skipped} dupes. Waiting 6h...")
            await asyncio.sleep(900)

    except Exception as e:
        print(f"❌ gdelt_loop crashed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        _loop_running = False
        print("⚠️ gdelt_loop exited")


@app.on_event("startup")
async def startup_event():
    print("🚀 Starting SENTINEL (ACLED mode)...")
    load_model()
    asyncio.create_task(gdelt_loop())
