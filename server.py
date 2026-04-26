# server.py

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import asyncio
import json
import csv
import io
from datetime import datetime

from ml_model import classify_event, load_model
from hormuz_tracker import run_hormuz_tracker, get_stats as get_hormuz_stats
from db import save_event, get_conn, cleanup_old_events, get_seen_keys, add_seen_key, cleanup_seen_keys
from news_ingest import get_news_events, fetch_nitter_rss, fetch_google_news, items_to_events

app = FastAPI()

clients: list[WebSocket] = []
_loop_running = False

# ── DISCORD WEBHOOK ───────────────────────────────────────────────────────────
import os
import requests as _requests

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
_last_discord_alert = {}  # country → timestamp, to avoid spam


def send_discord_alert(event: dict):
    """Send critical event to Discord webhook if configured."""
    if not DISCORD_WEBHOOK:
        return

    country = event.get("location", "Unknown")
    now = datetime.utcnow()

    # Rate limit: max 1 alert per country per 30 minutes
    last = _last_discord_alert.get(country)
    if last and (now - last).seconds < 1800:
        return
    _last_discord_alert[country] = now

    try:
        color = 0xff3e3e if event["type"] == "critical" else 0xffb800
        payload = {
            "embeds": [{
                "title": f"⚠️ {event['type'].upper()} EVENT — {country}",
                "description": event.get("message", "No details"),
                "color": color,
                "fields": [
                    {"name": "Location", "value": country, "inline": True},
                    {"name": "Source", "value": event.get("source", "SENTINEL"), "inline": True},
                    {"name": "Coordinates", "value": f"{event['lat']:.4f}, {event['lng']:.4f}", "inline": True},
                    {"name": "Time", "value": event.get("time", now.isoformat()) + " UTC", "inline": False},
                ],
                "footer": {"text": "SENTINEL // Global Intelligence Monitor"},
                "timestamp": now.isoformat(),
            }]
        }
        _requests.post(DISCORD_WEBHOOK, json=payload, timeout=5)
        print(f"🔔 Discord alert sent: {country}")
    except Exception as e:
        print(f"⚠️ Discord alert failed: {e}")


# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.get("/")
async def home():
    return FileResponse("intel_map.html")


@app.head("/")
async def head_home():
    return Response()


@app.get("/favicon.ico")
async def favicon():
    return FileResponse("favicon.ico") if os.path.exists("favicon.ico") else Response(status_code=404)


# ── REST API ──────────────────────────────────────────────────────────────────
@app.get("/api/events")
async def get_events(hours: int = Query(default=24, ge=1, le=72)):
    """Return all events from the last N hours for the timeline slider."""
    from db import get_events_since
    events = get_events_since(hours=hours)
    return JSONResponse(content={"events": events, "count": len(events)})


@app.get("/api/events/export")
async def export_events(
    hours: int = Query(default=24, ge=1, le=72),
    fmt: str = Query(default="json")
):
    """Export events as JSON or CSV."""
    from db import get_events_since
    events = get_events_since(hours=hours)

    if fmt == "csv":
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["lat", "lng", "message", "type", "time"])
        writer.writeheader()
        writer.writerows(events)
        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=sentinel_events_{hours}h.csv"}
        )

    return JSONResponse(
        content={"events": events, "count": len(events), "hours": hours},
        headers={"Content-Disposition": f"attachment; filename=sentinel_events_{hours}h.json"}
    )


@app.get("/api/stats")
async def get_stats():
    """Return summary statistics."""
    from db import get_events_since
    events = get_events_since(hours=24)
    counts = {"critical": 0, "warning": 0, "info": 0}
    countries = {}
    for e in events:
        counts[e.get("type", "info")] = counts.get(e.get("type", "info"), 0) + 1
        c = e.get("location", "Unknown")
        countries[c] = countries.get(c, 0) + 1

    top_countries = sorted(countries.items(), key=lambda x: -x[1])[:10]
    return JSONResponse(content={
        "total": len(events),
        "by_type": counts,
        "top_countries": [{"country": k, "count": v} for k, v in top_countries],
        "period_hours": 24,
    })


@app.get("/api/health")
async def health():
    return JSONResponse(content={"status": "ok", "timestamp": datetime.utcnow().isoformat()})


# ── WEBSOCKET ─────────────────────────────────────────────────────────────────
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
            "message": row[2], "type": row[3], "time": row[4],
            "source": row[5] if len(row) > 5 else "Unknown",
            "location": row[6] if len(row) > 6 else "Unknown",
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


# ── MAIN INTEL LOOP ───────────────────────────────────────────────────────────
# DB-backed seen_keys — persists across restarts
_seen_keys: set = set()
_seen_keys_loaded: bool = False

async def _process_events(events: list, source_label: str):
    """Broadcast new events, dedup against DB-backed seen_keys."""
    global _seen_keys, _seen_keys_loaded
    # Load seen keys from DB on first run
    if not _seen_keys_loaded:
        loop = asyncio.get_event_loop()
        _seen_keys = await loop.run_in_executor(None, get_seen_keys)
        _seen_keys_loaded = True
        print(f"📋 Loaded {len(_seen_keys)} seen keys from DB")
    loop = asyncio.get_event_loop()
    new_count = 0
    skipped = 0
    for event in events:
        key = f"{round(event['lat'],2)},{round(event['lng'],2)}:{event['message'][:40]}"
        if key in _seen_keys:
            skipped += 1
            continue
        _seen_keys.add(key)
        await loop.run_in_executor(None, add_seen_key, key)
        event["time"] = datetime.utcnow().isoformat()
        print(f"📡 [{event['type'].upper()}] {event.get('location','?')}: {event['message'][:70]}")
        save_event(event)
        await broadcast(event)
        if event["type"] == "critical":
            await loop.run_in_executor(None, send_discord_alert, event)
        new_count += 1
        await asyncio.sleep(0.1)
    if new_count or skipped:
        print(f"✅ {source_label} — {new_count} new, {skipped} dupes")
    return new_count


async def intel_loop():
    """Main loop: Google News every 10 minutes."""
    global _loop_running
    if _loop_running:
        print("⚠️ intel_loop already running — skipping duplicate")
        return
    _loop_running = True
    print("🚀 SENTINEL intelligence loop started")
    try:
        while True:
            print(f"🌐 Fetching Google News... ({len(clients)} client(s) connected)")
            loop = asyncio.get_event_loop()
            try:
                items = await loop.run_in_executor(None, fetch_google_news)
                events = await loop.run_in_executor(None, items_to_events, items)
            except Exception as e:
                print(f"⚠️ Google News fetch error: {e}")
                events = []
            await _process_events(events, "Google News cycle")
            cleanup_old_events(hours=24)
            print("⏳ Google News waiting 10min...")
            await asyncio.sleep(600)
    except Exception as e:
        print(f"❌ intel_loop crashed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        _loop_running = False
        print("⚠️ intel_loop exited")


async def nitter_loop():
    """Fast loop: X/Nitter every 2 minutes."""
    print("🐦 SENTINEL Nitter loop started")
    await asyncio.sleep(30)  # stagger start
    while True:
        try:
            loop = asyncio.get_event_loop()
            items = await loop.run_in_executor(None, fetch_nitter_rss)
            events = await loop.run_in_executor(None, items_to_events, items)
            await _process_events(events, "Nitter cycle")
        except Exception as e:
            print(f"⚠️ Nitter loop error: {e}")
        await asyncio.sleep(120)  # every 2 minutes


async def watchdog():
    """Restart intel_loop if it crashes."""
    global _loop_running
    while True:
        if not _loop_running:
            print("🔄 Watchdog: restarting intel loop...")
            asyncio.create_task(intel_loop())
        await asyncio.sleep(60)


@app.get("/api/hormuz")
async def hormuz_stats():
    """Live Hormuz strait vessel statistics."""
    try:
        from hormuz_tracker import get_stats
        return get_stats()
    except Exception as e:
        return {"in_strait": 0, "today_transits": 0, "by_type": {}, "vessels": [], "error": str(e)}

@app.on_event("startup")
async def startup_event():
    print("🚀 Starting SENTINEL...")
    # Load spaCy in background — don't block startup
    async def load_model_bg():
        try:
            load_model()
            print("✅ spaCy model loaded")
        except Exception as e:
            print(f"⚠️ spaCy load failed: {e}")
    asyncio.create_task(load_model_bg())
    cleanup_old_events(hours=24)  # Remove old/GDELT events on startup
    asyncio.create_task(intel_loop())
    asyncio.create_task(nitter_loop())
    asyncio.create_task(watchdog())
    asyncio.create_task(run_hormuz_tracker())
