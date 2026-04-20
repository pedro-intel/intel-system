from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import asyncio
from typing import List

app = FastAPI()

# CORS (allow your deployed frontend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_FILE = "intel_data.json"

# --- WebSocket manager ---
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

manager = ConnectionManager()

# --- REST endpoint (kept for fallback) ---
@app.get("/events")
def get_events():
    if not os.path.exists(DB_FILE):
        return []
    with open(DB_FILE, "r") as f:
        return json.load(f)

# --- WebSocket endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            # keep connection alive; client doesn't need to send anything
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

# --- Background loop: push updates every few seconds ---
async def push_loop():
    last_hash = None
    while True:
        await asyncio.sleep(3)
        if not os.path.exists(DB_FILE):
            continue
        with open(DB_FILE, "r") as f:
            data = json.load(f)
        # simple change detection
        cur_hash = hash(json.dumps(data))
        if cur_hash != last_hash:
            last_hash = cur_hash
            await manager.broadcast(data)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(push_loop())