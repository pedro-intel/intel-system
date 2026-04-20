from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import asyncio
from typing import List

app = FastAPI()

# 🔓 Allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_FILE = "intel_data.json"

# =========================
# 📄 SERVE FRONTEND
# =========================
@app.get("/")
def home():
    return FileResponse("intel_map.html")


# =========================
# 📊 EVENTS API
# =========================
@app.get("/events")
def get_events():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, "r") as f:
        return json.load(f)


# =========================
# 🔌 WEBSOCKET MANAGER
# =========================
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active:
            self.active.remove(websocket)

    async def broadcast(self, data):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# =========================
# 🔌 WEBSOCKET ROUTE
# =========================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# =========================
# 🔄 BACKGROUND PUSH LOOP
# =========================
async def push_updates():
    last_data = None

    while True:
        await asyncio.sleep(3)

        if not os.path.exists(DATA_FILE):
            continue

        with open(DATA_FILE, "r") as f:
            data = json.load(f)

        if data != last_data:
            last_data = data
            await manager.broadcast(data)


# =========================
# 🚀 STARTUP EVENT
# =========================
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(push_updates())