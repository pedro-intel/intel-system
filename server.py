from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse, JSONResponse
import json
import asyncio

# 🔥 import the news system
from news_ingest import fetch_news

app = FastAPI()

clients = []
events = []


# 🌍 serve frontend
@app.get("/")
async def root():
    return FileResponse("intel_map.html")


# 📜 history endpoint
@app.get("/history")
async def history():
    return events


# 🔌 websocket
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)

    try:
        while True:
            await websocket.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in clients:
            clients.remove(websocket)


# 📡 manual event endpoint (curl still works)
@app.post("/events")
async def receive_event(request: Request):
    data = await request.json()

    if "lat" not in data or "lng" not in data:
        return {"error": "Missing lat/lng"}

    if "type" not in data:
        data["type"] = "info"

    events.append(data)

    dead_clients = []

    for client in clients:
        try:
            await client.send_text(json.dumps(data))
        except:
            dead_clients.append(client)

    for dc in dead_clients:
        if dc in clients:
            clients.remove(dc)

    return {"status": "sent"}


# 🚀 AUTO NEWS STARTS HERE
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(fetch_news(events, clients))