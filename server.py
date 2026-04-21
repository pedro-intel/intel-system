from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse
import json
import asyncio

app = FastAPI()

clients = []


# Serve frontend
@app.get("/")
async def root():
    return FileResponse("intel_map.html")


# ✅ FIXED WebSocket (non-blocking, stable)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print("🔥 WS ROUTE HIT")

    await websocket.accept()
    clients.append(websocket)

    print(f"✅ CONNECTED | clients: {len(clients)}")

    try:
        while True:
            await asyncio.sleep(1)  # 👈 KEY FIX (keeps loop alive without blocking)

    except WebSocketDisconnect:
        print("❌ DISCONNECTED")

    finally:
        if websocket in clients:
            clients.remove(websocket)
        print(f"👋 Client removed | clients: {len(clients)}")


# ✅ Broadcast endpoint
@app.post("/events")
async def receive_event(request: Request):
    data = await request.json()
    print("📡 EVENT:", data)

    dead = []

    for client in clients:
        try:
            await client.send_text(json.dumps(data))
        except Exception as e:
            print("⚠️ Failed:", e)
            dead.append(client)

    for d in dead:
        if d in clients:
            clients.remove(d)

    print(f"📤 Sent to {len(clients)} clients")

    return {"status": "sent", "clients": len(clients)}