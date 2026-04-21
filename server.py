from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse
import json

app = FastAPI()

# Store connected clients
clients = []


# ✅ Serve your frontend
@app.get("/")
async def root():
    return FileResponse("intel_map.html")


# ✅ WebSocket endpoint (FIXED)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print("🔥 WS ROUTE HIT")

    await websocket.accept()
    clients.append(websocket)

    print("✅ WS CONNECTED")

    try:
        while True:
            # Keep connection alive
            await websocket.send_text("ping")

    except WebSocketDisconnect:
        print("❌ WS DISCONNECTED")
        if websocket in clients:
            clients.remove(websocket)

    except Exception as e:
        print("⚠️ WS ERROR:", e)
        if websocket in clients:
            clients.remove(websocket)


# ✅ Events endpoint (broadcast to all clients)
@app.post("/events")
async def receive_event(request: Request):
    data = await request.json()
    print("📡 Event received:", data)

    dead_clients = []

    for client in clients:
        try:
            await client.send_text(json.dumps(data))
        except:
            dead_clients.append(client)

    # Clean up disconnected clients
    for client in dead_clients:
        if client in clients:
            clients.remove(client)

    return {"status": "sent", "clients": len(clients)}