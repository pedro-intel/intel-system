from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import FileResponse
import json

app = FastAPI()

# Store connected clients
clients = []


# ✅ Serve frontend
@app.get("/")
async def root():
    return FileResponse("intel_map.html")


# ✅ WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print("🔥 WS ROUTE HIT")

    await websocket.accept()
    clients.append(websocket)

    print(f"✅ WS CONNECTED | Total clients: {len(clients)}")

    try:
        while True:
            # Wait for client messages (keeps connection alive)
            await websocket.receive_text()

    except WebSocketDisconnect:
        print("❌ WS DISCONNECTED")

    except Exception as e:
        print("⚠️ WS ERROR:", e)

    finally:
        if websocket in clients:
            clients.remove(websocket)
        print(f"👋 Client removed | Total clients: {len(clients)}")


# ✅ Event broadcast endpoint
@app.post("/events")
async def receive_event(request: Request):
    data = await request.json()
    print("📡 Event received:", data)

    dead_clients = []

    for client in clients:
        try:
            await client.send_text(json.dumps(data))
        except Exception as e:
            print("⚠️ Failed to send to client:", e)
            dead_clients.append(client)

    # Remove dead clients
    for dc in dead_clients:
        if dc in clients:
            clients.remove(dc)

    print(f"📤 Sent to {len(clients)} clients")

    return {"status": "sent", "clients": len(clients)}