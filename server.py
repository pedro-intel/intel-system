from fastapi import Request
import json

@app.post("/events")
async def receive_event(request: Request):
    data = await request.json()
    print("Event:", data)

    for client in clients:
        await client.send_text(json.dumps(data))

    return {"status": "sent"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print("🔥 WS ROUTE HIT")

    await websocket.accept()
    print("✅ WS CONNECTED")

    while True:
        await websocket.send_text("ping")