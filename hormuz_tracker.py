import asyncio
import json
import websockets
from datetime import datetime, timezone
from collections import defaultdict

# Strait of Hormuz bounding box — widened to catch more vessels
HORMUZ_BOX = {
    "minLat": 24.0,
    "maxLat": 28.0,
    "minLon": 54.0,
    "maxLon": 60.0
}

# Track vessels
vessels_in_strait = {}  # mmsi -> {name, type, timestamp, lat, lon}
daily_transits = {}     # mmsi -> first_seen today
transit_count_today = 0
ship_type_counts = defaultdict(int)

AISSTREAM_KEY = "5cad9d11366d82a7380708612318fbaf7e3e9bd8"

def get_vessel_type(ship_type_code):
    """Convert AIS ship type code to category."""
    if ship_type_code is None:
        return "Unknown"
    code = int(ship_type_code)
    if 80 <= code <= 89:
        return "Tanker"
    elif 70 <= code <= 79:
        return "Cargo"
    elif 60 <= code <= 69:
        return "Passenger"
    elif 30 <= code <= 39:
        return "Fishing"
    elif code in [1, 2, 3, 4, 5]:
        return "Military/Gov"
    elif 50 <= code <= 59:
        return "Service"
    else:
        return "Other"

def get_stats():
    """Return current strait statistics."""
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    
    # Count today's unique transits
    today_count = sum(1 for d in daily_transits.values() if d == today)
    
    # Count by type currently in strait
    by_type = defaultdict(int)
    for v in vessels_in_strait.values():
        by_type[v["type"]] += 1
    
    # Build vessel list sorted by type
    vessel_list = sorted([
        {
            "mmsi": mmsi,
            "name": v.get("name", "Unknown"),
            "type": v.get("type", "Unknown"),
            "lat": v.get("lat", 0),
            "lon": v.get("lon", 0),
            "timestamp": v.get("timestamp", "")
        }
        for mmsi, v in vessels_in_strait.items()
    ], key=lambda x: x["type"])
    
    return {
        "in_strait": len(vessels_in_strait),
        "today_transits": today_count,
        "by_type": dict(by_type),
        "vessels": vessel_list[:50],  # limit to 50
        "last_updated": now.isoformat()
    }

async def run_hormuz_tracker():
    """Connect to AISStream and track Hormuz vessels."""
    global vessels_in_strait, daily_transits
    
    subscribe_msg = {
        "APIKey": AISSTREAM_KEY,
        "BoundingBoxes": [[
            [HORMUZ_BOX["minLat"], HORMUZ_BOX["minLon"]],
            [HORMUZ_BOX["maxLat"], HORMUZ_BOX["maxLon"]]
        ]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
    }
    
    while True:
        try:
            print("🚢 Connecting to AISStream for Hormuz tracking...")
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
                await ws.send(json.dumps(subscribe_msg))
                print("✅ Hormuz tracker connected!")
                
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        msg_type = msg.get("MessageType")
                        meta = msg.get("MetaData", {})
                        mmsi = str(meta.get("MMSI", ""))
                        
                        if not mmsi:
                            continue
                        
                        now = datetime.now(timezone.utc)
                        today = now.date().isoformat()
                        
                        if msg_type == "PositionReport":
                            lat = meta.get("latitude", 0)
                            lon = meta.get("longitude", 0)
                            
                            # Check if in Hormuz bounding box
                            in_box = (
                                HORMUZ_BOX["minLat"] <= lat <= HORMUZ_BOX["maxLat"] and
                                HORMUZ_BOX["minLon"] <= lon <= HORMUZ_BOX["maxLon"]
                            )
                            
                            if in_box:
                                name = meta.get("ShipName", "Unknown").strip()
                                if mmsi not in vessels_in_strait:
                                    print(f"🚢 New vessel in Hormuz: {name} ({mmsi})")
                                
                                vessels_in_strait[mmsi] = {
                                    "name": name,
                                    "type": vessels_in_strait.get(mmsi, {}).get("type", "Unknown"),
                                    "lat": lat,
                                    "lon": lon,
                                    "timestamp": now.isoformat()
                                }
                                
                                # Record daily transit
                                if mmsi not in daily_transits:
                                    daily_transits[mmsi] = today
                            else:
                                # Vessel left the strait
                                if mmsi in vessels_in_strait:
                                    print(f"🚢 Vessel left Hormuz: {vessels_in_strait[mmsi].get('name', mmsi)}")
                                    del vessels_in_strait[mmsi]
                        
                        elif msg_type == "ShipStaticData":
                            ship_type = msg.get("Message", {}).get("ShipStaticData", {}).get("Type", None)
                            if mmsi in vessels_in_strait and ship_type:
                                vessels_in_strait[mmsi]["type"] = get_vessel_type(ship_type)
                    
                    except Exception as e:
                        pass
                        
        except Exception as e:
            print(f"⚠️ Hormuz tracker disconnected: {e}. Reconnecting in 30s...")
            await asyncio.sleep(30)
