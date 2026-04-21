import feedparser
import asyncio
import random
import json

RSS_FEEDS = [
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "https://rss.cnn.com/rss/edition_world.rss"
]

def fake_coordinates():
    return {
        "lat": random.uniform(-60, 60),
        "lng": random.uniform(-180, 180)
    }

async def fetch_news(events, clients):
    seen_titles = set()

    while True:
        for url in RSS_FEEDS:
            feed = feedparser.parse(url)

            for entry in feed.entries[:5]:
                if entry.title in seen_titles:
                    continue

                seen_titles.add(entry.title)

                coords = fake_coordinates()

                event = {
                    "lat": coords["lat"],
                    "lng": coords["lng"],
                    "message": entry.title,
                    "type": "critical" if "war" in entry.title.lower() else "info"
                }

                events.append(event)

                for client in clients:
                    try:
                        await client.send_text(json.dumps(event))
                    except:
                        pass

        await asyncio.sleep(60)