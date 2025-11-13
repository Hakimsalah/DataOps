import os, time, json, requests
from confluent_kafka import Producer
from datetime import datetime

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "realtime-products")
API = "https://dummyjson.com/products?limit=30"
INTERVAL = 60  # secondes

p = Producer({'bootstrap.servers': KAFKA})

def send(rec):
    p.produce(TOPIC, json.dumps(rec).encode('utf-8'))
    p.poll(0)

def fetch_and_send():
    try:
        r = requests.get(API, timeout=10)
        r.raise_for_status()
        data = r.json().get("products", [])
        for item in data:
            rec = {
                "product_id": int(item.get("id", 0)),
                "product_name": item.get("title"),
                "price": float(item.get("price", 0)),
                "category": item.get("category"),
                "brand": item.get("brand"),
                "rating": float(item.get("rating", 0)) if item.get("rating") is not None else None,
                "stock": int(item.get("stock", 0)) if item.get("stock") is not None else None,
                "source": "api_dummyjson",
                "ts": datetime.utcnow().isoformat()
            }
            send(rec)
        p.flush()
        print(f"Sent {len(data)} products to {TOPIC}")
    except Exception as e:
        print("Error fetching API:", e)

if __name__ == "__main__":
    while True:
        fetch_and_send()
        time.sleep(INTERVAL)
