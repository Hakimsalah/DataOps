import requests, json, time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

API_URL = "https://dummyjson.com/products?limit=5"  # limite à 5 produits

# Envoi d'un seul batch
resp = requests.get(API_URL)
if resp.status_code == 200:
    products = resp.json().get("products", [])
    for product in products:
        data = {
            "product_id": product.get("id"),
            "product_name": product.get("title"),
            "price": product.get("price"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "rating": product.get("rating"),
            "stock": product.get("stock"),
            "source": "dummyjson",
            "ts": datetime.utcnow().isoformat()
        }
        producer.send("realtime-products", data)
        print(f"Sent: {data}")
        time.sleep(0.2)  # petit délai pour ne pas saturer Kafka
    producer.flush()
    print("Batch sent successfully!")
else:
    print(f"Failed to fetch data: {resp.status_code}")
