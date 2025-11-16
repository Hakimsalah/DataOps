import requests, json, time
from kafka import KafkaProducer
from datetime import datetime
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    """Crée et retourne un producer Kafka avec retry"""
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                batch_size=16384,
                linger_ms=10,
                retries=3
            )
            logger.info("Kafka producer créé avec succès")
            return producer
        except Exception as e:
            logger.warning(f"Tentative {attempt + 1}/{max_retries} échouée: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    raise Exception("Impossible de se connecter à Kafka après plusieurs tentatives")

def fetch_products():
    """Récupère les produits depuis l'API"""
    API_URL = "https://dummyjson.com/products?limit=10"
    
    try:
        resp = requests.get(API_URL, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("products", [])
        else:
            logger.error(f"API returned status code: {resp.status_code}")
            return []
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données: {e}")
        return []

def main():
    producer = create_producer()
    
    while True:
        try:
            products = fetch_products()
            
            if products:
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
                    logger.info(f"Sent product: {product.get('title')}")
                    time.sleep(0.5)  # Délai entre les messages
                
                producer.flush()
                logger.info(f"Batch de {len(products)} produits envoyé avec succès")
            else:
                logger.warning("Aucun produit récupéré")
            
            # Attendre avant le prochain batch
            logger.info("Attente de 30 secondes avant le prochain batch...")
            time.sleep(30)
            
        except KeyboardInterrupt:
            logger.info("Arrêt du producer...")
            break
        except Exception as e:
            logger.error(f"Erreur inattendue: {e}")
            time.sleep(30)  # Attendre avant de réessayer

if __name__ == "__main__":
    main()