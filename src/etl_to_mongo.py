import json
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
import os


MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongo:27017/"

clientMongo = MongoClient(MONGO_URI)
db = clientMongo["solar_data"]
collection = db["real_time_data"]

consumer = KafkaConsumer(
    'topic_solarman_data',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🛠️ ETL to MongoDB iniciado. Esperando mensajes de Kafka...")
for message in consumer:
    data = message.value
    print(f"📥 Mensaje recibido de Kafka: {data}")

    documento_mongo = {
        "requestId": data.get("requestId"),
        "timestamp": data.get("timestamp"),
        "stationid": data.get("stationId"),
        "usePower": data.get("usePower"),
        "batteryPower": data.get("batteryPower"),
        "batterySOC": data.get("batterySOC"),
        "generationPower": data.get("generationPower"),
        "generationTotal": data.get("generationTotal"),
        "name": data.get("name")
    }
    # Insertar datos en MongoDB
    result = collection.insert_one(documento_mongo)
    print(f"✅ Datos insertados en MongoDB con ID: {result.inserted_id}")

    # Esperar un breve momento antes de procesar el siguiente mensaje
    #time.sleep(2)