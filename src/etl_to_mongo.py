import json
import time
from datetime import datetime  # 🔥 Importante para convertir fechas
from kafka import KafkaConsumer
from pymongo import MongoClient
import os

# Configuración de MongoDB
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password123")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongo:27017/"

# Conexión a la BBDD
clientMongo = MongoClient(MONGO_URI)
db = clientMongo["solar_data"]
collection = db["real_time_data"]

consumer = None

# Bucle de conexión resiliente
while consumer is None:
    try:
        consumer = KafkaConsumer(
            'topic_solarman_data', 'topic_solarman_static_data', # 🔥 CORRECCIÓN: Lista de topics
            bootstrap_servers='kafka:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='grupo_etl_mongo' # Recomendado: define un grupo
        )
        print("✅ ¡Conexión con Kafka exitosa!")
    except Exception as e:
        print(f"⚠️ Kafka no responde aún: {e}")
        print("🔁 Reintentando en 5 segundos...")
        time.sleep(5)

# 🔥 Memoria inicializada FUERA del bucle
memoria_static_data = {}

print("🚀 Iniciando el procesamiento de mensajes...")

for message in consumer:
    try:
        data = message.value
        # print(f"📥 Mensaje recibido del topic: {message.topic}") # Descomentar para debug

        # --- CASO 1: DATOS ESTÁTICOS (Guardar en memoria) ---
        if message.topic == 'topic_solarman_static_data':
            id_station = data.get("id")
            
            # Guardamos/Actualizamos la ficha de la estación
            memoria_static_data[id_station] = {
                "nombre": data.get("name"),
                "Localizacion": data.get("locationAddress"),
                "regionTimezone": data.get("regionTimezone"),
                "Tipo": data.get("type"),
                "EstatusRed": data.get("networkStatus")
            }
            print(f"💾 Memoria actualizada para estación ID: {id_station}")
            
        # --- CASO 2: DATOS EN TIEMPO REAL (Enriquecer y guardar en Mongo) ---
        elif message.topic == 'topic_solarman_data':
            id_actual = data.get("stationId")
            datos_estaticos = memoria_static_data.get(id_actual)
            
            # 🔥 MEJORA: Convertir timestamp Unix a Fecha Real de Mongo
            raw_time = data.get("lastUpdateTime") # 🔥 CORRECCIÓN: nombre del campo
            fecha_iso = None
            if raw_time:
                fecha_iso = datetime.fromtimestamp(int(raw_time))

            # Crear documento base
            documento_mongo = {
                "requestId": data.get("requestId"),
                "timestamp": fecha_iso, # Guardamos la fecha convertida
                "raw_timestamp": raw_time, # Guardamos el original por si acaso
                "stationid": id_actual,
                "usePower": data.get("usePower"),
                "batteryPower": data.get("batteryPower"),
                "batterySOC": data.get("batterySOC"),
                "generationPower": data.get("generationPower"),
                "generationTotal": data.get("generationTotal"),
            }

            # Enriquecimiento (JOIN)
            if datos_estaticos:
                documento_mongo.update({
                    "nombreEstacion": datos_estaticos.get("nombre"),
                    "Localizacion": datos_estaticos.get("Localizacion"),
                    "regionTimezone": datos_estaticos.get("regionTimezone"),
                    "TipoEstacion": datos_estaticos.get("Tipo"),
                    "EstatusRed": datos_estaticos.get("EstatusRed")
                })
            else:
                print(f"⚠️ Warning: No tengo datos estáticos para la estación {id_actual} (¿Llegaron ya?)")

            # Inserción en MongoDB
            result = collection.insert_one(documento_mongo)
            print(f"✅ Documento enriquecido guardado. ID: {result.inserted_id}")

    except Exception as e:
        print(f"❌ Error procesando un mensaje: {e}")
        # El bucle continuará con el siguiente mensaje gracias al try/except