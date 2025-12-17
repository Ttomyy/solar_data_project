import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Configuración desde variables de entorno (.env)
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# 2. Conexión a InfluxDB
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 3. Conexión a Kafka (Bucle de reintento simple)
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            'topic_solarman_data', 'topic_solarman_static_data',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='earliest', # Leemos desde el principio para rellenar gráficas
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='grupo_etl_influx'   # IMPORTANTE: Grupo distinto a Mongo para no robar mensajes
        )
        print("✅ ETL-Influx conectado a Kafka")
    except Exception as e:
        print(f"⚠️ Esperando a Kafka... {e}")
        time.sleep(5)

# 4. Memoria volátil para enriquecer datos (Nombre y Tipo)
memoria_estaciones = {}

print("🚀 ETL InfluxDB escuchando métricas...")

for message in consumer:
    try:
        data = message.value

        # A. Si es dato estático: Guardamos en memoria
        if message.topic == 'topic_solarman_static_data':
            id_st = data.get("stationId")
            if id_st:
                memoria_estaciones[id_st] = {
                    "nombre": data.get("name"),
                    "tipo": data.get("stationType")
                }
                print(f"ℹ️ Configuración recibida para estación: {id_st}")

        # B. Si es dato tiempo real: Escribimos en Influx
        elif message.topic == 'topic_solarman_data':
            id_actual = data.get("stationId")
            
            # Solo procesamos si hay datos de potencia válidos
            if data.get("generationPower") is not None:
                
                # Recuperamos info estática (si existe)
                info_estatica = memoria_estaciones.get(id_actual, {})

                # Gestión de fecha
                raw_time = data.get("lastUpdateTime")
                tiempo_dato = datetime.utcnow()
                if raw_time:
                    tiempo_dato = datetime.fromtimestamp(int(raw_time))

                # Crear Punto Influx
                point = Point("sistema_solar") \
                    .tag("station_id", str(id_actual)) \
                    .tag("nombre_hash", info_estatica.get("nombre", "desconocido")) \
                    .field("generationPower", float(data.get("generationPower", 0))) \
                    .field("usePower", float(data.get("usePower", 0))) \
                    .field("batterySOC", float(data.get("batterySOC", 0))) \
                    .time(tiempo_dato)

                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                print(f"⚡ Influx: Gen={data.get('generationPower')}W | Cons={data.get('usePower')}W")

    except Exception as e:
        print(f"❌ Error escribiendo en Influx: {e}")