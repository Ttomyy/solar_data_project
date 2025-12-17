import requests
import json
import os
import hashlib
from dotenv import load_dotenv

load_dotenv()
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 0, 2))
# --- CONFIG ---
APP_ID = os.getenv("CLIENT_ID_SOLARMAN")
APP_SECRET = os.getenv("CLIENT_SECRET_SOLARMAN")
USER_EMAIL = os.getenv("EMAIL_SOLARMAN")       # Nuevo
USER_PASSWORD = os.getenv("PASSWORD_SOLARMAN") # Nuevo

# URL Oficial (Global)
TOKEN_URL = "https://globalapi.solarmanpv.com/account/v1.0/token"
# URL De estacioesn
STATION_URL = "https://globalapi.solarmanpv.com/station/v1.0/list"



def get_access_token():
    print(f"📡 1. Preparando autenticación...")
    
    if not all([APP_ID, APP_SECRET, USER_EMAIL, USER_PASSWORD]):
        print("❌ ERROR: Faltan variables en el .env (Asegúrate de tener EMAIL y PASSWORD)")
        return None

    # --- PASO CRÍTICO: Encriptar password (SHA256) ---
    # Solarman NO acepta la contraseña en texto plano
    password_hash = hashlib.sha256(USER_PASSWORD.encode()).hexdigest()

    # --- ESTRUCTURA EXACTA SEGÚN DOCUMENTACIÓN 2.1 ---
    # 1. appId va en los parámetros de la URL (Query String)
    params = {
        'appId': APP_ID,
        'language': 'en'
    }
    
    # 2. El resto va en el cuerpo JSON
    payload = {
        'appSecret': APP_SECRET,
        'email': USER_EMAIL,
        'password': password_hash,
        'orgId': "" # Opcional, dejar vacío para usuarios normales
    }
    
    headers = {'Content-Type': 'application/json'}
    
    print(f"📡 2. Enviando petición a: {TOKEN_URL}...")
    
    try:
        response = requests.post(TOKEN_URL, params=params, json=payload, headers=headers)
        
        print(f"📥 3. Estado HTTP: {response.status_code}")
        
        # Si falla, mostramos el error crudo
        if response.status_code != 200:
            print(f"📦 Respuesta de Error: {response.text}")
        
        response.raise_for_status()
        
        # Solarman suele devolver el token así:
        # { "access_token": "...", "success": true }
        json_data = response.json()
        token = json_data.get('access_token')
        
        return token

    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return None
    
def get_station_list(token):
    print(f"📡 4. Buscando estaciones...")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Este endpoint suele funcionar con POST y un body vacío o filtro básico
    payload = {
        'language': 'en',
        'page': 1,
        'size': 10
    }

    try:
        response = requests.post(STATION_URL, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()
        return data

    except Exception as e:
        print(f"❌ Error al buscar estaciones: {e}")
        return None

def get_real_time_data(token, station_id):

    print(f"📡 4. Buscando datos del domicilio...")

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    payload = {
        'stationId': station_id,
        'language': 'en'
    }
    # URL del endpoint de datos en tiempo real
    REAL_TIME_URL = "https://globalapi.solarmanpv.com/station/v1.0/realTime"

    try:
        # ¿Cómo completarías esta línea?
        response = requests.post( REAL_TIME_URL, headers=headers, json=payload) 
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:  
        print(f"❌ Error al buscar datos en tiempo real: {e}")
        # ... resto del manejo de respuesta ...
        
#******************************************************************************
#***********************encriptacion de datos SHA256***********************
def hash_sensitive_data(station_data):
    """
    Crea una copia de los datos y encripta los campos sensibles.
    """
    # 1. Hacemos una copia para no modificar el diccionario original por accidente
    secure_data = station_data.copy()
    
    # 2. Lista de campos que queremos proteger
    sensitive_keys = ['name', 'locationAddress']
    
    for key in sensitive_keys:
        # Solo encriptamos si la clave existe en el diccionario
        if key in secure_data:
            valor_original = str(secure_data[key])
            # Convertimos a hash SHA-256 (irreversible)
            hash_object = hashlib.sha256(valor_original.encode())
            secure_data[key] = hash_object.hexdigest()
            
    return secure_data


#********************************************************************************

    
if __name__ == "__main__":
    while True:
        token = get_access_token()
        
        if token:
            print(f"✅ Token obtenido. Longitud: {len(token)}")

            # Nuevo paso:
            stations_data = get_station_list(token)

            if stations_data:
             # Suponiendo que stations_data tiene una lista 'stationList'
             # Procesamos la primera estación (o podrías hacer un bucle si tienes varias)
                original_station = stations_data['stationList'][0]
             
             # 🔒 Aquí ocurre la magia: Creamos la versión segura
                secure_station = hash_sensitive_data(original_station)

                print("\n🏠 Estación encontrada (Datos Protegidos):")
             # Imprimimos la versión segura (verás los hash en vez de tu dirección)
                print(json.dumps(secure_station, indent=2)) 
             
             # Enviamos la versión segura al nuevo topic estático
                producer.send('topic_solarman_static_data', secure_station) 
                producer.flush()
                print("✅ Estaciones enviadas a Kafka correctamente.")
            else:
                print("⚠️ No se pudieron descargar las estaciones.")
            
            # Obtenemos el ID que ya conocemos de la lista
            el_id_de_mi_estacion = stations_data['stationList'][0]['id']
                
            data_real_time = get_real_time_data(token, stations_data['stationList'][0]['id'])
            if data_real_time:
                # Inyectamos el ID dentro de los datos de tiempo real
                # Así el ETL sabrá de quién son estos datos
                data_real_time['stationId'] = el_id_de_mi_estacion
                
                print("\n📊 Datos en tiempo real:")
                print(json.dumps(data_real_time, indent=2))
                producer.send('topic_solarman_data', data_real_time)
                producer.flush()
                print("✅ Datos enviados a Kafka correctamente.")
            else:
                print("⚠️ No se pudieron descargar los datos en tiempo real.")
        else:
            print("❌ Fallo en la autenticación.")
    
        print("⏱️ Esperando 4 minutos para la siguiente consulta...\n")
        import time
        time.sleep(180)  # Esperar 10 minutos antes de la siguiente consulta