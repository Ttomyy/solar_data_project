import os
import json
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
from databricks import sql

def run_daily_etl():
    # 1. CONEXIÓN A MONGO (Tu fuente local)
    client = MongoClient(f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@mongo:27017/")
    db = client["solar_data"]
    col = db["real_time_data"]

    # Definir "Ayer"
    ayer = datetime.now() - timedelta(days=1)
    inicio = ayer.replace(hour=0, minute=0, second=0, microsecond=0)
    fin = ayer.replace(hour=23, minute=59, second=59, microsecond=999)

    # EXTRAER
    query = {"timestamp": {"$gte": inicio, "$lt": fin}}
    datos_crudos = list(col.find(query))
    
    if not datos_crudos:
        return {"status": "no_data", "message": "No hay datos de ayer"}

    # 2. TRANSFORMACIÓN BÁSICA (Capa Bronze -> Silver ligera)
    df = pd.DataFrame(datos_crudos)
    
# LIMPIEZA: Rellenamos los valores vacíos (NaN) con 0 para que SQL no falle
    df = df.fillna(0)

    # Calculamos el resumen diario asegurando que sean tipos nativos de Python (float)
    resumen_diario = {
        "fecha": inicio.strftime('%Y-%m-%d'),
        "energia_generada_total": float(df['generationPower'].sum() / 60 / 1000),
        "consumo_total": float(df['usePower'].sum() / 60 / 1000),
        "max_soc_bateria": float(df['batterySOC'].max()),
        "estacion_id": str(df['stationid'].iloc[0])
    }

    # 3. CARGA A DATABRICKS (Azure Cloud)
    # Estos valores los configuraremos en n8n o .env
    try:
        with sql.connect(server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
                         http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                         access_token=os.getenv("DATABRICKS_TOKEN")) as connection:
            
            with connection.cursor() as cursor:
                # Insertamos en una tabla llamada 'solar_bronze'
                query_sql = f"""
                    INSERT INTO default.solar_bronze 
                    (fecha, energia_generada_total, consumo_total, max_soc_bateria, estacion_id) 
                    VALUES 
                    ('{resumen_diario['fecha']}', 
                      {resumen_diario['energia_generada_total']}, 
                      {resumen_diario['consumo_total']}, 
                      {resumen_diario['max_soc_bateria']}, 
                     '{resumen_diario['estacion_id']}')
                """

                cursor.execute(query_sql)
        
        return {"status": "success", "data_sent": resumen_diario}
    
    except Exception as e:
        return {"status": "error", "error_message": str(e)}

if __name__ == "__main__":
    result = run_daily_etl()
    print(json.dumps(result))