# 1. Usamos una imagen base de Python ligera
FROM python:3.11-slim

# 2. Creamos una carpeta de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiamos los requisitos y los instalamos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiamos el resto del código (incluyendo la carpeta src y el .env)
COPY . .

# 5. Comando para ejecutar tu script (ajusta la ruta si es necesario)
CMD ["python", "src/fetch_solarman_data.py"]

