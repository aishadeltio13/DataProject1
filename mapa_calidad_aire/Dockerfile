FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar codigo y assets
COPY mapa_londres.py .
COPY assets/ ./assets/

# Puerto de Dash
EXPOSE 8050

# Ejecutar aplicacion
CMD ["python", "mapa_londres.py"]

# http://localhost:8050

