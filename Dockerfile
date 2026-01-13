FROM python:3.11-slim

WORKDIR /app

# Instalamos dependencias primero para aprovechar la caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código
COPY . .

# El comando de ejecución (Compose puede sobrescribir esto si es necesario)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3000"]