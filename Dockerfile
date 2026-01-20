FROM python:3.13-slim

WORKDIR /app

# Copier requirements
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY api/ ./api/

# Exposer le port
EXPOSE 8000

# Variable d'environnement par défaut
ENV API_PORT=8000
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9093

# Commande de démarrage
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]