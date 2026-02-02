FROM python:3.11-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY app /app/app

# Environment variables (can be overridden in docker-compose)
ENV MONGO_URI=mongodb://mongo:27017
ENV MONGO_DB=itd
ENV MONGO_COLLECTION=reviews_enriched

# Expose API port
EXPOSE 3001

# Start FastAPI server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3001"]


