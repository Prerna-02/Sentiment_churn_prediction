FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app

# Default model path (override via docker-compose env)
ENV MODEL_PATH=/models/sentiment_model_3class.pkl
ENV MODEL_VERSION=hashing_sgd_3class_v1

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
