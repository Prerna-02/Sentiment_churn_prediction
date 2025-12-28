
FROM python:3.11-slim

WORKDIR /app

# Install deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY app /app/app

CMD ["python", "app/producer.py"]
