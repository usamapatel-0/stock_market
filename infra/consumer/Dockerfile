FROM python:3.11-slim

WORKDIR /app

COPY consumer.py .

RUN pip install --no-cache-dir kafka-python boto3

CMD ["python", "consumer.py"]