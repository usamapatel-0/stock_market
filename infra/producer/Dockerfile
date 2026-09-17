FROM python:3.11-slim

WORKDIR /app

COPY producer.py .

RUN pip install --no-cache-dir requests kafka-python

CMD ["python", "producer.py"]