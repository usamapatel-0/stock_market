#Import Requirements
import os
import time
import json
import requests
from kafka import KafkaProducer

#Define Variables For API
API_KEY = os.environ.get("FINNHUB_API_KEY")

BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

#Initial Producer
producer = KafkaProducer(
    bootstrap_servers=["host.docker.internal:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

#Retrieve Data
def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())

        return data

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

#Looping And Pushing To Stream
while True:

    for symbol in SYMBOLS:

        print(f"Fetching {symbol}...")

        quote = fetch_quote(symbol)

        if quote:
            print(f"Producing: {quote}")

            try:
                future = producer.send(
                    "stock-quotes",
                    value=quote
                )

                # Kafka se confirmation
                future.get(timeout=10)

                print(f"✅ Sent to Kafka: {symbol}")

            except Exception as e:
                print(f"❌ Kafka error for {symbol}: {e}")

    print("😴 Sleeping 6 seconds...")
    time.sleep(6)



