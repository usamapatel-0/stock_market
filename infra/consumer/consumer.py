# Import requirements
import json
import boto3
import time
from kafka import KafkaConsumer


# ==============================
# MinIO Connection
# ==============================

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    region_name="us-east-1"
)

bucket_name = "bronze-transactions"


# ==============================
# Create Bucket if it doesn't exist
# ==============================

try:
    s3.head_bucket(Bucket=bucket_name)
    print(
        f"Bucket already exists: {bucket_name}",
        flush=True
    )

except Exception:
    print(
        f"Bucket does not exist. Creating: {bucket_name}",
        flush=True
    )

    s3.create_bucket(
        Bucket=bucket_name
    )

    print(
        f"Bucket created successfully: {bucket_name}",
        flush=True
    )


# ==============================
# Define Kafka Consumer
# ==============================

consumer = KafkaConsumer(
    "stock-quotes",

    bootstrap_servers=[
        "host.docker.internal:29092"
    ],

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    group_id="bronze-consumer1",

    value_deserializer=lambda v: json.loads(
        v.decode("utf-8")
    )
)


print(
    "Consumer streaming and saving to MinIO...",
    flush=True
)


# ==============================
# Main Consumer Loop
# ==============================

for message in consumer:

    try:

        # Get Kafka message
        record = message.value

        # Get stock symbol
        symbol = record.get(
            "symbol",
            "unknown"
        )

        # Get timestamp
        ts = record.get(
            "fetched_at",
            int(time.time())
        )

        # MinIO object path
        key = f"{symbol}/{ts}.json"


        # Save JSON record to MinIO
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(record),
            ContentType="application/json"
        )


        print(
            f"Saved record for {symbol} = "
            f"s3://{bucket_name}/{key}",
            flush=True
        )


    except Exception as e:

        print(
            f"Error processing message: {e}",
            flush=True
        )