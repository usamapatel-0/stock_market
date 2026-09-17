# Real-Time Stock Market Data Pipeline

A complete near-real-time stock market data engineering project built
with Docker, Python, Kafka, MinIO, Apache Airflow, Snowflake, dbt, and
Power BI.

> **Architecture:** Finnhub API → Python Producer → Kafka → Python
> Consumer → MinIO → Airflow → Snowflake → dbt → Power BI

## Architecture

![Real-Time Stock Market Data Pipeline
Architecture](docs/architecture.png)

### Add this image

Save the architecture diagram shown in this project as:

`docs/architecture.png`

------------------------------------------------------------------------

## Project Overview

This project demonstrates an end-to-end data engineering pipeline for
stock quote data.

The pipeline:

1.  Fetches stock quotes from the Finnhub API.
2.  Publishes quote events to Kafka.
3.  Consumes Kafka messages with Python.
4.  Stores raw JSON records in a MinIO S3-compatible bucket.
5.  Uses Apache Airflow to orchestrate MinIO → Snowflake ingestion.
6.  Stores raw JSON in Snowflake using `VARIANT`.
7.  Uses dbt to build Bronze, Silver, Fact/Dimension, and Gold models.
8.  Connects the business-ready Gold models to Power BI.
9.  Visualizes stock prices, volatility, change percentage, tree chart,
    and candlestick data.

------------------------------------------------------------------------

## Tech Stack

  Technology                Purpose
  ------------------------- --------------------------------------------
  Python                    API ingestion, Kafka producer and consumer
  Docker / Docker Compose   Containerized infrastructure
  Apache Kafka              Data streaming
  Kafdrop                   Kafka topic monitoring
  MinIO                     S3-compatible object storage
  Apache Airflow            Workflow orchestration
  PostgreSQL                Airflow metadata database
  Snowflake                 Cloud data warehouse
  dbt                       SQL transformation and data modeling
  Power BI                  Business intelligence and visualization
  Finnhub API               Stock quote source

------------------------------------------------------------------------

## End-to-End Architecture

``` text
Finnhub API
    ↓
Python Producer
    ↓
Kafka
    ↓
Python Consumer
    ↓
MinIO / S3 Bucket
    ↓
Apache Airflow
    ↓
Snowflake
    ↓
dbt
    ↓
Power BI
```

### Data layers

``` text
API / Streaming
      ↓
Bronze / Raw
      ↓
Silver / Cleaned
      ↓
Fact + Dimensions
      ↓
Gold / Business Ready
      ↓
Power BI
```

------------------------------------------------------------------------

## 1. Finnhub API --- Data Source

The Python producer periodically requests stock quotes from the Finnhub
API.

Configured symbols include:

``` text
AAPL
MSFT
TSLA
GOOGL
AMZN
```

The quote payload contains fields such as:

-   Current price
-   Change amount
-   Change percentage
-   Day high
-   Day low
-   Day open
-   Previous close
-   Symbol
-   Fetch timestamp

### Image to add

Optional screenshot of the API response:

`docs/finnhub-api-response.png`

------------------------------------------------------------------------

## 2. Kafka --- Streaming Layer

Kafka receives the stock quote events produced by Python.

Kafka topic:

``` text
stock-quotes
```

Logical flow:

``` text
Finnhub API
    ↓
Python Producer
    ↓
Kafka
    └── stock-quotes
            ↓
       Python Consumer
```

Kafka provides the streaming and decoupling layer between ingestion and
downstream storage.

### Image to add

Take a Kafdrop screenshot showing the `stock-quotes` topic and messages.

Save as:

`docs/kafdrop-topic.png`

Kafdrop runs locally at:

`http://localhost:9000`

------------------------------------------------------------------------

## 3. MinIO --- Raw Data Storage

The Python Kafka consumer writes the received quote messages as JSON
objects into MinIO.

Bucket:

``` text
bronze-transactions
```

The bucket represents the Bronze / Raw data layer.

Logical structure:

``` text
bronze-transactions/
├── AAPL/
├── AMZN/
├── GOOGL/
├── MSFT/
└── TSLA/
```

MinIO provides an S3-compatible object-storage layer that can be run
locally with Docker.

### Image to add

Take a MinIO console screenshot showing the `bronze-transactions` bucket
and stored JSON objects.

Save as:

`docs/minio-bucket.png`

MinIO console:

`http://localhost:9001`

------------------------------------------------------------------------

## 4. Apache Airflow --- Orchestration

Airflow orchestrates the ingestion from MinIO to Snowflake.

Main DAG:

``` text
minio_to_snowflake
```

Main workflow:

``` text
download_minio
      ↓
load_to_snowflake
```

The DAG:

1.  Connects to MinIO.
2.  Lists objects from the Bronze bucket.
3.  Downloads JSON files.
4.  Connects to Snowflake.
5.  Uploads files to a Snowflake stage.
6.  Runs `COPY INTO`.
7.  Loads the JSON records into Snowflake.

### Image to add

Take a screenshot of the Airflow DAG with successful task runs.

Save as:

`docs/airflow-dag.png`

Airflow UI:

`http://localhost:8080`

------------------------------------------------------------------------

## 5. Snowflake --- Data Warehouse

Snowflake is the cloud data warehouse used for analytical storage.

Database:

``` text
STOCK_MDS
```

Schema:

``` text
COMMON
```

Raw table:

``` text
BRONZE_STOCK_QUOTES_RAW
```

The raw JSON is stored using Snowflake's `VARIANT` type.

Example:

``` sql
CREATE TABLE bronze_stock_quotes_raw (
    v VARIANT
);
```

Example validation query:

``` sql
SELECT *
FROM STOCK_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW
LIMIT 10;
```

### Image to add

Take a Snowflake screenshot showing the raw table and sample JSON
records.

Save as:

`docs/snowflake-raw.png`

------------------------------------------------------------------------

## 6. dbt --- Transformation Layer

dbt transforms the raw Snowflake data into analytics-ready models.

Model flow:

``` text
Bronze
  ↓
Silver
  ↓
Fact + Dimensions
  ↓
Gold
```

### Bronze

``` text
bronze_stg_stock_quotes
```

Extracts fields from the raw JSON payload.

### Silver

``` text
silver_clean_stock_quotes
```

Cleans and standardizes stock quote fields.

Important fields include:

``` text
symbol
current_price
change_amount
change_percent
day_high
day_low
day_open
prev_close
market_timestamp
fetched_at
```

------------------------------------------------------------------------

## Fact and Dimension Models

### Dimension: Stock

``` text
dim_stock
```

Contains distinct stock symbols.

### Dimension: Date

``` text
dim_date
```

Contains date attributes such as:

``` text
trade_date
year
month
day
```

### Fact: Stock Quotes

``` text
fact_stock_quotes
```

Contains quote measurements such as:

``` text
symbol
trade_date
current_price
change_amount
change_percent
day_high
day_low
day_open
prev_close
market_timestamp
fetched_at
```

------------------------------------------------------------------------

## Gold Models

### Gold KPI

``` text
gold_kpi
```

Provides the latest quote for each stock and supports KPI/card visuals.

### Gold Candlestick

``` text
gold_candlestick
```

Creates OHLC candle data.

Each daily candle contains:

``` text
Open
High
Low
Close
```

The model also calculates a trend line.

The current implementation uses:

``` text
trade_date = CAST(market_timestamp AS DATE)
```

so the candles are **daily candles**.

The current model keeps the latest 12 candles per symbol.

### Gold Tree Chart

``` text
gold_treechart
```

Provides:

-   Average price
-   Volatility
-   Relative volatility

for Power BI comparison visuals.

------------------------------------------------------------------------

## 7. Power BI --- Visualization

Power BI is connected to the Snowflake/dbt business-ready datasets.

The dashboard includes:

-   Stock price KPI cards
-   Volatility visuals
-   Tree chart
-   Change percentage chart
-   Candlestick chart
-   Stock symbol filtering

Recommended datasets:

``` text
GOLD_KPI
GOLD_CANDLESTICK
GOLD_TREECHART
FACT_STOCK_QUOTES
DIM_STOCK
DIM_DATE
```

### Main dashboard image

Take a screenshot of the completed Power BI dashboard and save it as:

`docs/power-bi-dashboard.png`

Then this README can display it with:

``` markdown
![Power BI Dashboard](docs/power-bi-dashboard.png)
```

------------------------------------------------------------------------

## Project Structure

``` text
Real-Time-Stocks-MDS/
│
├── infra/
│   ├── docker-compose.yml
│   │
│   ├── producer/
│   │   ├── producer.py
│   │   └── Dockerfile
│   │
│   ├── consumer/
│   │   ├── consumer.py
│   │   └── Dockerfile
│   │
│   └── dags/
│       └── minio_to_snowflake.py
│
├── dbt_stocks/
│   └── dbt_stocks/
│       ├── dbt_project.yml
│       └── models/
│           ├── bronze/
│           │   ├── bronze_stg_stock_quotes.sql
│           │   └── sources.yml
│           ├── silver/
│           │   └── silver_clean_stock_quotes.sql
│           └── gold/
│               ├── dim_date.sql
│               ├── dim_stock.sql
│               ├── fact_stock_quotes.sql
│               ├── gold_candlestick.sql
│               ├── gold_kpi.sql
│               └── gold_treechart.sql
│
├── docs/
│   ├── architecture.png
│   ├── finnhub-api-response.png
│   ├── kafdrop-topic.png
│   ├── minio-bucket.png
│   ├── airflow-dag.png
│   ├── snowflake-raw.png
│   └── power-bi-dashboard.png
│
└── README.md
```

------------------------------------------------------------------------

## Prerequisites

Install:

-   Docker Desktop
-   Python 3.11+
-   Git
-   Power BI Desktop
-   Snowflake account
-   Finnhub API key

For local dbt development:

-   dbt Core
-   Snowflake dbt adapter

------------------------------------------------------------------------

## Configuration and Secrets

Do not commit credentials to GitHub.

Use environment variables or a secrets manager for:

``` text
FINNHUB_API_KEY
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
```

For local development, MinIO credentials are configured through Docker
Compose.

Recommended `.gitignore`:

``` gitignore
.env
*.env
venv/
.venv/
__pycache__/
*.pyc
logs/
target/
.DS_Store
```

If a real API key or password has ever been committed publicly, rotate
it immediately.

------------------------------------------------------------------------

## Running the Project

### 1. Clone the repository

``` bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd Real-Time-Stocks-MDS
```

### 2. Start Docker services

``` powershell
cd infra
docker compose up -d
```

Check:

``` powershell
docker ps
```

### 3. Verify Kafka

Open Kafdrop:

``` text
http://localhost:9000
```

Verify:

``` text
stock-quotes
```

### 4. Verify MinIO

Open:

``` text
http://localhost:9001
```

Verify:

``` text
bronze-transactions
```

### 5. Run Airflow

Open:

``` text
http://localhost:8080
```

Trigger:

``` text
minio_to_snowflake
```

Verify:

``` text
download_minio → load_to_snowflake
```

### 6. Verify Snowflake

``` sql
SELECT COUNT(*)
FROM STOCK_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW;
```

### 7. Run dbt

From the dbt project directory:

``` powershell
cd dbt_stocks\dbt_stocks
```

Test connection:

``` powershell
dbt debug
```

Run models:

``` powershell
dbt run
```

### 8. Refresh Power BI

After the warehouse/dbt data is updated:

``` text
Snowflake
   ↓
dbt run
   ↓
Power BI Refresh
```

------------------------------------------------------------------------

## Docker Services

The Docker Compose environment contains:

``` text
zookeeper
kafka
producer
consumer
kafdrop
minio
airflow-webserver
airflow-scheduler
postgres
```

Useful commands:

``` powershell
docker ps
docker logs stock-producer
docker logs stock-consumer
docker logs airflow-scheduler
```

------------------------------------------------------------------------

## Validation Checklist

Use these checks to validate the pipeline:

``` text
[ ] Finnhub API returns quote data
[ ] Producer container is running
[ ] Kafka stock-quotes topic receives messages
[ ] Consumer reads Kafka messages
[ ] MinIO receives JSON objects
[ ] Airflow DAG succeeds
[ ] Snowflake raw table contains records
[ ] dbt debug succeeds
[ ] dbt run succeeds
[ ] Gold models contain data
[ ] Power BI refresh succeeds
```

------------------------------------------------------------------------

## Data Engineering Concepts Demonstrated

This project demonstrates:

-   Real-time / near-real-time ingestion
-   Kafka streaming
-   Producer-consumer architecture
-   Object storage
-   Data lake Bronze layer
-   Workflow orchestration
-   Cloud data warehousing
-   Semi-structured JSON handling
-   ELT
-   dbt transformations
-   Fact and dimension modeling
-   Business-ready Gold datasets
-   BI dashboarding
-   Docker containerization
-   Pipeline validation and monitoring

------------------------------------------------------------------------

## Current Implementation Notes

This is a portfolio/learning implementation.

The producer uses periodic API polling rather than an exchange-grade
streaming market-data feed.

The current MinIO → Snowflake Airflow flow processes objects available
in the bucket when the DAG runs. A production implementation could
improve this with:

-   Incremental ingestion
-   Watermarks
-   Control/metadata tables
-   Idempotent loading
-   Better duplicate handling
-   Unique object keys
-   Schema validation
-   Data quality tests
-   Secret management
-   Monitoring and alerting

------------------------------------------------------------------------

## Future Improvements

-   [ ] Incremental MinIO → Snowflake ingestion
-   [ ] dbt tests
-   [ ] dbt documentation
-   [ ] Source freshness checks
-   [ ] Kafka Schema Registry
-   [ ] Data quality validation
-   [ ] Airflow failure alerts
-   [ ] Proper secret management
-   [ ] Automated Power BI refresh
-   [ ] GitHub Actions CI/CD
-   [ ] Producer/consumer unit tests
-   [ ] Better duplicate/idempotency handling
-   [ ] Historical stock data
-   [ ] Additional financial metrics
-   [ ] Observability and monitoring

------------------------------------------------------------------------

## Portfolio Highlights

This project demonstrates a complete data journey:

``` text
External API
     ↓
Streaming
     ↓
Raw Object Storage
     ↓
Orchestration
     ↓
Cloud Data Warehouse
     ↓
Transformation
     ↓
Dimensional / Business Modeling
     ↓
BI Visualization
```

It combines both **streaming concepts** and **modern ELT/data warehouse
practices** in one end-to-end project.

------------------------------------------------------------------------

## Author

**Usama**

Data Engineering Portfolio Project
