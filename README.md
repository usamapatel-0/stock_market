# 📈 Real-Time Stock Market Data Engineering Pipeline

> End-to-end real-time stock market data engineering project using Python, Kafka, MinIO, Apache Airflow, Snowflake, dbt and Power BI.

## 📌 Table of Contents

- [Project Overview](#-project-overview)
- [Business Problem](#-business-problem)
- [Project Objectives](#-project-objectives)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Project Structure](#-project-structure)
- [Data Flow](#-data-flow)
- [Finnhub API](#1-finnhub-api)
- [Kafka Streaming](#2-kafka-streaming)
- [MinIO Bronze Storage](#3-minio-bronze-storage)
- [Apache Airflow](#4-apache-airflow)
- [Snowflake](#5-snowflake)
- [dbt Transformation](#6-dbt-transformation)
- [Power BI](#7-power-bi)
- [Automatic dbt Execution](#-automatic-dbt-execution)
- [Docker Setup](#-docker-setup)
- [Prerequisites](#-prerequisites)
- [Configuration](#-configuration)
- [How to Run](#-how-to-run)
- [How to Verify](#-how-to-verify-the-pipeline)
- [dbt Commands](#-dbt-commands)
- [Troubleshooting](#-troubleshooting)
- [Security](#-security)
- [Future Improvements](#-future-improvements)
- [Key Learning Outcomes](#-key-learning-outcomes)
- [Resume Description](#-resume-description)
- [Author](#-author)

---

## 🚀 Project Overview

This project implements a complete stock market data engineering pipeline.

Stock quote data is collected from the **Finnhub API**, published to **Apache Kafka**, consumed by a Python service and stored as raw JSON objects in **MinIO**.

**Apache Airflow** orchestrates the downstream workflow:

```text
MinIO
  ↓
Snowflake
  ↓
dbt
  ↓
Power BI
```

The dbt transformation is integrated directly into the Airflow DAG. After the Snowflake load succeeds, Airflow automatically executes:

```bash
dbt build
```

The transformed Gold-layer data is then consumed by **Power BI** for analytics and visualization.

---

## 🎯 Business Problem

Stock market data changes continuously. An analytics system therefore needs to:

- Collect stock quote data repeatedly.
- Stream incoming records.
- Preserve raw data.
- Load data into an analytical warehouse.
- Transform raw data into clean analytical models.
- Automate the workflow.
- Provide a dashboard for analysis.

This project demonstrates how these requirements can be implemented using a modern data engineering stack.

---

## 🎯 Project Objectives

1. Build a real-time stock data ingestion pipeline.
2. Stream stock quote records through Kafka.
3. Store raw JSON data in MinIO.
4. Orchestrate the pipeline with Apache Airflow.
5. Load raw data into Snowflake.
6. Build Bronze, Silver and Gold transformation layers with dbt.
7. Automatically execute dbt from Airflow.
8. Connect Power BI to the analytical layer.
9. Containerize the infrastructure with Docker Compose.

---

# 🏗️ Architecture

```text
                    ┌─────────────────┐
                    │   Finnhub API   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Python Producer │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Kafka      │
                    │  stock-quotes   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Python Consumer │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     MinIO       │
                    │ Bronze Storage  │
                    └────────┬────────┘
                             │
                             ▼
                 ┌────────────────────────┐
                 │    Apache Airflow      │
                 │    DAG Orchestration   │
                 └───────────┬────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Snowflake    │
                    │   Raw/Bronze    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      dbt        │
                    │ Silver → Gold   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Power BI     │
                    │    Dashboard    │
                    └─────────────────┘
```

---

# 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| **Python** | API ingestion and Kafka/MinIO processing |
| **Finnhub API** | Stock quote source |
| **Apache Kafka** | Real-time streaming/message broker |
| **Zookeeper** | Kafka coordination for the configured Kafka version |
| **Kafdrop** | Kafka topic/message monitoring |
| **MinIO** | S3-compatible raw object storage |
| **Apache Airflow** | Workflow orchestration |
| **PostgreSQL** | Airflow metadata database |
| **Snowflake** | Cloud data warehouse |
| **dbt** | SQL transformations and data modeling |
| **Power BI** | Dashboard and visualization |
| **Docker** | Containerization |
| **Docker Compose** | Multi-service infrastructure |
| **Git/GitHub** | Version control |

---

# 📂 Project Structure

```text
Real-Time-Stocks-MDS/
│
├── dbt_stocks/
│   └── dbt_stocks/
│       ├── analyses/
│       ├── logs/
│       ├── macros/
│       ├── models/
│       │   ├── bronze/
│       │   │   ├── bronze_stg_stock_quotes.sql
│       │   │   └── sources.yml
│       │   ├── silver/
│       │   │   └── silver_clean_stock_quotes.sql
│       │   └── gold/
│       │       ├── dim_date.sql
│       │       ├── dim_stock.sql
│       │       ├── fact_stock_quotes.sql
│       │       ├── gold_candlestick.sql
│       │       ├── gold_kpi.sql
│       │       └── gold_treechart.sql
│       ├── seeds/
│       ├── snapshots/
│       ├── tests/
│       └── dbt_project.yml
│
├── infra/
│   ├── airflow/
│   │   └── Dockerfile
│   ├── consumer/
│   │   ├── consumer.py
│   │   └── Dockerfile
│   ├── dags/
│   │   └── minio_to_snowflake.py
│   ├── producer/
│   │   ├── producer.py
│   │   └── Dockerfile
│   └── docker-compose.yml
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
├── requirements.txt
├── STOCK.pbix
├── .gitignore
└── README.md
```

---

# 🔄 Data Flow

```text
Finnhub API
    ↓
Python Producer
    ↓
Kafka Topic: stock-quotes
    ↓
Python Consumer
    ↓
MinIO Bucket: bronze-transactions
    ↓
Airflow DAG
    ↓
Snowflake Raw Table
    ↓
dbt Bronze
    ↓
dbt Silver
    ↓
dbt Gold
    ↓
Power BI
```

---

# 1. 📡 Finnhub API

The Python producer retrieves stock quote data from the Finnhub quote API.

Configured symbols:

```text
AAPL
MSFT
TSLA
GOOGL
AMZN
```

The producer adds metadata such as:

```text
symbol
fetched_at
```

and publishes the JSON record to Kafka.

### Logical record structure

```json
{
  "c": 150.25,
  "d": 1.25,
  "dp": 0.84,
  "h": 151.10,
  "l": 148.90,
  "o": 149.50,
  "pc": 149.00,
  "t": 1726560000,
  "symbol": "AAPL",
  "fetched_at": 1726560000
}
```

Values vary because the source is live market data.

---

# 2. 📨 Kafka Streaming

Kafka is the streaming layer between the producer and consumer.

Topic:

```text
stock-quotes
```

Flow:

```text
Finnhub
   ↓
Python Producer
   ↓
Kafka: stock-quotes
   ↓
Python Consumer
```

Kafdrop is included to inspect topics and messages.

Open:

```text
http://localhost:9000
```

---

# 3. 🪣 MinIO Bronze Storage

The Kafka consumer reads messages and stores them as JSON objects in MinIO.

Bucket:

```text
bronze-transactions
```

Logical structure:

```text
bronze-transactions/
├── AAPL/
├── MSFT/
├── TSLA/
├── GOOGL/
└── AMZN/
```

MinIO acts as the raw/bronze object-storage layer.

MinIO Console:

```text
http://localhost:9001
```

---

# 4. ⚙️ Apache Airflow

Airflow orchestrates the downstream pipeline.

DAG:

```text
minio_to_snowflake
```

Task dependency:

```text
download_minio
      ↓
load_snowflake
      ↓
run_dbt
```

### Task 1 — `download_minio`

A `PythonOperator`:

- Connects to MinIO.
- Reads objects from the bronze bucket.
- Downloads JSON files.
- Passes the downloaded file information to the next task.

### Task 2 — `load_snowflake`

A `PythonOperator`:

- Connects to Snowflake.
- Uploads the downloaded files.
- Executes `COPY INTO`.
- Loads JSON data into the raw Snowflake table.

Target:

```text
STOCK_MDS.COMMON.BRONZE_STOCK_QUOTES_RAW
```

### Task 3 — `run_dbt`

A `BashOperator` executes:

```bash
cd /opt/airflow/dbt_stocks
dbt build
```

Dependency:

```python
task1 >> task2 >> task3
```

Therefore:

```text
download_minio
      ↓
load_snowflake
      ↓
run_dbt
```

---

# ⏱️ Automatic dbt Execution

The key point is that **dbt is not a separate manual step anymore**.

For example, if the DAG schedule is:

```python
schedule_interval="* * * * *"
```

the DAG is scheduled every minute.

For each run:

```text
Airflow starts
      ↓
download_minio
      ↓
load_snowflake
      ↓
Snowflake load succeeds
      ↓
run_dbt
      ↓
dbt build
```

Airflow itself executes the command:

```bash
dbt build
```

So the user does not need to manually run dbt after every Snowflake load.

### How to confirm

In the Airflow DAG graph:

```text
download_minio → load_snowflake → run_dbt
```

If all three tasks show:

```text
success
```

the complete DAG run succeeded.

---

# 5. ❄️ Snowflake

Snowflake is the cloud data warehouse.

Configured database:

```text
STOCK_MDS
```

Configured schema:

```text
COMMON
```

Raw table:

```text
BRONZE_STOCK_QUOTES_RAW
```

Snowflake receives the raw JSON data before dbt transformations.

---

# 6. 🧱 dbt Transformation

The dbt project follows:

```text
Raw
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

## Bronze Layer

```text
models/bronze/bronze_stg_stock_quotes.sql
models/bronze/sources.yml
```

The Bronze layer defines the raw source/staging layer.

## Silver Layer

```text
models/silver/silver_clean_stock_quotes.sql
```

The Silver layer cleans and prepares stock quote data.

## Gold Layer

```text
dim_date.sql
dim_stock.sql
fact_stock_quotes.sql
gold_candlestick.sql
gold_kpi.sql
gold_treechart.sql
```

### Gold models

| Model | Purpose |
|---|---|
| `dim_date` | Date dimension |
| `dim_stock` | Distinct stock/symbol dimension |
| `fact_stock_quotes` | Main stock quote fact dataset |
| `gold_kpi` | Latest stock KPI information |
| `gold_candlestick` | Daily candlestick-style analytics |
| `gold_treechart` | Price and volatility-related analytics |

---

# 📊 dbt Model Flow

```text
Snowflake Raw
      │
      ▼
bronze_stg_stock_quotes
      │
      ▼
silver_clean_stock_quotes
      │
      ├──────────────┬───────────────┐
      ▼              ▼               ▼
dim_stock        dim_date     fact_stock_quotes
      │
      ├──────────────┬───────────────┐
      ▼              ▼               ▼
 gold_kpi    gold_candlestick  gold_treechart
                    │
                    ▼
                 Power BI
```

---

# 7. 📊 Power BI

The project includes:

```text
STOCK.pbix
```

Power BI connects to Snowflake using **DirectQuery**.

The dashboard consumes analytical data from the Gold layer.

The dashboard can be used for:

- Current stock price analysis
- Price change analysis
- Percentage change analysis
- Stock-level KPIs
- Candlestick-style analysis
- Trend analysis
- Volatility-related analysis

---

# 🐳 Docker Setup

The project uses Docker Compose to run the infrastructure.

Main services:

```text
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

## Local Interfaces

| Service | URL |
|---|---|
| Airflow | `http://localhost:8080` |
| Kafdrop | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |

---

# 📋 Prerequisites

Install/configure:

- Docker Desktop
- Docker Compose
- Python 3.x
- Git
- Finnhub API key
- Snowflake account
- Power BI Desktop

---

# ⚙️ Configuration

## Finnhub

The producer requires a Finnhub API key.

Keep the key outside the source code when publishing the project.

Recommended:

```text
Environment variable / secret
```

## Snowflake

The pipeline requires:

```text
User
Password
Account
Warehouse
Database
Schema
```

Use environment variables, Airflow Connections, Docker secrets, or a protected local dbt profile.

Do not commit passwords to GitHub.

---

# ▶️ How to Run

Open PowerShell and go to:

```powershell
cd C:\Users\USAMA\Documents\Real-Time-Stocks-MDS\infra
```

Start/build the stack:

```powershell
docker compose up -d --build
```

Check containers:

```powershell
docker compose ps
```

---

# 🔍 How to Verify the Pipeline

## 1. Kafka

Open:

```text
http://localhost:9000
```

Check:

```text
stock-quotes
```

---

## 2. MinIO

Open:

```text
http://localhost:9001
```

Check:

```text
bronze-transactions
```

and verify JSON objects are being created.

---

## 3. Airflow

Open:

```text
http://localhost:8080
```

Check that:

```text
minio_to_snowflake
```

is visible and enabled.

---

## 4. Check DAGs from CLI

```powershell
docker exec airflow-scheduler airflow dags list
```

Expected DAG:

```text
minio_to_snowflake
```

---

## 5. Check DAG Runs

```powershell
docker exec airflow-scheduler airflow dags list-runs -d minio_to_snowflake
```

---

## 6. Check Task Status

For a specific run:

```powershell
docker exec airflow-scheduler airflow tasks states-for-dag-run minio_to_snowflake <RUN_ID>
```

Expected successful flow:

```text
download_minio   success
load_snowflake   success
run_dbt          success
```

---

## 7. Check dbt Installation

```powershell
docker exec airflow-scheduler dbt --version
```

The custom Airflow image installs:

```text
dbt-core
dbt-snowflake
```

---

## 8. Check dbt Project Inside Airflow

```powershell
docker exec airflow-scheduler ls /opt/airflow/dbt_stocks
```

Expected project files include:

```text
dbt_project.yml
models
macros
seeds
snapshots
tests
```

---

# 🧪 dbt Commands

### Debug connection

```bash
dbt debug
```

### Parse project

```bash
dbt parse
```

### Build models and tests

```bash
dbt build
```

### Run models

```bash
dbt run
```

### Run tests

```bash
dbt test
```

### Generate documentation

```bash
dbt docs generate
```

---

# 🩺 Troubleshooting

## Airflow DAG not visible

Run:

```powershell
docker exec airflow-scheduler airflow dags list
```

Make sure:

```text
minio_to_snowflake
```

appears.

---

## dbt command not found

Run:

```powershell
docker exec airflow-scheduler dbt --version
```

Rebuild the Airflow image if required:

```powershell
docker compose up -d --build airflow-scheduler airflow-webserver
```

---

## dbt profile not found

Check:

```powershell
docker exec airflow-scheduler ls /home/airflow/.dbt
```

Expected:

```text
profiles.yml
```

---

## Snowflake connection problem

Run:

```powershell
docker exec -it airflow-scheduler dbt debug --project-dir /opt/airflow/dbt_stocks
```

Check the Snowflake connection result.

---

## `run_dbt` is `None`

Check:

```text
load_snowflake
```

Because:

```text
load_snowflake → run_dbt
```

dbt will execute only after the Snowflake task succeeds.

---

## Multiple DAG runs are running

If the DAG takes longer than the schedule interval, runs can overlap.

For a production-oriented implementation, consider:

- `max_active_runs`
- task concurrency
- incremental ingestion
- sensible scheduling intervals
- data watermarks

---

# 🔐 Security

**Never commit secrets to GitHub.**

Do not commit:

```text
.env
API keys
Snowflake passwords
Cloud credentials
Private keys
```

Recommended approaches:

```text
Environment variables
Docker secrets
Airflow Connections
Secret managers
```

Keep `.env` in `.gitignore`.

If a credential has been exposed, rotate/change it before publishing the repository.

---

# ⚠️ Current Implementation Considerations

This project is primarily a portfolio/learning implementation. A production version should improve the ingestion design further.

## Incremental ingestion

The MinIO-to-Snowflake workflow should track which objects have already been processed to avoid repeatedly loading the same files.

Possible approaches:

- File manifests
- Watermarks
- Processed-file metadata
- Snowflake load metadata
- Object timestamps/ETags

## Unique MinIO object names

Object keys should be unique enough to prevent same-second collisions.

A stronger pattern could be:

```text
symbol/timestamp_offset.json
```

instead of relying only on a timestamp.

## Local download filenames

When multiple MinIO objects have the same basename, downloading only the basename can overwrite files.

A production implementation should preserve unique object paths or filenames.

---

# 📸 Project Documentation

Suggested screenshots:

```text
docs/
├── architecture.png
├── finnhub-api-response.png
├── kafdrop-topic.png
├── minio-bucket.png
├── airflow-dag.png
├── snowflake-raw.png
└── power-bi-dashboard.png
```

These screenshots document the major stages of the pipeline.

---

# 📈 End-to-End Example

```text
1. Finnhub provides stock quote
             ↓
2. Python Producer receives quote
             ↓
3. Producer publishes JSON to Kafka
             ↓
4. Kafka stores message in stock-quotes
             ↓
5. Python Consumer reads message
             ↓
6. Consumer writes JSON to MinIO
             ↓
7. Airflow starts the DAG
             ↓
8. download_minio retrieves files
             ↓
9. load_snowflake loads raw data
             ↓
10. run_dbt executes "dbt build"
             ↓
11. dbt builds Bronze/Silver/Gold models
             ↓
12. Power BI reads analytical data
             ↓
13. Dashboard displays transformed data
```

---

# 🏆 Key Learning Outcomes

## Data Ingestion

- REST API ingestion
- JSON data handling
- Repeated stock quote collection

## Streaming

- Kafka producer
- Kafka consumer
- Topic-based streaming

## Storage

- S3-compatible object storage
- MinIO
- Bronze/raw data layer

## Orchestration

- Airflow DAGs
- PythonOperator
- BashOperator
- Task dependencies
- Scheduled execution
- Automated dbt execution

## Data Warehousing

- Snowflake
- Raw data loading
- JSON ingestion
- Warehouse-based analytics

## Transformation

- dbt
- Sources
- SQL models
- Bronze/Silver/Gold architecture
- Fact and dimension modeling

## BI

- Power BI
- DirectQuery
- KPI analysis
- Candlestick-style visualization

## DevOps

- Docker
- Docker Compose
- Containerized services
- Git/GitHub

---

# 💼 Resume Description

### Real-Time Stock Market Data Engineering Pipeline

Built an end-to-end stock market data engineering pipeline using **Python, Kafka, MinIO, Apache Airflow, Snowflake, dbt and Power BI**. Implemented API-based stock quote ingestion, Kafka streaming, raw JSON storage in MinIO, Airflow orchestration, Snowflake loading, Bronze/Silver/Gold dbt transformations, and Power BI analytics. Integrated `dbt build` into the Airflow DAG using `BashOperator`, enabling automatic transformation execution after successful Snowflake ingestion.

---

# 🔗 Repository

GitHub:

**https://github.com/usamapatel-0/stock_market**

---

# 👤 Author

**Usama Patel**

Real-Time Stock Market Data Engineering Project

---

# ⭐ Pipeline Summary

```text
╔══════════════════════════════════════════════════════════════╗
║                 REAL-TIME STOCK PIPELINE                    ║
╚══════════════════════════════════════════════════════════════╝

 Finnhub API
      │
      ▼
 Python Producer
      │
      ▼
 Kafka → stock-quotes
      │
      ▼
 Python Consumer
      │
      ▼
 MinIO → bronze-transactions
      │
      ▼
 Airflow DAG
      │
      ├── download_minio
      │
      ├── load_snowflake
      │
      └── run_dbt
              │
              ▼
       Snowflake + dbt
              │
              ▼
        Bronze → Silver → Gold
              │
              ▼
           Power BI
```

**End-to-end automated data engineering pipeline — from market data ingestion to analytics.**
