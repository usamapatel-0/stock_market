import os
import boto3
import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ============================================================
# MINIO CONFIGURATION
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"

LOCAL_DIR = "/tmp/minio_downloads"


# ============================================================
# SNOWFLAKE CONFIGURATION
# ============================================================

SNOWFLAKE_USER = "USAMAPATEL"
SNOWFLAKE_PASSWORD = "TJjmxDnWrpX5bbr"
SNOWFLAKE_ACCOUNT = "lv45656.ap-southeast-7.aws"

SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCK_MDS"
SNOWFLAKE_SCHEMA = "COMMON"


# ============================================================
# DOWNLOAD DATA FROM MINIO
# ============================================================

def download_from_minio():

    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    response = s3.list_objects_v2(
        Bucket=BUCKET
    )

    objects = response.get("Contents", [])

    if not objects:
        print("No files found in MinIO.")
        return []

    local_files = []

    for obj in objects:

        key = obj["Key"]

        filename = os.path.basename(key)

        local_file = os.path.join(
            LOCAL_DIR,
            filename
        )

        s3.download_file(
            BUCKET,
            key,
            local_file
        )

        print(
            f"Downloaded: {key} -> {local_file}"
        )

        local_files.append(local_file)

    print(
        f"Total files downloaded: {len(local_files)}"
    )

    return local_files


# ============================================================
# LOAD DATA INTO SNOWFLAKE
# ============================================================

def load_to_snowflake(**kwargs):

    local_files = kwargs["ti"].xcom_pull(
        task_ids="download_minio"
    )

    if not local_files:
        print("No files to load into Snowflake.")
        return

    print(
        f"Files received from MinIO: {len(local_files)}"
    )

    # --------------------------------------------------------
    # Connect to Snowflake
    # --------------------------------------------------------

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )

    cur = conn.cursor()

    try:

        print("Connected to Snowflake successfully.")

        # ----------------------------------------------------
        # Upload files to Snowflake table stage
        # ----------------------------------------------------

        for f in local_files:

            print(
                f"Uploading file: {f}"
            )

            cur.execute(
                f"PUT file://{f} @%bronze_stock_quotes_raw"
            )

            result = cur.fetchall()

            print(
                f"PUT result: {result}"
            )

        # ----------------------------------------------------
        # Copy JSON data into table
        # ----------------------------------------------------

        print(
            "Loading JSON data into BRONZE_STOCK_QUOTES_RAW..."
        )

        cur.execute("""
            COPY INTO bronze_stock_quotes_raw (v)
            FROM @%bronze_stock_quotes_raw
            FILE_FORMAT = (
                TYPE = JSON
            )
        """)

        results = cur.fetchall()

        print(
            "COPY INTO executed successfully."
        )

        print(
            f"COPY result: {results}"
        )

    except Exception as e:

        print(
            f"Snowflake loading error: {e}"
        )

        raise

    finally:

        cur.close()
        conn.close()

        print(
            "Snowflake connection closed."
        )


# ============================================================
# AIRFLOW DEFAULT ARGUMENTS
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,

    "start_date": datetime(
        2026,
        9,
        12
    ),

    "retries": 1,

    "retry_delay": timedelta(
        minutes=5
    ),
}


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(

    dag_id="minio_to_snowflake",

    default_args=default_args,

    schedule_interval="*/1 * * * *",

    catchup=False,

    tags=[
        "minio",
        "snowflake",
        "stocks"
    ],

) as dag:

    # --------------------------------------------------------
    # Task 1: MinIO → Airflow
    # --------------------------------------------------------

    task1 = PythonOperator(

        task_id="download_minio",

        python_callable=download_from_minio,

    )


    # --------------------------------------------------------
    # Task 2: Airflow → Snowflake
    # --------------------------------------------------------

    task2 = PythonOperator(

        task_id="load_snowflake",

        python_callable=load_to_snowflake,

        provide_context=True,

    )


    # --------------------------------------------------------
    # Task dependency
    # --------------------------------------------------------

    task1 >> task2