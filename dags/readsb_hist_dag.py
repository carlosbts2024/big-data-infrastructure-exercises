import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import io
import boto3
import psycopg2
import psycopg2.extras
import psycopg2.pool
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from s8_helper import delete_files_in_s3_folder, download_and_upload, list_s3_files, process_s3_files, upload_to_s3, get_db_conn, put_db_conn
from tqdm import tqdm

from bdi_api.settings import DBCredentials

sys.path.insert(0, "/usr/local/airflow")

from bdi_api.settings import Settings

# --- Configuration ---
settings = Settings()
s3_client = boto3.client("s3")
_db_pool = None

def download_data(bucket_name, **context):
    execution_date = context["ds"]
    print(f"📅 DAG Execution Date: {execution_date}")
    year, month, day = execution_date.split("-")
    partition = f"{year}{month}{day}"

    base_url = f"{settings.source_url}/{year}/{month}/{day}/"
    s3_bucket = bucket_name
    s3_prefix_path = f"raw/day={partition}/"

    try:
        delete_files_in_s3_folder(s3_bucket, s3_prefix_path)

        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        file_links = [
            link["href"]
            for row in soup.find_all("tr", class_="file")
            if (link := row.find("a", href=True)) and link["href"].endswith(".json.gz")
        ]

        if not file_links:
            print("No files found to download.")
            return "No files found."

        file_links = file_links[:100]  # Limit to 100 files

        print(f"Starting upload of {len(file_links)} files to S3...")

        with ThreadPoolExecutor(max_workers=10) as executor:
            list(
                tqdm(
                    executor.map(
                        lambda link: download_and_upload(link, base_url, s3_bucket, s3_prefix_path),
                        file_links,
                    ),
                    total=len(file_links),
                    desc="Uploading Files",
                )
            )

        print("✅ All uploads completed.")
        return "OK"

    except Exception as e:
        print(f"❌ Error in download_data: {e}")
        raise

# --- Main DAG-compatible function ---
def prepare_data(bucket_name, **context) -> str:
    execution_date = context["ds"]  # e.g. '2023-11-01'
    partition = execution_date.replace("-", "")
    s3_prefix_path = f"raw/day={partition}/"
    s3_output_prefix = f"prepared_day={partition}/"

    try:
        file_keys = list_s3_files(bucket_name, s3_prefix_path)
        if not file_keys:
            logging.info("No files found in S3.")
            return "No data found for this date."

        print("[STEP 1] Loading raw data from S3...")
        aircraft_data = process_s3_files(bucket_name, file_keys)
        print(f"[STEP 1 COMPLETE] Raw data loaded: {len(aircraft_data)} rows")

        if aircraft_data:
            upload_to_s3(aircraft_data, bucket_name, s3_output_prefix)
        else:
            return "No valid aircraft data found for preparation."

        create_table_if_not_exists()
        insert_data_to_postgres(aircraft_data)
        return f"✅ Inserted {len(aircraft_data)} records into PostgreSQL."

    except Exception as e:
        logging.error(f"❌ Processing error: {str(e)}")
        raise

def create_table_if_not_exists():
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_data (
                id SERIAL PRIMARY KEY,
                icao VARCHAR(10),
                registration VARCHAR(20),
                type VARCHAR(10),
                lat FLOAT,
                lon FLOAT,
                max_altitude_baro INTEGER,
                max_ground_speed FLOAT,
                had_emergency VARCHAR(10),
                timestamp TIMESTAMP,
                CONSTRAINT aircraft_unique UNIQUE (icao, timestamp)
            );
            """)
            conn.commit()
    finally:
        put_db_conn(conn)

def insert_data_to_postgres(data: list[dict]):
    if not data:
        logging.info("⚡ No data to insert. Skipping...")
        return

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Prepare data into CSV format in memory
            output = io.StringIO()
            for r in data:
                ts = (
                    datetime.fromtimestamp(r["timestamp"], tz=timezone.utc)
                    if r.get("timestamp")
                    else datetime.now(timezone.utc)
                )
                row = [
                    r["icao"] or '',
                    r["registration"] or '',
                    r["type"] or '',
                    r["lat"] if r.get("lat") is not None else '',
                    r["lon"] if r.get("lon") is not None else '',
                    r["max_altitude_baro"] if r.get("max_altitude_baro") is not None else '',
                    r["max_ground_speed"] if r.get("max_ground_speed") is not None else '',
                    r["had_emergency"] or '',
                    ts.isoformat(),
                    ]
                output.write('\t'.join(map(str, row)) + '\n')
            output.seek(0)

            # Create a temp table to safely bulk load
            cur.execute("""
                CREATE TEMP TABLE tmp_aircraft_data (
                    icao VARCHAR(10),
                    registration VARCHAR(20),
                    type VARCHAR(10),
                    lat FLOAT,
                    lon FLOAT,
                    max_altitude_baro INTEGER,
                    max_ground_speed FLOAT,
                    had_emergency VARCHAR(10),
                    timestamp TIMESTAMP
                ) ON COMMIT DROP;
            """)

            # Copy data into the temp table
            cur.copy_from(output, 'tmp_aircraft_data', sep='\t')

            # Merge data into the real table (with upsert)
            cur.execute("""
                INSERT INTO aircraft_data (
                    icao, registration, type, lat, lon,
                    max_altitude_baro, max_ground_speed, had_emergency, timestamp
                )
                SELECT 
                    icao, registration, type, lat, lon,
                    max_altitude_baro, max_ground_speed, had_emergency, timestamp
                FROM tmp_aircraft_data
                ON CONFLICT (icao, timestamp) DO UPDATE SET
                    registration = EXCLUDED.registration,
                    type = EXCLUDED.type,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    max_altitude_baro = EXCLUDED.max_altitude_baro,
                    max_ground_speed = EXCLUDED.max_ground_speed,
                    had_emergency = EXCLUDED.had_emergency,
                    timestamp = EXCLUDED.timestamp;
            """)

            conn.commit()
            logging.info(f"🚀 COPY FROM + UPSERT completed successfully for {len(data)} rows.")

    except Exception as e:
        logging.error(f"🔥 Error inserting with COPY: {str(e)}")
        raise
    finally:
        put_db_conn(conn)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id="readsb_hist_dag",
        start_date=datetime(2023, 11, 1),
        end_date=datetime(2024, 11, 1),
        schedule_interval="@monthly",
        catchup=True,
        default_args=default_args,
        tags=["readsb", "elt"],
) as dag:

    download_task = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
        provide_context=True,
        op_kwargs={"bucket_name": "bdi-aircraft2"},
        dag=dag,
    )

    prepare_task = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
        provide_context=True,
        op_kwargs={"bucket_name": "bdi-aircraft2"},
        dag=dag,
    )

    download_task >> prepare_task
