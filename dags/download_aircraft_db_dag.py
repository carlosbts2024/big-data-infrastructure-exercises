from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from io import BytesIO
import gzip
import json
import psycopg2
import psycopg2.extras
from itertools import islice
from s8_helper import get_db_conn, put_db_conn, s3_client

DB_FILE_PATH = "/usr/local/airflow/include/basic-ac-db.json.gz"
BATCH_SIZE = 1000

AIRCRAFT_DB_URL = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
LOCAL_FILE_PATH = "/usr/local/airflow/include/basic-ac-db.json.gz"
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk

def download_aircraft_database_gz(bucket_name: str):
    import requests

    print(f"📥 Downloading aircraft database from {AIRCRAFT_DB_URL}")
    response = requests.get(AIRCRAFT_DB_URL, timeout=60)
    response.raise_for_status()

    with open(LOCAL_FILE_PATH, "wb") as f:
        f.write(response.content)
    print(f"✅ Saved compressed file at {LOCAL_FILE_PATH}")

    print(f"📦 Uploading to S3 bucket {bucket_name}...")
    output_key = "raw/aircraft_db/basic-ac-db.json.gz"
    with BytesIO(response.content) as file_buffer:
        s3_client.upload_fileobj(file_buffer, bucket_name, output_key)
    print(f"✅ Upload complete: s3://{bucket_name}/{output_key}")


def store_aircraft_database():
    print("🔗 Getting DB connection from pool...")
    conn = get_db_conn()
    cur = conn.cursor()

    print(f"📦 Decompressing and reading file: {DB_FILE_PATH}")
    with gzip.open(DB_FILE_PATH, "rt", encoding="utf-8") as f:
        lines = f.readlines()

    print("🛠 Creating table if not exists...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS aircraft_db (
            icao VARCHAR PRIMARY KEY,
            reg TEXT,
            icaotype TEXT,
            year INTEGER,
            manufacturer TEXT,
            model TEXT,
            ownop TEXT,
            faa_pia BOOLEAN,
            faa_ladd BOOLEAN,
            short_type TEXT,
            mil BOOLEAN
        );
    """)
    conn.commit()

    print(f"📊 Preparing to insert {len(lines)} total records...")

    insert_query = """
        INSERT INTO aircraft_db (
            icao, reg, icaotype, year, manufacturer,
            model, ownop, faa_pia, faa_ladd, short_type, mil
        ) VALUES %s
        ON CONFLICT (icao) DO UPDATE SET
            reg = EXCLUDED.reg,
            icaotype = EXCLUDED.icaotype,
            year = EXCLUDED.year,
            manufacturer = EXCLUDED.manufacturer,
            model = EXCLUDED.model,
            ownop = EXCLUDED.ownop,
            faa_pia = EXCLUDED.faa_pia,
            faa_ladd = EXCLUDED.faa_ladd,
            short_type = EXCLUDED.short_type,
            mil = EXCLUDED.mil;
    """

    total = len(lines)
    inserted = 0

    for chunk in chunked_iterable(lines, BATCH_SIZE):
        records = []
        for line in chunk:
            try:
                r = json.loads(line)
                records.append((
                    r.get("icao"),
                    r.get("reg"),
                    r.get("icaotype"),
                    int(r["year"]) if r.get("year") not in ["", None] else None,
                    r.get("manufacturer"),
                    r.get("model"),
                    r.get("ownop"),
                    r.get("faa_pia"),
                    r.get("faa_ladd"),
                    r.get("short_type"),
                    r.get("mil"),
                ))
            except Exception as e:
                print(f"⚠️ Skipping line due to error: {e}")

        if records:
            psycopg2.extras.execute_values(cur, insert_query, records)
            conn.commit()
            inserted += len(records)
            print(f"✅ Inserted {inserted} / {total} records ({round(100*inserted/total, 2)}%)")

    cur.close()
    put_db_conn(conn)
    print("🚀 All data ingested.")

with DAG(
        dag_id="download_and_store_aircraft_db_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["aircraft", "db", "fast-ingest"]
) as dag:

    download_task = PythonOperator(
        task_id="download_aircraft_database",
        python_callable=download_aircraft_database_gz,
        op_kwargs={"bucket_name": "bdi-aircraft2"},
        dag=dag,
    )

    store_task = PythonOperator(
        task_id="store_aircraft_database",
        python_callable=store_aircraft_database,
        dag=dag,
    )

    download_task >> store_task
