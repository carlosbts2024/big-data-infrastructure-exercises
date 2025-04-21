from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import requests

from s8_helper import  get_db_conn, put_db_conn, upload_file_to_s3

# Constants
AIRCRAFT_TYPE_DATA_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"
LOCAL_FILE_PATH = "/usr/local/airflow/include/aircraft_type_fuel_consumption_rates.json"

def download_aircraft_type_data_only(bucket_name: str):
    print(f"✨ Downloading aircraft type fuel consumption data from {AIRCRAFT_TYPE_DATA_URL}")
    response = requests.get(AIRCRAFT_TYPE_DATA_URL)
    response.raise_for_status()

    with open(LOCAL_FILE_PATH, "w") as f:
        json.dump(response.json(), f, indent=4)

    print(f"✅ Successfully saved to {LOCAL_FILE_PATH}")

    # Upload to S3 after download
    upload_file_to_s3(LOCAL_FILE_PATH, bucket_name, "raw/aircraft_type_fuel_consumption_rates.json")

def store_aircraft_type_data():
    print("🔗 Getting DB connection from pool...")
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            print(f"📦 Reading file: {LOCAL_FILE_PATH}")
            with open(LOCAL_FILE_PATH, "r") as f:
                data = json.load(f)

            print("🛠 Creating table if not exists...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aircraft_type_data (
                    icao_code VARCHAR(10) PRIMARY KEY,
                    name TEXT,
                    galph INTEGER,
                    category TEXT
                );
            """)

            print("📤 Inserting/Updating records...")
            for icao_code, attributes in data.items():
                cur.execute("""
                    INSERT INTO aircraft_type_data (icao_code, name, galph, category)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (icao_code) DO UPDATE SET
                        name = EXCLUDED.name,
                        galph = EXCLUDED.galph,
                        category = EXCLUDED.category;
                """, (
                    icao_code,
                    attributes.get("name"),
                    attributes.get("galph"),
                    attributes.get("category")
                ))

            conn.commit()
            print("✅ All aircraft type data inserted successfully.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error while storing aircraft type data: {e}")
        raise
    finally:
        put_db_conn(conn)

# Define DAG
with DAG(
        dag_id="download_and_store_aircraft_type_data_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["aircraft", "fuel-consumption"],
) as dag:

    download_aircraft_type_data_task = PythonOperator(
        task_id="download_aircraft_type_data",
        python_callable=download_aircraft_type_data_only,
        op_kwargs={"bucket_name": "bdi-aircraft2"},
        dag=dag,
    )

    store_aircraft_type_data_task = PythonOperator(
        task_id="store_aircraft_type_data",
        python_callable=store_aircraft_type_data,
        dag=dag,
    )

    download_aircraft_type_data_task >> store_aircraft_type_data_task
