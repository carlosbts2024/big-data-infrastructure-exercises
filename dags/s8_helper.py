import io
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
from tqdm import tqdm
from bdi_api.settings import DBCredentials
import psycopg2
import psycopg2.extras
import psycopg2.pool

s3_client = boto3.client("s3")
_db_pool = None

def download_and_upload(link, base_url, s3_bucket, s3_prefix_path):
    file_url = f"{base_url}{link}"
    s3_key = f"{s3_prefix_path}{link.replace('.json.gz', '.json')}"

    try:
        response = requests.get(file_url, stream=True, timeout=10)
        response.raise_for_status()

        file_buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=1024):
            file_buffer.write(chunk)

        file_buffer.seek(0)
        s3_client.upload_fileobj(file_buffer, s3_bucket, s3_key)
        file_buffer.close()

        print(f"✅ Uploaded: {s3_key}")

    except requests.RequestException as e:
        print(f"❌ Failed: {file_url} - {str(e)}")

def delete_files_in_s3_folder(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        print(f"No files to delete in s3://{bucket_name}/{prefix}")
        return

    keys_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
    s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": keys_to_delete})
    print(f"🗑️ Deleted {len(keys_to_delete)} files from s3://{bucket_name}/{prefix}")


def upload_to_s3(data: list, s3_bucket: str, s3_output_prefix: str):
    output_key = f"{s3_output_prefix}prepared.json"
    print("prepared json file: " + output_key)

    with io.BytesIO(json.dumps(data, indent=4).encode()) as file_buffer:
        s3_client.upload_fileobj(file_buffer, s3_bucket, output_key)
    print(f"Upload complete: s3://{s3_bucket}/{output_key}")

def upload_file_to_s3(local_path: str, s3_bucket: str, s3_key: str):
    """Uploads a local file to S3 as-is."""
    print(f"📤 Uploading {local_path} to s3://{s3_bucket}/{s3_key}")
    with open(local_path, "rb") as file_buffer:
        s3_client.upload_fileobj(file_buffer, s3_bucket, s3_key)
    print(f"✅ Upload complete: s3://{s3_bucket}/{s3_key}")


# --- S3 Handling ---
def list_s3_files(bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def get_json_from_s3(bucket: str, key: str) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.load(response["Body"])

# --- Data Processing ---
def process_s3_files(bucket: str, file_keys: list[str], batch_size: int = 100) -> list[dict]:
    seen = set()
    all_data = []

    def process_file(key):
        print(f"[DOWNLOAD] Fetching and processing file: {key}")
        try:
            data = get_json_from_s3(bucket, key)
            processed = process_aircraft_data(data, batch_size)
            unique = []
            for record in processed:
                uid = (
                    record["icao"], record["registration"], record["type"],
                    record["lat"], record["lon"], record["max_altitude_baro"],
                    record["max_ground_speed"], record["had_emergency"]
                )
                if uid not in seen:
                    seen.add(uid)
                    unique.append(record)
            print(f"[SUCCESS] Finished processing file: {key} (Records added: {len(unique)})")
            return unique
        except Exception as e:
            logging.warning(f"Error processing {key}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_file, key): key for key in file_keys}
        for future in tqdm(as_completed(futures), total=len(file_keys), desc="Processing files", unit="file"):
            try:
                result = future.result()
                all_data.extend(result)
            except Exception as e:
                print(f"[ERROR] Error during future completion: {e}")

    print(f"[COMPLETE] Finished processing all files. Total unique records collected: {len(all_data)}")
    return all_data

def process_batch_s8(batch, aircraft_timestamp):
    processed_batch = []
    unique_aircraft_data = set()

    for ac in batch:
        if not all(ac.get(field) for field in ["hex", "lat", "lon"]):
            continue
        if ac.get("r") == "TWR":
            continue

        emergency = ac.get("emergency")
        had_emergency = emergency not in {None, "none", "None", "null"}

        alt_baro = ac.get("alt_baro")
        try:
            alt_baro = int(alt_baro) if alt_baro not in {"ground", None} and int(alt_baro) >= 0 else 0
        except (ValueError, TypeError):
            alt_baro = 0

        try:
            gs = float(ac.get("gs", 0.0)) if ac.get("gs") not in {None, "none", "None", "null"} else 0.0
        except (ValueError, TypeError):
            gs = 0.0

        unique_key = (
            str(ac.get("hex")),
            str(ac.get("r")),
            str(ac.get("t")),
            round(float(ac["lat"]), 6),
            round(float(ac["lon"]), 6),
            alt_baro,
            gs,
            had_emergency,
        )

        if unique_key not in unique_aircraft_data:
            unique_aircraft_data.add(unique_key)
            processed_batch.append(
                {
                    "icao": unique_key[0],
                    "registration": unique_key[1],
                    "type": unique_key[2],
                    "lat": unique_key[3],
                    "lon": unique_key[4],
                    "max_altitude_baro": unique_key[5],
                    "max_ground_speed": unique_key[6],
                    "had_emergency": unique_key[7],
                    "timestamp": aircraft_timestamp,
                }
            )

    return processed_batch

def process_aircraft_data(data: dict, batch_size: int = 100) -> list[dict]:
    aircraft = data.get("aircraft", [])
    ts = data.get("now")
    if not aircraft:
        return []

    batches = [aircraft[i:i+batch_size] for i in range(0, len(aircraft), batch_size)]

    with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
        results = executor.map(process_batch_s8, batches, [ts]*len(batches))

    return [item for r in results for item in r]

# --- PostgreSQL Handling ---
def get_db_pool():
    global _db_pool
    if _db_pool is None:
        creds = DBCredentials()  # instantiate credentials
        _db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=creds.POSTGRES_DSN  # ✅ USE THE DYNAMIC DSN from DBCredentials
        )
    return _db_pool

def get_db_conn():
    return get_db_pool().getconn()

def put_db_conn(conn):
    return get_db_pool().putconn(conn)