import io
import json
import os
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import boto3
import requests
from tqdm import tqdm

s3_client = boto3.client("s3")
thread_local = threading.local()

def get_session() -> requests.Session:
    """Get or create a session for the current thread."""
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

def remove_gz_extension(file_name):
    return file_name[:-3] if file_name.endswith(".gz") else file_name


def delete_files_in_s3_folder(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        print("No files to delete in S3 folder.")
        return

    keys_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
    s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": keys_to_delete})
    print(f"Deleted {len(keys_to_delete)} files from s3://{bucket_name}/{prefix}")


def clear_directory(directory_path):
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return

    files_in_directory = os.listdir(directory_path)
    if files_in_directory:
        for file_name in files_in_directory:
            file_path = os.path.join(directory_path, file_name)
            if os.path.isfile(file_path):  # Check if it's a file
                os.remove(file_path)
    else:
        print(f"No files found in {directory_path}.")


def list_s3_files(s3_bucket: str, s3_prefix_path: str) -> list:
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
    return [obj["Key"] for obj in response.get("Contents", [])]


def get_json_from_s3(s3_bucket: str, file_key: str) -> dict:
    file_response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
    return json.load(file_response["Body"])


def process_batch(batch, aircraft_timestamp):
    processed_batch = []
    unique_aircraft_data = set()

    for ac in batch:
        if not all(ac.get(field) for field in ["hex", "lat", "lon"]):
            continue

        if ac.get("r") == "TWR":
            continue

        ac["emergency"] = False if ac.get("emergency") in [None, "none", "None", "null"] else ac.get("emergency")

        if ac.get("alt_baro") == "ground" or (isinstance(ac.get("alt_baro"), (int, float)) and ac["alt_baro"] < 0):
            ac["alt_baro"] = 0

        string_record = json.dumps(ac, sort_keys=True)
        if string_record not in unique_aircraft_data:
            unique_aircraft_data.add(string_record)
            processed_batch.append(
                {
                    "icao": ac.get("hex"),
                    "registration": ac.get("r"),
                    "type": ac.get("t"),
                    "lat": ac.get("lat"),
                    "lon": ac.get("lon"),
                    "max_altitude_baro": ac.get("alt_baro"),
                    "max_ground_speed": ac.get("gs"),
                    "had_emergency": ac.get("emergency"),
                    "timestamp": aircraft_timestamp,
                },
            )
    return processed_batch

def process_aircraft_data(data: dict, batch_size: int = 100) -> list:
    aircraft_data = data.get("aircraft", [])
    aircraft_timestamp = data.get("now")

    cleaned_data = []
    total_records = len(aircraft_data)

    batches = [aircraft_data[i:i + batch_size] for i in range(0, total_records, batch_size)]

    max_workers = os.cpu_count() or 4
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_batch, batches, [aircraft_timestamp] * len(batches)))

    for result in results:
        cleaned_data.extend(result)

    return cleaned_data


def process_s3_files(s3_bucket: str, file_keys: list, batch_size: int = 100) -> list:
    all_aircraft_data = []
    max_threads = 10

    def download_and_process(file_key):
        try:
            file_data = get_json_from_s3(s3_bucket, file_key)
            return process_aircraft_data(file_data, batch_size)
        except Exception as e:
            print(f"Error processing {file_key}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_key = {executor.submit(download_and_process, file_key): file_key for file_key in file_keys}

        for future in tqdm(as_completed(future_to_key), total=len(file_keys), desc="Processing Files", unit="file"):
            all_aircraft_data.extend(future.result())

    return all_aircraft_data


def upload_to_s3(data: list, s3_bucket: str, s3_output_prefix: str):
    output_key = f"{s3_output_prefix}prepared.json"
    print("prepared json file: " + output_key)

    with io.BytesIO(json.dumps(data, indent=4).encode()) as file_buffer:
        s3_client.upload_fileobj(file_buffer, s3_bucket, output_key)
    print(f"Upload complete: s3://{s3_bucket}/{output_key}")


def download_file(link, base_url, s3_bucket, s3_prefix_path):
    file_url = f"{base_url}{link}"
    s3_object_key = f"{s3_prefix_path}{remove_gz_extension(link)}"
    session = get_session()

    try:
        with session.get(file_url, stream=True, timeout=10) as response:
            response.raise_for_status()

            file_buffer = io.BytesIO()

            for chunk in response.iter_content(chunk_size=1024):
                file_buffer.write(chunk)

            file_buffer.seek(0)
            s3_client.upload_fileobj(file_buffer, s3_bucket, s3_object_key)
            file_buffer.close()

    except requests.RequestException as e:
        print(f"Download Error: {file_url} - {str(e)}")
