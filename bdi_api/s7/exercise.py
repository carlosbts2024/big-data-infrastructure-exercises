import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from botocore.exceptions import BotoCoreError, NoCredentialsError
from fastapi import APIRouter, HTTPException, status

from bdi_api.settings import DBCredentials, Settings

from .s7_helper import list_s3_files, process_s3_files

settings = Settings()
db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONN = psycopg2.connect(
    host=db_credentials.host,
    port=db_credentials.port,
    user=db_credentials.username,
    password=db_credentials.password,
    dbname=db_credentials.database
)

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)

def create_table_if_not_exists():
    with DB_CONN.cursor() as cur:
        sql = """
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
        """
        cur.execute(sql)
        DB_CONN.commit()

def insert_data_to_postgres(data: list[dict]):
    if not data:
        logging.info("No data available to insert.")
        return

    num_records = len(data)
    logging.info(f"Preparing to insert {num_records} valid records into PostgreSQL.")

    if num_records == 0:
        logging.warning("No valid records found. Skipping database insertion.")
        return

    records = [
        (
            record["icao"],
            record["registration"],
            record["type"],
            record["lat"],
            record["lon"],
            record["max_altitude_baro"],
            record["max_ground_speed"],
            record["had_emergency"],
            datetime.fromtimestamp(record["timestamp"], tz=timezone.utc)
            if record.get("timestamp")
            else datetime.now(timezone.utc)
        )
        for record in data
    ]
    with DB_CONN.cursor() as cur:
        insert_sql = """
        INSERT INTO aircraft_data (
            icao, registration, type, lat, lon,
            max_altitude_baro, max_ground_speed, had_emergency, timestamp
        )
        VALUES %s
        ON CONFLICT (icao, timestamp)
        DO UPDATE SET
            registration = EXCLUDED.registration,
            type = EXCLUDED.type,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            max_altitude_baro = EXCLUDED.max_altitude_baro,
            max_ground_speed = EXCLUDED.max_ground_speed,
            had_emergency = EXCLUDED.had_emergency,
            timestamp = EXCLUDED.timestamp;
        """
        psycopg2.extras.execute_values(cur, insert_sql, records)
        DB_CONN.commit()

    logging.info(f"Successfully inserted {num_records} records into PostgreSQL.")


def refine_data() -> list:
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    try:
        file_keys = list_s3_files(s3_bucket, s3_prefix_path)
        if not file_keys:
            return []

        all_aircraft_data = process_s3_files(s3_bucket, file_keys)
        return all_aircraft_data

    except (NoCredentialsError, BotoCoreError) as e:
        raise HTTPException(status_code=500, detail=f"S3 Error: {str(e)}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing Error: {str(e)}") from e

@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    try:
        create_table_if_not_exists()
        aircraft_data = refine_data()  # Fetch from S3
        insert_data_to_postgres(aircraft_data)
        return "Aircraft data successfully inserted into PostgreSQL."

    except Exception as e:
        logging.error(f"Processing Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Processing Error: {str(e)}") from e

@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict[str, str]]:
    try:
        with DB_CONN.cursor() as cur:
            offset = page * num_results

            query = """
            SELECT icao, registration, type
            FROM aircraft_data
            ORDER BY icao ASC
            LIMIT %s OFFSET %s;
            """
            cur.execute(query, (num_results, offset))
            rows = cur.fetchall()

        aircraft_list = [{"icao": row[0], "registration": row[1], "type": row[2]} for row in rows]

        logging.info(f"Retrieved {len(aircraft_list)} aircraft records from database.")
        return aircraft_list

    except Exception as e:
        logging.error(f"Database error while listing aircraft: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving aircraft data from the database") from e


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    try:
        with DB_CONN.cursor() as cur:
            offset = page * num_results  # Pagination logic

            query = """
            SELECT timestamp, lat, lon
            FROM aircraft_data
            WHERE icao = %s
            ORDER BY timestamp ASC
            LIMIT %s OFFSET %s;
            """
            cur.execute(query, (icao, num_results, offset))
            rows = cur.fetchall()

        positions = [{"timestamp": row[0].timestamp(), "lat": row[1], "lon": row[2]} for row in rows]

        logging.info(f"Retrieved {len(positions)} positions for aircraft {icao}.")
        return positions

    except Exception as e:
        logging.error(f"Database error while retrieving positions for {icao}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving aircraft position data from the database") from e


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> list[dict[str, int]]:
    """Returns all recorded statistics about the aircraft.

    Fields returned:
    * max_altitude_baro
    * max_ground_speed
    * had_emergency

    FROM THE DATABASE

    Uses credentials passed from `db_credentials`
    """
    try:
        with DB_CONN.cursor() as cur:
            query = """
            SELECT max_altitude_baro, max_ground_speed, had_emergency
            FROM aircraft_data
            WHERE icao = %s;
            """
            cur.execute(query, (icao,))
            rows = cur.fetchall()

        if not rows:
            logging.info(f"No data found for aircraft {icao}. Returning empty list.")
            return []

        return [
            {
                "max_altitude_baro": str(row[0]),
                "max_ground_speed": str(row[1]),
                "had_emergency": bool(row[2])
            }
            for row in rows
        ]

    except Exception as e:
        logging.error(f"Database error while retrieving stats for {icao}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving aircraft statistics from the database") from e

    #return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}
