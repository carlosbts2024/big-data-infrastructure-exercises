import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from bdi_api.settings import Settings
from dags.s8_helper import get_db_conn, put_db_conn

settings = Settings()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)


class AircraftReturn(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    registration: Optional[str]
    type: Optional[str]
    owner: Optional[str]
    manufacturer: Optional[str]
    model: Optional[str]

@s8.get("/aircraft/", response_model=list[AircraftReturn])
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    conn = None
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            offset = page * num_results

            query = """
                    SELECT
                    a.icao, a.reg, a.icaotype,
                    d.manufacturer, d.model, d.ownop
                    FROM aircraft_db a
                    LEFT JOIN aircraft_database d ON a.icao = d.icao
                    WHERE d.manufacturer IS NOT NULL AND d.model IS NOT NULL
                    ORDER BY a.icao ASC
                    LIMIT %s OFFSET %s;
                """
            cur.execute(query, (num_results, offset))
            rows = cur.fetchall()

        result = [
            AircraftReturn(
                icao=row[0],
                registration=row[1],
                type=row[2],
                manufacturer=row[3],
                model=row[4],
                owner=row[5],
            )
            for row in rows
        ]

        logging.info(f"Retrieved {len(result)} aircraft records.")
        return result

    except Exception as e:
        logging.error(f"Database error while listing aircraft: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving aircraft data from database") from e
    finally:
        put_db_conn(conn)


class AircraftCO2(BaseModel):
    # DO NOT MODIFY IT
    icao: str
    hours_flown: float
    """Co2 tons generated"""
    co2: Optional[float]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2:
    day_to_compute = day
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM aircraft_data
                WHERE icao = %s
                  AND DATE(timestamp) = %s
            """,
                (icao, day_to_compute),
            )
            result = cur.fetchone()

            if not result or result[0] == 0:
                logging.error(f"No flight data found for {icao} on {day_to_compute}")
                raise HTTPException(status_code=404, detail="No flight data found for this aircraft and day")

            record_count = result[0]
            hours_flown = record_count * 5 / 3600
            cur.execute(
                """
                SELECT type
                FROM aircraft_data
                WHERE icao = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """,
                (icao,),
            )
            type_result = cur.fetchone()
            aircraft_type = type_result[0] if type_result else None

            galph = None
            if aircraft_type:
                cur.execute(
                    """
                    SELECT galph
                    FROM aircraft_type_data
                    WHERE icao_code = %s
                """,
                    (aircraft_type,),
                )
                fuel_result = cur.fetchone()
                galph = fuel_result[0] if fuel_result else None

            co2_tons = None
            if galph:
                fuel_used_gal = galph * hours_flown
                fuel_used_kg = fuel_used_gal * 3.04
                co2_tons = (fuel_used_kg * 3.15) / 907.185

            return AircraftCO2(icao=icao, hours_flown=hours_flown, co2=co2_tons)

    except Exception as e:
        logging.error(f"Error calculating CO2 for {icao} on {day_to_compute}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") from e
    finally:
        put_db_conn(conn)
