import datetime
import math
import os
import typing
from collections import defaultdict
from concurrent.futures.process import ProcessPoolExecutor
from typing import Iterable, Tuple

import psycopg
from dotenv import load_dotenv
from psycopg.rows import tuple_row
from tqdm import tqdm
from upath import UPath

from jsoncache import *
from jsoncache.build.lib.jsoncache import JsonStore


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # Earth radius in meters

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


# import pipifax_io.dyn_codegen

load_dotenv()

DATABASE_URL = "postgresql://localhost:5432/nextbike"


def get_bike_locations(
    data, country_whitelist: typing.Container[str] | None = None
) -> dict[str, dict[str, list[float]]]:
    bike_locations = defaultdict(dict)

    for country_id, country in data["countries"].items():
        if country_whitelist and country_id not in country_whitelist:
            continue

        for city in country["cities"].values():
            for place in city["places"].values():
                for bike in place["bike_list"].values():
                    bike_locations[country_id][bike["number"]] = [
                        place["lat"],
                        place["lng"],
                    ]

    return bike_locations


def get_standing_times(
    since: datetime.datetime, until: datetime.datetime, country: str
):
    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")), preprocessors=[]
    )

    with ProcessPoolExecutor(11) as executor:
        iterable = cache.iter_files(since=since, until=until, executor=executor)
        bikes = {}
        standing_times = []
        missing = {}

        DIST_THRESH_M = 50
        MAX_GAP_SECONDS = 30  # allow up to 3 ticks missing (with 10s cadence)

        try:
            for timestamp, data in tqdm(
                iterable,
                smoothing=0,
                total=round((until - since).total_seconds()) // 10,
            ):
                country_bikes = get_bike_locations(data, {country})[country]
                present_ids = set(country_bikes.keys())

                # mark currently present bikes (and clear missing marker)
                for bike_id, (lat, lon) in country_bikes.items():
                    missing.pop(bike_id, None)

                    if bike_id not in bikes:
                        # (start_t, last_seen_t, lat, lon)
                        bikes[bike_id] = (timestamp, timestamp, lat, lon)
                        continue

                    start_t, last_seen_t, lat0, lon0 = bikes[bike_id]

                    if haversine(lat, lon, lat0, lon0) < DIST_THRESH_M:
                        # still at same place: advance last_seen_t
                        bikes[bike_id] = (start_t, timestamp, lat0, lon0)
                        continue

                    # moved: close at last time we saw it at the old location
                    standing_times.append((bike_id, start_t, last_seen_t, lat0, lon0))

                    # start new segment at the current observation time
                    bikes[bike_id] = (timestamp, timestamp, lat, lon)

                # handle bikes not present now: start/continue missing timer
                for bike_id in list(bikes.keys()):
                    if bike_id in present_ids:
                        continue

                    if bike_id not in missing:
                        missing[bike_id] = timestamp
                        continue

                    # if missing too long, close at the last time we actually saw it (not first_missing_ts)
                    first_missing_ts = missing[bike_id]
                    if (timestamp - first_missing_ts).total_seconds() > MAX_GAP_SECONDS:
                        start_t, last_seen_t, lat0, lon0 = bikes.pop(bike_id)
                        standing_times.append(
                            (bike_id, start_t, last_seen_t, lat0, lon0)
                        )
                        missing.pop(bike_id, None)

            # flushing remaining bikes (choose your semantics for "open" intervals)
            for bike_id, (start_t, last_seen_t, lat0, lon0) in bikes.items():
                # standing_times.append((bike_id, start_t, last_seen_t, lat0, lon0))
                ...

            # Todo: figure out, why sometimes one end time is the same as the next start time

        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            iterable.close()

    return standing_times


BikeRecord = Tuple[str, datetime.datetime, datetime.datetime, float, float]


def insert_bike_records(db_url: str, records: Iterable[BikeRecord]) -> None:
    """
    Insert multiple bike parking records into the PostgreSQL table using a URL
    like: postgresql://localhost:5432/nextbike
    """
    # psycopg handles database URLs natively
    with psycopg.connect(db_url) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.executemany(
                """
                INSERT INTO bike_parking (
                    bike_id, start_time, end_time, latitude, longitude
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                records,
            )
        conn.commit()


if __name__ == "__main__":
    # print(haversine(51.333049, 12.38122, 51.332009, 12.382188))
    # quit()
    since = datetime.datetime.strptime(
        "2025-11-01T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    until = datetime.datetime.strptime(
        "2025-12-01T10-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    country = "nextbike Leipzig"

    trips = get_standing_times(since, until, country)
    insert_bike_records("postgresql://localhost:5432/nextbike", trips)
