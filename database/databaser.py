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

from database.helpers import is_bike_code
from database.metadata_databaser import insert_bikes
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
) -> dict[str, dict[str, Tuple[str, float, float, str | None, str]]]:
    bike_locations = defaultdict(dict)

    for country_id, country in data["countries"].items():
        if country_whitelist and country_id not in country_whitelist:
            continue

        for city in country["cities"].values():
            for place in city["places"].values():
                for bike in place["bike_list"].values():
                    bike_locations[country_id][bike["number"]] = [
                        bike["bike_type"],
                        place["lat"],
                        place["lng"],
                        None if is_bike_code(place["name"]) else place["uid"],
                        city["uid"],
                    ]

    return bike_locations


def get_standing_times(
    since: datetime.datetime,
    until: datetime.datetime,
    country_whitelist: list[str],
    insert_continuously: bool = True,
):
    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")), preprocessors=[]
    )

    with ProcessPoolExecutor(1) as executor:
        iterable = cache.iter_files(since=since, until=until, executor=executor)
        bikes = {}
        bike_positions = {}
        standing_times = []
        missing = {}

        DIST_THRESH_M = 100
        MAX_GAP_SECONDS = 30  # allow up to 3 ticks missing (with 10s cadence)
        INSERT_THRESHOLD = 1000

        try:
            for timestamp, data in tqdm(
                iterable,
                smoothing=0,
                total=round((until - since).total_seconds()) // 10,
            ):
                if insert_continuously and len(standing_times) >= INSERT_THRESHOLD:
                    insert_bike_records(
                        "postgresql://localhost:5432/nextbike", standing_times
                    )
                    standing_times = []

                all_locations = get_bike_locations(data, country_whitelist)
                country_bikes = {}
                for country in country_whitelist:
                    country_bikes |= all_locations[country]
                present_ids = set(country_bikes.keys())

                # mark currently present bikes (and clear missing marker)
                for bike_id, (
                    bike_type,
                    lat,
                    lon,
                    place_uid,
                    city_uid,
                ) in country_bikes.items():
                    bikes[bike_id] = (bike_id, bike_type)
                    missing.pop(bike_id, None)

                    if bike_id not in bike_positions:
                        # (start_t, last_seen_t, lat, lon)
                        bike_positions[bike_id] = (
                            timestamp,
                            timestamp,
                            lat,
                            lon,
                            place_uid,
                            city_uid,
                        )
                        continue

                    start_t, last_seen_t, lat0, lon0, place_uid0, city_uid0 = (
                        bike_positions[bike_id]
                    )

                    if (
                        (city_uid == city_uid0)  # bike did not change city
                        and (place_uid == place_uid0)  # bike did not change place
                        and (
                            (
                                place_uid0 is not None
                            )  # place id set -> bike stayed at the same place
                            or haversine(lat, lon, lat0, lon0)
                            < DIST_THRESH_M  # bike did not move too far
                        )
                    ):
                        # still at same place: advance last_seen_t
                        bike_positions[bike_id] = (
                            start_t,
                            timestamp,
                            lat0,
                            lon0,
                            place_uid,
                            city_uid,
                        )
                        continue

                    # moved: close at last time we saw it at the old location
                    standing_times.append(
                        (
                            bike_id,
                            start_t,
                            last_seen_t,
                            lat0,
                            lon0,
                            place_uid0,
                            city_uid0,
                        )
                    )

                    # start new segment at the current observation time
                    bike_positions[bike_id] = (
                        timestamp,
                        timestamp,
                        lat,
                        lon,
                        place_uid,
                        city_uid,
                    )

                # handle bikes not present now: start/continue missing timer
                for bike_id in list(bike_positions.keys()):
                    if bike_id in present_ids:
                        continue

                    if bike_id not in missing:
                        missing[bike_id] = timestamp
                        continue

                    # if missing too long, close at the last time we actually saw it (not first_missing_ts)
                    first_missing_ts = missing[bike_id]
                    if (timestamp - first_missing_ts).total_seconds() > MAX_GAP_SECONDS:
                        start_t, last_seen_t, lat0, lon0, place_uid0, city_uid0 = (
                            bike_positions.pop(bike_id)
                        )
                        standing_times.append(
                            (
                                bike_id,
                                start_t,
                                last_seen_t,
                                lat0,
                                lon0,
                                place_uid0,
                                city_uid0,
                            )
                        )
                        missing.pop(bike_id, None)

            # flushing remaining bikes (choose your semantics for "open" intervals)
            for bike_id, (
                start_t,
                last_seen_t,
                lat0,
                lon0,
                place_uid0,
                city_uid0,
            ) in bike_positions.items():
                # standing_times.append((bike_id, start_t, last_seen_t, lat0, lon0, place_uid0))
                ...

            # Todo: figure out, why sometimes one end time is the same as the next start time

        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            iterable.close()

    if insert_continuously:
        insert_bike_records("postgresql://localhost:5432/nextbike", standing_times)
        return []
    return bikes, standing_times


BikeRecord = Tuple[
    str, datetime.datetime, datetime.datetime, float, float, str | None, str
]


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
                    bike_local_id, start_time, end_time, latitude, longitude, place_uid, city_uid
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                records,
            )
        conn.commit()


def main():
    since = datetime.datetime.strptime(
        "2025-11-01T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    until = datetime.datetime.strptime(
        "2025-12-01T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    country_whitelist = [
        "Bre.Bike",
        "welo",  # Bonn
        "nextbike Leipzig",
        "nextbike Berlin",
    ]
    for country in country_whitelist:
        bikes, trips = get_standing_times(since, until, [country], False)
        insert_bikes("postgresql://localhost:5432/nextbike", bikes.values())
        insert_bike_records("postgresql://localhost:5432/nextbike", trips)


if __name__ == "__main__":
    # print(haversine(51.333049, 12.38122, 51.332009, 12.382188))
    main()
