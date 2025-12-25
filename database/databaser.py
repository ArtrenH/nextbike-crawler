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

from database.helpers import get_single_key, is_bike_code
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


"""
Class representing a collection of bike positions with interruptions < MAX_GAP_SECONDS
"""


class BikePositionCollection:
    def __init__(
        self,
        bike_local_id: str,
        start_t: datetime.datetime,
        lat: float,
        lon: float,
        place_uid: str,
        city_uid: str,
    ):
        self.bike_local_id: str = bike_local_id
        self.start_t: datetime.datetime = start_t
        self.last_seen_t: datetime.datetime = start_t
        self.positions: dict[tuple[float, float], int] = {(lat, lon): 1}
        self.place_uids: dict[str, int] = {place_uid: 1}
        self.city_uids: dict[str, int] = {city_uid: 1}

    # update how often each position occured
    def update_position(
        self,
        t: datetime.datetime,
        lat: float,
        lon: float,
        place_uid: str,
        city_uid: str,
    ):
        self.last_seen_t = t
        if (lat, lon) not in self.positions:
            self.positions[(lat, lon)] = 0
        if place_uid not in self.place_uids:
            self.place_uids[place_uid] = 0
        if city_uid not in self.city_uids:
            self.city_uids[city_uid] = 0

        self.positions[(lat, lon)] += 1
        self.place_uids[place_uid] += 1
        self.city_uids[city_uid] += 1

    def compile_position_data(
        self,
    ) -> tuple[str | None, str | None, tuple[float, float]]:
        return (
            self.compile_city_uid(),
            self.compile_place_uid(),
            self.compile_position(),
        )

    def compile_city_uid(self) -> str | None:
        return get_single_key(self.city_uids)

    def compile_place_uid(self) -> str | None:
        return get_single_key(self.place_uids)

    # determine coordinate -> weighted average of >= 60% most occuring positions
    def compile_position(self) -> tuple[float, float]:
        items = sorted(self.positions.items(), key=lambda x: x[1], reverse=True)
        total = sum(w for _, w in items)
        selected = []
        acc = 0
        for (lat, lon), w in items:
            selected.append((lat, lon, w))
            acc += w
            if acc >= 0.6 * total:
                break
        lat = sum(lat * w for lat, _, w in selected) / sum(w for *_, w in selected)
        lon = sum(lon * w for _, lon, w in selected) / sum(w for *_, w in selected)
        return lat, lon

    def max_distance(self) -> float:
        max_distance = 0
        for pos1 in self.positions.keys():
            for pos2 in self.positions.keys():
                distance = haversine(
                    pos1[0],
                    pos1[1],
                    pos2[0],
                    pos2[1],
                )
                max_distance = max(max_distance, distance)
        return max_distance

    # compile the position collection into a standing time
    def compile_standing_time(self):
        final_position = self.compile_position()
        return (
            self.bike_local_id,
            self.start_t,
            self.last_seen_t,
            final_position[0],
            final_position[1],
            self.compile_place_uid(),
            self.compile_city_uid(),
        )


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
        bike_positions: dict[str, BikePositionCollection] = {}
        standing_times = []
        missing = {}

        # DIST_THRESH_M = 100
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
                for bike_local_id, (
                    bike_type,
                    lat,
                    lon,
                    place_uid,
                    city_uid,
                ) in country_bikes.items():
                    bikes[bike_local_id] = (bike_local_id, bike_type)
                    missing.pop(bike_local_id, None)

                    if bike_local_id not in bike_positions:
                        # (start_t, last_seen_t, lat, lon)
                        bike_positions[bike_local_id] = BikePositionCollection(
                            bike_local_id, timestamp, lat, lon, place_uid, city_uid
                        )
                        continue

                    bike_positions[bike_local_id].update_position(
                        timestamp, lat, lon, place_uid, city_uid
                    )

                # handle bikes not present now: start/continue missing timer
                to_delete = []

                for bike_local_id, bike_position_collection in bike_positions.items():
                    if bike_local_id in present_ids:
                        continue

                    if bike_local_id not in missing:
                        missing[bike_local_id] = timestamp
                        continue

                    first_missing_ts = missing[bike_local_id]
                    if (timestamp - first_missing_ts).total_seconds() > MAX_GAP_SECONDS:
                        standing_times.append(
                            bike_position_collection.compile_standing_time()
                        )
                        to_delete.append(bike_local_id)

                for bike_local_id in to_delete:
                    missing.pop(bike_local_id, None)
                    del bike_positions[bike_local_id]

            # flushing remaining bikes
            for bike_local_id, bike_position_collection in bike_positions.items():
                standing_times.append(bike_position_collection.compile_standing_time())

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
        # "nextbike Leipzig",
        # "Bre.Bike",
        # "welo",  # Bonn
        # "nextbike Berlin",
        # "nextbike BIH",  # Sarajevo
        # "Potsdam Rad",  # Potsdam
        # "Frelo",  # Freiburg
        # "MOBIbike",  # Dresden
        # "nextbike Frankfurt",  # Frankfurt
        # "KVV.nextbike",  # Karlsruhe
        # "KVB Rad",  # Köln
        "VRNnextbike",  # Heidelberg
        "VETURILO 3.0",  # Warszawa
        "WienMobil Rad",  # Wien
    ]
    for country in country_whitelist:
        print(f"finding standing times for {country:>40}")
        bikes, trips = get_standing_times(since, until, [country], False)
        insert_bikes("postgresql://localhost:5432/nextbike", bikes.values())
        insert_bike_records("postgresql://localhost:5432/nextbike", trips)


# TODO: implement pickup from DB
# TODO: implement consecutive save to DB without memory leak
# TODO: implement non-duplicates when running the code for a city again
if __name__ == "__main__":
    # print(haversine(51.333049, 12.38122, 51.332009, 12.382188))
    main()
