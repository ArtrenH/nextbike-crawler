import datetime
import os
from calendar import c
from concurrent.futures.process import ProcessPoolExecutor
from typing import Iterable

import psycopg
from dotenv import load_dotenv
from psycopg.rows import tuple_row
from tqdm import tqdm
from upath import UPath

from jsoncache import *
from jsoncache.build.lib.jsoncache import JsonStore

from .helpers import (
    BikeRecord,
    CityRecord,
    CountryRecord,
    PlaceRecord,
    get_city_dict,
    get_country_dict,
    is_bike_code,
)

load_dotenv()

DATABASE_URL = "postgresql://localhost:5432/nextbike"


def insert_countries(db_url: str = DATABASE_URL, records: Iterable[CountryRecord] = []):
    with psycopg.connect(db_url) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.executemany(
                """
                INSERT INTO countries (
                    local_id, timezone, country, country_name, lat, lng
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (local_id)
                DO UPDATE SET
                    timezone = EXCLUDED.timezone,
                    country = EXCLUDED.country,
                    country_name = EXCLUDED.country_name,
                    lat = EXCLUDED.lat,
                    lng = EXCLUDED.lng
                """,
                records,
            )
        conn.commit()


def extract_countries(
    since: datetime.datetime,
    until: datetime.datetime,
    country_whitelist: list | None = None,
):
    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")), preprocessors=[]
    )

    with ProcessPoolExecutor(1) as executor:
        iterable = cache.iter_files(since=since, until=until, executor=executor)
        country_data = {}

        try:
            for timestamp, data in tqdm(
                iterable,
                smoothing=0,
                total=round((until - since).total_seconds()) // 10,
            ):
                for country in data["countries"].values():
                    if country_whitelist and country["name"] not in country_whitelist:
                        continue
                    country_data[country["name"]] = (
                        country["name"],
                        country["timezone"],
                        country["country"],
                        country["country_name"],
                        country["lat"],
                        country["lng"],
                    )
            insert_countries(records=country_data.values())

        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            iterable.close()


UPSERT_CITY = """
INSERT INTO cities (
    country_id, uid, name, alias,
    lat, lng, lat_min, lng_min, lat_max, lng_max
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (uid)
DO UPDATE SET
    name    = EXCLUDED.name,
    alias   = EXCLUDED.alias,
    lat     = EXCLUDED.lat,
    lng     = EXCLUDED.lng,
    lat_min = EXCLUDED.lat_min,
    lng_min = EXCLUDED.lng_min,
    lat_max = EXCLUDED.lat_max,
    lng_max = EXCLUDED.lng_max;
"""


def insert_cities(db_url: str = DATABASE_URL, records: Iterable[CityRecord] = []):
    with psycopg.connect(db_url) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.executemany(
                UPSERT_CITY,
                records,
            )
        conn.commit()


def extract_cities(
    since: datetime.datetime,
    until: datetime.datetime,
    country_whitelist: list | None = None,
):
    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")), preprocessors=[]
    )
    country_map = get_country_dict(DATABASE_URL)

    with ProcessPoolExecutor(1) as executor:
        iterable = cache.iter_files(since=since, until=until, executor=executor)

        try:
            city_data = {}
            for timestamp, data in tqdm(
                iterable,
                smoothing=0,
                total=round((until - since).total_seconds()) // 10,
            ):
                for country_local_id, country in data["countries"].items():
                    if country_whitelist and country_local_id not in country_whitelist:
                        continue
                    for city in country["cities"].values():
                        city_data[city["uid"]] = (
                            country_map[country_local_id],
                            city["uid"],
                            city["name"],
                            city["alias"],
                            city["lat"],
                            city["lng"],
                            city["bounds"]["south_west"]["lat"],
                            city["bounds"]["south_west"]["lng"],
                            city["bounds"]["north_east"]["lat"],
                            city["bounds"]["north_east"]["lng"],
                        )

            insert_cities(records=city_data.values())

        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            iterable.close()


UPSERT_AND_LINK_PLACE_MANY_CITIES = """
WITH upserted AS (
  INSERT INTO places (uid, name, lat, lng)
  VALUES (%s, %s, %s, %s)
  ON CONFLICT (uid) DO UPDATE SET
    name = EXCLUDED.name,
    lat  = EXCLUDED.lat,
    lng  = EXCLUDED.lng
  RETURNING id
)
INSERT INTO city_places (city_id, place_id)
SELECT unnest(%s::bigint[]), id
FROM upserted
ON CONFLICT DO NOTHING;
"""


def insert_places(
    db_url: str = DATABASE_URL,
    records: Iterable[PlaceRecord] = (),
    place_city_map: dict | None = None,
):
    if place_city_map is None:
        place_city_map = {}
    # records are (city_id, uid, name, lat, lng)
    with psycopg.connect(db_url) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            params = [
                (uid, name, lat, lng, sorted(place_city_map.get(uid, ())))
                for (uid, name, lat, lng) in records
            ]
            cur.executemany(UPSERT_AND_LINK_PLACE_MANY_CITIES, params)
        conn.commit()


def extract_places(
    since: datetime.datetime,
    until: datetime.datetime,
    country_whitelist: list | None = None,
):
    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")), preprocessors=[]
    )
    city_map = get_city_dict(DATABASE_URL)
    place_data = {}
    place_city_data = {}

    with ProcessPoolExecutor(1) as executor:
        iterable = cache.iter_files(since=since, until=until, executor=executor)

        try:
            for timestamp, data in tqdm(
                iterable,
                smoothing=0,
                total=round((until - since).total_seconds()) // 10,
            ):
                for country_local_id, country in data["countries"].items():
                    if country_whitelist and country_local_id not in country_whitelist:
                        continue
                    for city_uid, city in country["cities"].items():
                        for place in city["places"].values():
                            if is_bike_code(place["name"]):
                                continue
                            place_data[place["uid"]] = (
                                place["uid"],
                                place["name"],
                                place["lat"],
                                place["lng"],
                            )
                            if place["uid"] not in place_city_data:
                                place_city_data[place["uid"]] = set()

                            place_city_data[place["uid"]].add(city_map[city_uid])

            insert_places(records=place_data.values(), place_city_map=place_city_data)

        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            iterable.close()


UPSERT_BIKE = """
INSERT INTO bikes (local_id, bike_type)
VALUES (%s, %s)
ON CONFLICT (local_id) DO UPDATE SET
    bike_type = EXCLUDED.bike_type
RETURNING id
"""


def insert_bikes(
    db_url: str = DATABASE_URL,
    records: Iterable[BikeRecord] = (),
):
    with psycopg.connect(db_url) as conn:
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.executemany(UPSERT_BIKE, records)
        conn.commit()


def extract_bikes(
    since: datetime.datetime,
    until: datetime.datetime,
    country_whitelist: list | None = None,
):
    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")), preprocessors=[]
    )

    bike_data = {}

    with ProcessPoolExecutor(1) as executor:
        iterable = cache.iter_files(since=since, until=until, executor=executor)
        try:
            total = -1
            for timestamp, data in tqdm(
                iterable,
                smoothing=0,
                total=round((until - since).total_seconds()) // 10,
            ):
                total += 1
                if total % 30 != 0:
                    continue
                for country_local_id, country in data["countries"].items():
                    if country_whitelist and country_local_id not in country_whitelist:
                        continue
                    for _, city in country["cities"].items():
                        for place in city["places"].values():
                            for bike in place["bike_list"].values():
                                bike_data[bike["number"]] = (
                                    bike["number"],
                                    str(bike["bike_type"]),
                                )

            insert_bikes(records=bike_data.values())

        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            iterable.close()


if __name__ == "__main__":
    # print(haversine(51.333049, 12.38122, 51.332009, 12.382188))
    # quit()
    since = datetime.datetime.strptime(
        "2025-11-01T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    until = datetime.datetime.strptime(
        "2025-12-01T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    for i in tqdm(range(1)):
        # extract_countries(since, until)
        # extract_cities(since, until)
        # extract_places(since, until)
        extract_bikes(
            since,
            until,
            [
                "nextbike Leipzig",
                "nextbike Berlin",
                "welo",  # Bonn
                "Bre.Bike",
            ],
        )
    quit()

    since = datetime.datetime.strptime(
        "2025-11-01T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    until = datetime.datetime.strptime(
        "2025-11-01T00-30-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    for i in tqdm(range(31 * 4)):
        extract_bikes(since, until)
        since += datetime.timedelta(hours=6)
        until += datetime.timedelta(days=6)
