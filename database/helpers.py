import re
from typing import Tuple

import pandas as pd

pattern = re.compile(r"^BIKE \d+$")


def is_bike_code(s: str) -> bool:
    return bool(pattern.match(s))


# local_id, timezone, country, country_name, lat, lng
CountryRecord = Tuple[str, str, str, str, float, float]
# country_id, uid, name, alias, lat, lng, lat_min, lng_min, lat_max, lng_max
CityRecord = Tuple[int, str, str, str, float, float, float, float, float, float]
# uid, name, lat, lng
PlaceRecord = Tuple[int, str, float, float]
# local_id, bike_type, city_id
BikeRecord = Tuple[str, str]


def query_df(db_url: str, sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql_query(sql, db_url, params=params)


def get_city_dict(db_url: str) -> dict[str, int]:
    return (
        query_df(
            db_url=db_url,
            sql="""
                SELECT id, uid
                FROM cities
            """,
        )
        .set_index("uid")["id"]
        .to_dict()
    )


def get_country_dict(db_url: str) -> dict[str, int]:
    return (
        query_df(
            db_url=db_url,
            sql="""
                SELECT id, local_id
                FROM countries
            """,
        )
        .set_index("local_id")["id"]
        .to_dict()
    )


def get_country_city_dict(db_url: str):
    country_dict = (
        query_df(
            db_url=db_url,
            sql="""
                SELECT id, local_id
                FROM countries
            """,
        )
        .set_index("local_id")["id"]
        .to_dict()
    )
    city_list = query_df(
        db_url=db_url,
        sql=""" SELECT id, uid, country_id FROM cities """,
    ).to_dict("records")
    return {
        k: {city["uid"]: city["id"] for city in city_list if city["country_id"] == v}
        for k, v in country_dict.items()
    }
