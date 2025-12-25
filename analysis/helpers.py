import math

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg://localhost:5432/nextbike")


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


def query_df(db_url: str, sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql_query(sql, db_url, params=params)
