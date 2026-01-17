from __future__ import annotations

import folium
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from folium import Popup
from folium.plugins import MarkerCluster
from sqlalchemy import create_engine
from tqdm import tqdm
from dotenv import load_dotenv
import os

from .helpers import haversine, query_df

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def plot_cities():
    df = query_df(
        DATABASE_URL,
        """
        SELECT name, lat, lng, lat_min, lng_min, lat_max, lng_max
        FROM cities
        """,
    )
    m = folium.Map(
        location=[df["lat"].mean(), df["lng"].mean()],
        zoom_start=6,
        tiles="OpenStreetMap",
    )

    # Optional: group markers if you have many points
    cluster = MarkerCluster().add_to(m)

    for _, r in df.iterrows():
        # Rectangle bounds are [[southWest], [northEast]] = [[lat_min, lng_min], [lat_max, lng_max]]
        bounds = [[r["lat_min"], r["lng_min"]], [r["lat_max"], r["lng_max"]]]

        folium.Rectangle(
            bounds=bounds,
            color="blue",
            weight=2,
            fill=True,
            fill_opacity=0.15,
            tooltip=r["name"],
        ).add_to(m)

        folium.Marker(
            location=[
                r["lat_min"] / 2 + r["lat_max"] / 2,
                r["lng_min"] / 2 + r["lng_max"] / 2,
            ],
            tooltip=r["name"],  # hover label
            popup=Popup(r["name"], max_width=250),  # click label
            icon=folium.Icon(color="red", icon="info-sign"),
        ).add_to(cluster)

    m.save("results/cities.html")


def plot_trip_speeds(city_uid):
    df = query_df(
        DATABASE_URL,
        """
        SELECT id, start_time, end_time, start_latitude, start_longitude, end_latitude, end_longitude
        FROM bike_trips
        WHERE (start_city_uid = %s OR end_city_uid = %s)
        -- AND end_place_uid IS NULL
        """,
        params=(city_uid, city_uid),
    )
    df["distance"] = df.apply(
        lambda row: haversine(
            row["start_latitude"],
            row["start_longitude"],
            row["end_latitude"],
            row["end_longitude"],
        ),
        axis=1,
    )
    df["time_seconds"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
    df["speed"] = df["distance"] / df["time_seconds"]
    # plot speed distribution
    # df = df[df["speed"] > 0.1]
    # df["speed"].hist(bins=100, figsize=(10, 5))
    df.plot.scatter(x="time_seconds", y="distance", s=0.5, alpha=0.7)
    plt.title("Trip Speed Distribution")
    plt.xlabel("Time (s)")
    plt.ylabel("Distance (m)")
    plt.show()


if __name__ == "__main__":
    # plot_cities()
    plot_trip_speeds("1")
