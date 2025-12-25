from __future__ import annotations

import json

import folium
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from folium.plugins import HeatMap, HeatMapWithTime
from tqdm import tqdm

from helpers import query_df


def get_standing_times():
    return query_df(
        "postgresql://localhost:5432/nextbike",
        """
        SELECT start_time, end_time
        FROM bike_parking
        WHERE end_time IS NOT NULL
        ORDER BY start_time DESC
        """,
    )


def get_bike(bike_local_id):
    return query_df(
        "postgresql://localhost:5432/nextbike",
        """
        SELECT bike_local_id, start_time, end_time, latitude, longitude
        FROM bike_parking
        WHERE bike_local_id = %s
        ORDER BY start_time DESC
        """,
        params=(bike_local_id,),
    )


def get_standing_coords(time, city_uid: str | None = None):
    return query_df(
        "postgresql://localhost:5432/nextbike",
        """
        SELECT latitude, longitude
        FROM bike_parking
        WHERE start_time <= %s
          AND end_time   >= %s
          AND (
                %s IS NULL
                OR city_uid = %s
          );
        """,
        params=(time, time, city_uid, city_uid),
    )


# linear interpolation of the trips
def get_moving_coords(time, city_uid: str | None = None):
    return query_df(
        "postgresql://localhost:5432/nextbike",
        """
        SELECT
            bike_local_id,
            (start_latitude + (
                EXTRACT(EPOCH FROM (%s - start_time))
                / NULLIF(EXTRACT(EPOCH FROM (end_time - start_time)), 0)
            ) * (end_latitude - start_latitude)) AS latitude,
            (start_longitude + (
                EXTRACT(EPOCH FROM (%s - start_time))
                / NULLIF(EXTRACT(EPOCH FROM (end_time - start_time)), 0)
            ) * (end_longitude - start_longitude)) AS longitude
        FROM bike_trips
        WHERE start_time < %s
          AND end_time   > %s
          AND end_time   > start_time
          AND (
                %s IS NULL
                OR start_city_uid = %s
                OR end_city_uid   = %s
          );
        """,
        params=(time, time, time, time, city_uid, city_uid, city_uid),
    )


def get_moving_coords_alt(interpolations, time):
    result = []
    for interpolation in interpolations.values():
        if time.isoformat() in interpolation:
            result.append(
                [
                    interpolation[time.isoformat()]["latitude"],
                    interpolation[time.isoformat()]["longitude"],
                ]
            )
    return result


def bike_track_map(df: pd.DataFrame, start_zoom: int = 13) -> folium.Map:
    # Ensure datetimes and correct ordering
    df = df.copy()
    df["start_time"] = pd.to_datetime(df["start_time"], utc=False, errors="coerce")
    df = df.sort_values("start_time").reset_index(drop=True)

    # Drop rows with missing coords
    df = df.dropna(subset=["latitude", "longitude"])
    if df.empty:
        raise ValueError("No valid coordinates to plot.")

    # Center map on first point (or mean)
    center = [df.loc[0, "latitude"], df.loc[0, "longitude"]]
    m = folium.Map(location=center, zoom_start=start_zoom, control_scale=True)

    # Build list of lat/lon pairs for the line
    points = df[["latitude", "longitude"]].to_numpy().tolist()

    # Line between consecutive points (single polyline)
    folium.PolyLine(points, weight=3, opacity=0.8).add_to(m)

    # Numbered markers
    for i, row in df.iterrows():
        lat, lon = float(row["latitude"]), float(row["longitude"])
        label = str(i + 1)

        # A DivIcon gives you a marker with a number on it
        icon = folium.DivIcon(
            html=f"""
            <div style="
                background: white;
                border: 2px solid black;
                border-radius: 12px;
                width: 24px;
                height: 24px;
                line-height: 20px;
                text-align: center;
                font-size: 12px;
                font-weight: 600;
            ">{label}</div>
            """
        )

        popup = (
            f"#{label}<br>"
            f"start: {row['start_time']}<br>"
            f"end: {row.get('end_time', '')}<br>"
            f"lat/lon: {lat:.6f}, {lon:.6f}"
        )

        folium.Marker(
            location=[lat, lon],
            icon=icon,
            popup=folium.Popup(popup, max_width=300),
            tooltip=f"{label}",
        ).add_to(m)

    return m


def heatmap(name: str, city_uid: str | None = None):
    day = pd.Timestamp("2025-11-01T01")
    times = pd.date_range(
        start=day,
        end=day + pd.Timedelta(days=1),
        freq="10s",
        inclusive="left",
    )

    labels = []
    frames = []

    with open("interpolations.json", "r") as f:
        interpolations = json.load(f)

    for t in tqdm(times):
        df_t = get_standing_coords(t, city_uid)
        # df_t2 = get_moving_coords(t, city_uid)
        coords2 = get_moving_coords_alt(interpolations, t)

        coords1 = df_t[["latitude", "longitude"]].dropna().to_numpy().tolist()
        # coords2 = df_t2[["latitude", "longitude"]].dropna().to_numpy().tolist()
        frames.append(coords1 + coords2)
        labels.append(t.strftime("%Y-%m-%d %H:%M:%S"))

    m = folium.Map(location=[51.34, 12.38], zoom_start=12, tiles="Cartodb Positron")

    HeatMapWithTime(
        data=frames,
        index=labels,
        radius=10,
        blur=1,
        max_opacity=0.8,
        auto_play=True,
        min_speed=10,
        max_speed=100,
    ).add_to(m)

    m.save(f"results/bike_heatmap_{name}.html")


def availability_chart():
    day = pd.Timestamp("2025-11-02")
    times = pd.date_range(
        start=day,
        end=day + pd.Timedelta(days=26),
        freq="120s",
        inclusive="left",
    )

    labels = []
    available_bikes = []

    for t in tqdm(times):
        df_t = get_standing_coords(t)

        coords = df_t[["latitude", "longitude"]].dropna().to_numpy().tolist()
        labels.append(t)
        available_bikes.append(len(coords))

    fig, ax = plt.subplots(figsize=(14, 6))

    ax.plot(labels, available_bikes, linewidth=1)
    ax.grid(True, which="major", linestyle="--", alpha=0.4)
    ax.grid(True, which="minor", linestyle=":", alpha=0.2)

    ax.set_xlabel("Time")
    ax.set_ylabel("Available Bikes")
    ax.set_title("Bike Availability (1-minute resolution)")

    # Major ticks: one per day
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

    # Minor ticks: every 6 hours
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=6))

    fig.autofmt_xdate()  # rotates + aligns labels
    plt.tight_layout()
    plt.savefig("bike_availability_chart.png", dpi=150)


def visualize_standing_times():
    times = get_standing_times()
    durations = times["end_time"] - times["start_time"]
    durations = durations.dt.total_seconds() / 60

    fig, ax = plt.subplots(figsize=(14, 6))

    ax.hist(durations, bins=100, density=True, alpha=0.7)
    ax.grid(True, which="major", linestyle="--", alpha=0.4)
    ax.grid(True, which="minor", linestyle=":", alpha=0.2)

    ax.set_xlabel("Duration (minutes)")
    ax.set_ylabel("Density")
    ax.set_title("Distribution of Standing Times")

    plt.tight_layout()
    plt.savefig("standing_times_histogram.png", dpi=150)


def location_tracker(
    name: str, lat_min: float, lon_min: float, lat_max: float, lon_max: float
):
    coords1 = (
        query_df(
            "postgresql://localhost:5432/nextbike",
            """
        SELECT end_latitude, end_longitude
        FROM bike_trips
        WHERE %s <= start_latitude
          AND start_latitude <= %s
          AND %s <= start_longitude
          AND start_longitude <= %s
        """,
            params=(lat_min, lat_max, lon_min, lon_max),
        )[["end_latitude", "end_longitude"]]
        .dropna()
        .to_numpy()
        .tolist()
    )

    coords2 = (
        query_df(
            "postgresql://localhost:5432/nextbike",
            """
        SELECT start_latitude, start_longitude
        FROM bike_trips
        WHERE %s <= end_latitude
          AND end_latitude <= %s
          AND %s <= end_longitude
          AND end_longitude <= %s
        """,
            params=(lat_min, lat_max, lon_min, lon_max),
        )[["start_latitude", "start_longitude"]]
        .dropna()
        .to_numpy()
        .tolist()
    )

    m = folium.Map(location=[51.34, 12.38], zoom_start=12)

    HeatMap(
        data=coords1 + coords2,
        radius=5,
        blur=1,
        max_opacity=0.8,
        auto_play=True,
        min_speed=10,
        max_speed=100,
    ).add_to(m)

    m.save(f"results/{name}.html")

    return coords1, coords2


if __name__ == "__main__":
    # location_tracker("albertina", 51.331864, 12.365781, 51.333317, 12.369291)
    # location_tracker("bnd", 52.531669, 13.374053, 52.536062, 13.379980)
    # location_tracker("campus_poppelsdorf", 50.724548, 7.082587, 50.729243, 7.089389)
    # location_tracker("uni_bremen", 53.10440496644197, 8.847620416735856, 53.10945481241409, 8.864020481447133)
    location_tracker("test", 0, 0, 90, 90)
    # heatmap("Bremen", "379")
    # heatmap("Leipzig_interpolated", "1")
    # heatmap("Leipzig", "1")
    # heatmap("Bonn", "1170")
    # heatmap("Berlin", "362")
