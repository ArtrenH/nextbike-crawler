import datetime
import json
import os

import networkx as nx
import osmnx as ox
import pandas as pd
from pyproj import Transformer
from shapely.geometry import LineString
from shapely.ops import linemerge
from tqdm import tqdm

from .helpers import query_df


def get_trips(city_uid, start_time, end_time):
    return query_df(
        "postgresql://localhost:5432/nextbike",
        """
        SELECT
            id,
            start_time,
            end_time,
            start_latitude,
            start_longitude,
            end_latitude,
            end_longitude
        FROM
            bike_trips
        WHERE
            (start_city_uid = %s OR end_city_uid = %s)
            AND ((start_time >= %s AND start_time <= %s) OR (end_time >= %s AND end_time <= %s))
        ORDER BY
            start_time
        """,
        params=(city_uid, city_uid, start_time, end_time, start_time, end_time),
    )


def _route_linestring_from_edges(route_edges):
    geoms = [g for g in route_edges.geometry if g is not None]
    if not geoms:
        return None
    merged = linemerge(geoms)
    if merged.geom_type == "LineString":
        return merged
    if merged.geom_type == "MultiLineString":
        return max(merged.geoms, key=lambda g: g.length)
    return None


def _stationary_series(start_time, end_time, lon, lat):
    ts = pd.date_range(start=start_time, end=end_time, freq="10s")
    return {
        t.isoformat(): {
            "longitude": float(lon),
            "latitude": float(lat),
            "fraction": 0.0,
            "distance_m": 0.0,
        }
        for t in ts
    }


def interpolate_trips(trips: pd.DataFrame):
    ox.settings.log_console = False
    os.makedirs("cache", exist_ok=True)

    graph_path = "cache/leipzig_bike_projected.graphml"
    if os.path.exists(graph_path):
        G = ox.load_graphml(graph_path)
    else:
        G0 = ox.graph_from_place("Leipzig, Saxony, Germany", network_type="bike")
        G = ox.project_graph(G0)
        ox.save_graphml(G, graph_path)

    graph_crs = G.graph["crs"]
    to_graph = Transformer.from_crs("EPSG:4326", graph_crs, always_xy=True)
    to_wgs84 = Transformer.from_crs(graph_crs, "EPSG:4326", always_xy=True)

    interpolations = {}

    for _, trip in tqdm(trips.iterrows(), total=len(trips)):
        trip_id = trip["id"]
        start_time = pd.to_datetime(trip["start_time"])
        end_time = pd.to_datetime(trip["end_time"])

        total_seconds = (end_time - start_time).total_seconds()
        if total_seconds <= 0:
            # no valid time window: return a single-point series (or empty)
            interpolations[trip_id] = _stationary_series(
                start_time, start_time, trip["start_longitude"], trip["start_latitude"]
            )
            continue

        # project start/end to graph CRS
        sx, sy = to_graph.transform(trip["start_longitude"], trip["start_latitude"])
        ex, ey = to_graph.transform(trip["end_longitude"], trip["end_latitude"])

        start_node = ox.distance.nearest_nodes(G, X=sx, Y=sy)
        end_node = ox.distance.nearest_nodes(G, X=ex, Y=ey)

        # Stationary shortcut: same snapped node -> no edges -> keep fixed position
        if start_node == end_node:
            interpolations[trip_id] = _stationary_series(
                start_time, end_time, trip["start_longitude"], trip["start_latitude"]
            )
            continue

        # Try to compute a route; if impossible, treat as stationary at start
        try:
            route_nodes = nx.shortest_path(G, start_node, end_node, weight="length")
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            interpolations[trip_id] = _stationary_series(
                start_time, end_time, trip["start_longitude"], trip["start_latitude"]
            )
            continue

        if len(route_nodes) < 2:
            interpolations[trip_id] = _stationary_series(
                start_time, end_time, trip["start_longitude"], trip["start_latitude"]
            )
            continue

        # Now safe to call route_to_gdf
        route_edges = ox.routing.route_to_gdf(G, route_nodes)
        route_line = _route_linestring_from_edges(route_edges)

        # If geometry missing/degenerate, fall back to stationary
        if route_line is None or route_line.length == 0:
            interpolations[trip_id] = _stationary_series(
                start_time, end_time, trip["start_longitude"], trip["start_latitude"]
            )
            continue

        route_length_m = float(route_line.length)
        ts = pd.date_range(start=start_time, end=end_time, freq="10s")

        per_trip = {}
        for t in ts:
            elapsed = (t - start_time).total_seconds()
            frac = min(max(elapsed / total_seconds, 0.0), 1.0)
            dist = frac * route_length_m
            pt = route_line.interpolate(dist)
            lon, lat = to_wgs84.transform(pt.x, pt.y)

            per_trip[t.isoformat()] = {
                "longitude": float(lon),
                "latitude": float(lat),
                "fraction": float(frac),
                "distance_m": float(dist),
            }

        interpolations[trip_id] = per_trip

    return interpolations


def main():
    since = datetime.datetime.strptime(
        "2025-11-01T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    until = datetime.datetime.strptime(
        "2025-11-02T00-00-00", "%Y-%m-%dT%H-%M-%S"
    ).replace(tzinfo=datetime.timezone.utc)

    trips = get_trips("1", since, until)
    interpolations = interpolate_trips(trips)
    with open("results/interpolations.json", "w+") as f:
        json.dump(interpolations, f)


if __name__ == "__main__":
    main()
