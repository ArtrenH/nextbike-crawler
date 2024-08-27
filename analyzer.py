import dataclasses
import datetime
import itertools
import json
from collections import defaultdict
from pathlib import Path

import folium
from folium.plugins import HeatMap
from folium.plugins import HeatMapWithTime
from tqdm import tqdm

from cache import *


@dataclasses.dataclass
class DataAnalyzer:
    timestamp: datetime.datetime | None = None
    # cache: Cache | None = None
    data: dict | None = None
    bike_locations: dict = dataclasses.field(default_factory=dict)

    def load_data(self):
        ...

    def read_data(self):
        with open("nextbike-live.json", "r") as file:
            self.data = json.load(file)

    @staticmethod
    def get_bike_locations(data) -> dict[str, dict[str, list[float]]]:
        bike_locations = {}
        for country in data['countries']:
            country_id = country['name']
            if bike_locations.get(country_id) is None:
                bike_locations[country_id] = {}
            for city in country["cities"]:
                for place in city["places"]:
                    lat, lon = place['lat'], place['lng']
                    for bike in place['bike_list']:
                        bike_locations[country_id][bike['number']] = [lat, lon]
        return bike_locations

    def save_bike_locations(self):
        with open("bike_locations.json", "w") as file:
            json.dump(self.bike_locations, file)

    def plot_locations(self, city: str):
        markers = list(self.bike_locations[city].keys())
        locations = list(self.bike_locations[city].values())
        # make a folium map
        center_lat = sum(loc[0] for loc in locations) / len(locations)
        center_lon = sum(loc[1] for loc in locations) / len(locations)
        m = folium.Map(location=[center_lat, center_lon], zoom_start=15)
        # for location, marker_text in zip(locations, markers):
        #     folium.Marker(
        #         location=location,
        #         popup=folium.Popup(marker_text, parse_html=True),
        #         icon=folium.Icon(color='red', icon='info-sign')
        #     ).add_to(m)

        HeatMap(
            locations,
            radius=7.5,
            min_opacity=1
        ).add_to(m)

        # Save the map to an HTML file
        m.save("locations_map.html")

    def animate_locations(self):
        cache = Cache(
            file_path=Path("cache"),
            executor=None,
            data_store=FileSystemStore(Path("cache/data"))
        )

        iterable = cache.iter_archive(datetime.datetime.strptime("2024-08-26T17-30-00", "%Y-%m-%dT%H-%M-%S").replace(tzinfo=datetime.timezone.utc))
        # iterable = cache.iter_files(since=datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), until=datetime.datetime.max.replace(tzinfo=datetime.timezone.utc))
        timestamps = []

        bikes = defaultdict(list)
        for i, (timestamp, data) in tqdm(enumerate(iterable), total=180*4):
            country_bikes = self.get_bike_locations(data)['nextbike Leipzig']

            for bike_nr, pos in country_bikes.items():
                bikes[bike_nr].append((i, timestamp, pos))

            timestamps.append(timestamp)

            # if i == 10:
            #     break

        bikes_interpolated: dict[str, dict[int, list[float]]] = {}
        for bike_nr, bike_positions in bikes.items():
            new_positions: dict[int, list[float]] = {}
            for (i1, t1, p1), (i2, t2, p2) in itertools.pairwise(bike_positions):
                for di in range(0, i2 - i1 + 1):
                    px = p1[0] + di / (i2 - i1) * (p2[0] - p1[0])
                    py = p1[1] + di / (i2 - i1) * (p2[1] - p1[1])
                    new_positions[i1 + di] = [px, py]

            bikes_interpolated[bike_nr] = new_positions

        _plot_data: dict[int, list[list[float]]] = defaultdict(list)
        for bike_data in bikes_interpolated.values():
            for i, pos in bike_data.items():
                _plot_data[i].append(pos)

        plot_data = [_plot_data[i] for i in range(max(_plot_data))]
        timestamp_index = [timestamps[i].isoformat() for i in range(max(_plot_data))]

        m = folium.Map()
        HeatMapWithTime(plot_data, auto_play=True, max_opacity=0.8, index=timestamp_index).add_to(m)
        m.save("animated_heatmap.html")


def main():
    cache = Cache(
        file_path=Path("cache"),
        executor=None,
        data_store=FileSystemStore(Path("cache/data"))
    )
    # cache.decompress(datetime.datetime.strptime("2024-08-24T18-30-00", "%Y-%m-%dT%H-%M-%S").replace(tzinfo=datetime.timezone.utc))
    # return
    d = DataAnalyzer()
    d.animate_locations()
    return
    d.read_data()
    d.get_bike_locations()
    d.plot_locations("WK-Bike (Bremen)")
    # d.save_bike_locations()


if __name__ == "__main__":
    main()
