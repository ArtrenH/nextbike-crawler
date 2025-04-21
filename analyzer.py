import dataclasses
import datetime
import itertools
import logging
import os
import pickle
import typing, math
from collections import defaultdict
from concurrent.futures.process import ProcessPoolExecutor
from pathlib import Path

import folium
import numpy as np
from folium.plugins import HeatMapWithTime
from tqdm import tqdm
from dotenv import load_dotenv
from upath import UPath

import pipifax_io.cache
import pipifax_io.serializable
import pipifax_io.dyn_codegen

load_dotenv()

from jsoncache import *


def get_bike_locations(
    data,
    country_whitelist: typing.Container[str] | None = None
) -> dict[str, dict[str, list[float]]]:
    bike_locations = defaultdict(dict)

    for country_id, country in data['countries'].items():
        if country_whitelist and country_id not in country_whitelist:
            continue

        for city in country["cities"].values():
            for place in city["places"].values():
                for bike in place['bike_list'].values():
                    bike_locations[country_id][bike['number']] = [place['lat'], place['lng']]

    return bike_locations


@dataclasses.dataclass(frozen=True, slots=True)
class Trip(pipifax_io.serializable.SimpleSerializable):
    lat1: float
    lon1: float
    lat2: float
    lon2: float
    begin: datetime.datetime
    end: datetime.datetime
    bike_nr: str

    def duration(self) -> datetime.timedelta:
        return self.end - self.begin


# @pipifax_io.cache.cache(
#     key=pipifax_io.cache.make_key_func(
#         ext=".pickle"
#     ),
#     serialize=pickle.dumps,
#     deserialize=pickle.loads,
# )
@pipifax_io.cache.cache_auto(
    version=5
)
def get_trips(
    since: datetime.datetime,
    until: datetime.datetime,
    country: str
) -> list[Trip]:
    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")),
        preprocessors=[]
    )

    with ProcessPoolExecutor(11) as executor:
        iterable = cache.iter_files(
            since=since,
            until=until,
            executor=executor
        )
        bikes: dict[str, tuple[int, float, float]] = {}
        timestamps = []
        trips = []

        try:
            for i, (timestamp, data) in tqdm(enumerate(iterable), total=len(iterable), smoothing=0):
                country_bikes = get_bike_locations(data, {country})[country]

                timestamps.append(timestamp)

                for bike_nr, (lat, lon) in country_bikes.items():
                    if bike_nr in bikes:
                        prev_i, lat1, lon1 = bikes[bike_nr]
                        if i - prev_i > 1:
                            trips.append(
                                Trip(
                                    lat1=lat1,
                                    lon1=lon1,
                                    lat2=lat,
                                    lon2=lon,
                                    begin=timestamps[prev_i],
                                    end=timestamp,
                                    bike_nr=bike_nr
                                )
                            )
                            # print(trips[-1])

                    bikes[bike_nr] = (i, lat, lon)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            iterable.close()

    return trips


def main():
    # os.nice(30)

    logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] [%(levelname)8s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S", force=True)
    logging.getLogger("fsspec").setLevel(logging.WARN)

    data = get_trips(
        country="nextbike Leipzig",

        # since=datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
        # until=datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
        since=datetime.datetime.strptime("2025-04-17T00-00-00", "%Y-%m-%dT%H-%M-%S").replace(
            tzinfo=datetime.timezone.utc),
        until=datetime.datetime.strptime("2025-04-19T00-00-00", "%Y-%m-%dT%H-%M-%S").replace(
            tzinfo=datetime.timezone.utc),

        # __disable_cache_lookup=True
    )

    # print(len(data))

    # out(data)
    # return

    m = generate_heatmap(data)
    m.save("animated_heatmap.html")

if __name__ == "__main__":
    with pipifax_io.cache.set_simple_cache(
        pipifax_io.cache.FileSystemCache(
            Path(".pipifax-cache")
        )
    ):
        main()
