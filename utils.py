import os
import datetime
from pathlib import Path

from upath import UPath

from jsoncache import *
from dotenv import load_dotenv

load_dotenv()


def decompress():
    ...


def main():
    rules = [
        ("$.countries[*].cities[*].places[*].bike_list", "number"),
        ("$.countries[*].cities[*].places", "uid"),
        ("$.countries[*].cities", "uid"),
        ("$.countries", "name"),
    ]

    cache = JsonStore(
        data_store=UPath(os.getenv("CACHE_FS_URL_ANALYZER")),
        preprocessors=[]
    )

    for timestamp in sorted(cache.iter_base_files_timestamps()):
        print(f" - {timestamp!s}")

    t = datetime.datetime.fromisoformat("2025-04-12 12:35:00+00:00")
    # cache.decompress(
    #     t
    # )

    # for timestamp, data in cache.iter_archive(t):
    #     print(timestamp)
        # print(data)

    for timestamp, data in cache.iter_files(
        since=datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
        until=datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
    ):
        print(timestamp)
        # print(data)

    # for file in

    # decompress()


if __name__ == "__main__":
    main()
