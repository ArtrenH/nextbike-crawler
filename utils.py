import os
from datetime import datetime
from pathlib import Path

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

    cache = Cache(
        file_path=Path(os.getenv("CACHE_PATH", "cache")),
        executor=None,
        data_store_factory=(
            FsStore.from_url,
            (os.getenv("CACHE_FS_URL"),)
        ),
        base_file_creation_interval=int(os.getenv("BASE_FILE_CREATION_INTERVAL", 30 * 60)),
        preprocessors=[
            JsonDictionarizer(rules)
        ]
    )

    for timestamp in sorted(cache.iter_base_files_timestamps()):
        print(f" - {timestamp!s}")

    t = datetime.fromisoformat("2025-04-12 12:35:00+00:00")
    # cache.decompress(
    #     t
    # )

    for timestamp, data in cache.iter_archive(t):
        print(timestamp)
        # print(data)

    # for file in

    # decompress()


if __name__ == "__main__":
    main()
