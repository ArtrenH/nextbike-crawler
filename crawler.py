from __future__ import annotations

import concurrent.futures
import datetime
import dataclasses
import time
from pathlib import Path

import requests

from cache import *


@dataclasses.dataclass
class NextBikeCrawler:
    cache: Cache
    base_timestamp: datetime.datetime | None = None
    wait_interval: int = 10

    def update(self, crawl_time: datetime.datetime, session: requests.Session):
        print("downloading...")
        response = session.get('https://maps.nextbike.net/maps/nextbike-live.json', headers={'Accept-Encoding': 'gzip'})
        self.cache.save_file(crawl_time, response.json())
        print("...done")

    def wait_until_next_update(self) -> datetime.datetime:
        next_time = (time.time() // self.wait_interval + 1) * self.wait_interval
        time.sleep(next_time - time.time())
        return datetime.datetime.fromtimestamp(next_time, datetime.timezone.utc)

    def crawl(self) -> None:
        session = requests.Session()

        while True:
            crawl_time = self.wait_until_next_update()
            self.update(crawl_time, session=session)
            # break


def main():
    cache = Cache(
        file_path=Path("cache"),
        executor=concurrent.futures.ProcessPoolExecutor(5),
        data_store=FileSystemStore(Path("cache/data"))
    )
    cache.init()

    crawler = NextBikeCrawler(cache)

    crawler.crawl()


if __name__ == "__main__":
    main()
