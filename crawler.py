from __future__ import annotations

import concurrent.futures
import logging
import os
import datetime
import dataclasses
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

from my_cache import *

load_dotenv()

log = logging.getLogger("NextBikeCrawler")


@dataclasses.dataclass
class NextBikeCrawler:
    cache: Cache
    base_timestamp: datetime.datetime | None = None
    wait_interval: int = 10

    def update(self, crawl_time: datetime.datetime, session: requests.Session):
        log.info(f"Updating data at {crawl_time}")
        response = session.get('https://maps.nextbike.net/maps/nextbike-live.json', headers={'Accept-Encoding': 'gzip'})
        self.cache.save_file(crawl_time, response.json())
        log.info("...done")

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
    logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] [%(levelname)8s] %(name)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").propagate = False
    logging.getLogger("charset_normalizer").setLevel(logging.CRITICAL)
    logging.getLogger("charset_normalizer").propagate = False

    with concurrent.futures.ThreadPoolExecutor(5) as executor:
        cache = Cache(
            file_path=Path("cache"),
            executor=executor,
            data_store=FileSystemStore(Path(os.getenv("DATA_STORE_PATH", "cache/data"))),
            base_file_creation_interval=30*60
        )
        cache.init()

        crawler = NextBikeCrawler(cache)

        crawler.crawl()


if __name__ == "__main__":
    main()
