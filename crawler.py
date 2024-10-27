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
        log.info(f"Fetching data for {crawl_time}")
        t1 = time.perf_counter()
        response = session.get('https://maps.nextbike.net/maps/nextbike-live.json', headers={'Accept-Encoding': 'gzip'})
        log.info(f"fetched ({time.perf_counter() - t1:.2f} s)")
        self.cache.save_file(crawl_time, response.json())
        log.info(f"saved {crawl_time}")

    def wait_until_next_update(self) -> datetime.datetime:
        next_time = (time.time() // self.wait_interval + 1) * self.wait_interval
        log.debug(f"waiting {next_time - time.time():.1f} s")
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
                        datefmt="%Y-%m-%d %H:%M:%S", force=True)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").propagate = False
    logging.getLogger("charset_normalizer").setLevel(logging.CRITICAL)
    logging.getLogger("charset_normalizer").propagate = False

    store_type = DirtyFileSystemStore if bool(os.getenv("USE_DIRTY_STORE", False)) else FileSystemStore

    with concurrent.futures.ProcessPoolExecutor(max_workers=int(os.getenv("MAX_WORKERS", None))) as executor:
        cache = Cache(
            file_path=Path(os.getenv("CACHE_PATH", "cache")),
            executor=executor,
            data_store=DirtyFileSystemStore(Path(os.getenv("DATA_STORE_PATH", "cache/data"))),
            base_file_creation_interval=int(os.getenv("BASE_FILE_CREATION_INTERVAL", 30 * 60)),
        )
        cache.init()

        crawler = NextBikeCrawler(
            cache=cache,
            wait_interval=int(os.getenv("WAIT_INTERVAL", 10))
        )

        crawler.crawl()


if __name__ == "__main__":
    main()
