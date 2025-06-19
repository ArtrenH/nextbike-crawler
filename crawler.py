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
from upath import UPath

from jsoncache.preprocessors import *
from jsoncache import *

load_dotenv()

log = logging.getLogger("NextBikeCrawler")


@dataclasses.dataclass
class NextBikeCrawler:
    cache: JsonCache
    base_timestamp: datetime.datetime | None = None
    wait_interval: int = 10
    save_executor: concurrent.futures.ThreadPoolExecutor = dataclasses.field(
        default_factory=lambda: concurrent.futures.ThreadPoolExecutor()
    )

    def update(self, crawl_time: datetime.datetime, session: requests.Session):
        log.info(f"Fetching data for {crawl_time}")
        t1 = time.perf_counter()
        response = session.get('https://maps.nextbike.net/maps/nextbike-live.json', headers={'Accept-Encoding': 'gzip'})
        log.info(f"fetched ({time.perf_counter() - t1:.2f} s)")
        # self.cache.save_file(crawl_time, response.json())

        def run_save():
            try:
                self.cache.save_file(crawl_time, response.json())
            except Exception:
                logging.critical("Could not save fetched data!", exc_info=True)
                raise

        self.save_executor.submit(run_save)
        log.info(f"saved {crawl_time}")

    def wait_until_next_update(self) -> datetime.datetime:
        next_time = (time.time() // self.wait_interval + 1) * self.wait_interval
        log.debug(f"waiting {next_time - time.time():.1f} s")  # TODO: how many skipped?
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

    rules = [
        ("$.countries[*].cities[*].places[*].bike_list", "number"),
        ("$.countries[*].cities[*].places", "uid"),
        ("$.countries[*].cities", "uid"),
        ("$.countries", "name"),
    ]

    with (
        concurrent.futures.ProcessPoolExecutor(max_workers=int(os.getenv("MAX_WORKERS", None))) as compression_executor,
        concurrent.futures.ThreadPoolExecutor() as future_wait_executor
    ):
        cache = JsonCache(
            writable_json_store=WritableJsonStore(
                json_store=JsonStore(
                    data_store=UPath(os.getenv("CACHE_FS_URL")),
                    preprocessors=[
                        JsonDictionarizer(rules)
                    ]
                ),
                cache_dir_path=Path(os.getenv("CACHE_PATH", "cache")),
                executor=compression_executor
            ),
            future_wait_executor=future_wait_executor,
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
