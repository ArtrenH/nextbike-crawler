from __future__ import annotations

import concurrent.futures
import random
import shutil
import tarfile
import datetime
import dataclasses
import time
import typing
from pathlib import Path

import bson
import requests
import jsonpatch


@dataclasses.dataclass
class Cache:
    file_path: Path
    executor: concurrent.futures.Executor

    def save_base_file(self, timestamp: datetime.datetime, content: bytes):
        if (self.file_path / "current" / "base_timestamp.txt").exists():
            self.compress_folder()

        print("saving base file...")
        folder_path = self.file_path / "current"

        folder_path.mkdir(parents=True, exist_ok=True)
        filename = "base.bson"

        (folder_path / filename).write_bytes(content)
        (self.file_path / "current" / "base_timestamp.txt").write_text(timestamp.astimezone(datetime.timezone.utc).isoformat())

    def save_patch_file(self, timestamp: datetime.datetime, content: bytes):
        print("saving patch file...")

        filename = timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S") + ".bson"

        (self.file_path / "current" / filename).write_bytes(content)

    def compress_folder(self):
        new_folder_name = f"current-{random.randbytes(16).hex()}"
        (self.file_path / "current").rename(self.file_path / new_folder_name)
        folder = self.file_path / new_folder_name
        self.executor.submit(self._compress_folder, folder=folder, file_path=self.file_path)

    @staticmethod
    def _compress_folder(folder: Path, file_path: Path):
        base_timestamp = datetime.datetime.fromisoformat((folder / "base_timestamp.txt").read_text())

        file_path = file_path / "data" / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with tarfile.open(file_path, "w:gz") as tar:
            for file in folder.iterdir():
                if file.name == "base_timestamp.txt":
                    continue

                tar.add(file, arcname=file.name)

        shutil.rmtree(folder)

    def iter_compressed(self, timestamp: datetime.datetime) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        base_timestamp = timestamp.astimezone(datetime.timezone.utc)
        file_path = self.file_path / "data" / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")

        with tarfile.open(file_path, "r|gz") as tar:
            base_file = bson.loads(tar.extractfile("base.bson").read())
            yield base_timestamp, base_file

            for file in sorted(tar.getmembers(), key=lambda m: m.name):
                if file.name == "base.bson":
                    continue

                timestamp_str, _ = file.name.split(".")

                timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S").replace(tzinfo=datetime.timezone.utc)

                with tar.extractfile(file) as f:
                    json_patch = list(bson.loads(f.read()).values())[0]

                    jsonpatch.JsonPatch(json_patch).apply(base_file, in_place=True)
                    
                    yield timestamp, base_file

    def iter_files(self, since: datetime.datetime, until: datetime.datetime) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        for file in (self.file_path / "data").iterdir():
            timestamp = datetime.datetime.strptime(file.name.split(".", 1)[0], "%Y-%m-%dT%H-%M-%S").replace(tzinfo=datetime.timezone.utc)

            if since < timestamp < until:
                yield from self.iter_compressed(timestamp)


@dataclasses.dataclass
class NextBikeCrawler:
    cache: Cache
    base_timestamp: datetime.datetime | None = None
    last_file: dict | None = None
    base_file_creation_interval: float = 30 * 60
    wait_interval: float = 10

    def update(self, crawl_time: datetime.datetime, session: requests.Session):
        print("downloading...")
        response = session.get('https://maps.nextbike.net/maps/nextbike-live.json', headers={'Accept-Encoding': 'gzip'})
        data = response.json()

        should_store_base_file = (
            (
                crawl_time.timestamp() % self.base_file_creation_interval == 0
            ) or (
                self.base_timestamp is not None
                and (datetime.datetime.now(datetime.timezone.utc) - self.base_timestamp).total_seconds() > self.base_file_creation_interval
            )
        )

        if should_store_base_file:
            self.last_file = None
            self.base_timestamp = None

        print("dumping...")
        if self.last_file is None:
            self.cache.save_base_file(crawl_time, bson.dumps(data))
            self.base_timestamp = crawl_time
        else:
            diff = jsonpatch.make_patch(self.last_file, data)
            print(f"changes: {len(diff.patch)}")
            self.cache.save_patch_file(crawl_time, bson.dumps({"": diff.patch}))


        self.last_file = data

    def wait_until_next_update(self) -> datetime.datetime:
        next_time = (time.time() // self.wait_interval + 1) * self.wait_interval
        time.sleep(next_time - time.time())
        return datetime.datetime.fromtimestamp(next_time, datetime.timezone.utc)

    def crawl(self) -> None:
        session = requests.Session()
        
        while True:
            print("waiting...")
            crawl_time = self.wait_until_next_update()
            self.update(crawl_time, session=session)


def main():
    cache = Cache(Path("cache"), concurrent.futures.ProcessPoolExecutor(2))

    crawler = NextBikeCrawler(cache)

    crawler.crawl()


if __name__ == "__main__":
    main()
