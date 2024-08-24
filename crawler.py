import shutil
import tarfile
import datetime
import dataclasses
import time
from pathlib import Path

import bson
import requests
import jsonpatch


@dataclasses.dataclass
class Cache:
    file_path: Path

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
        folder = self.file_path / "current"

        base_timestamp = datetime.datetime.fromisoformat((folder / "base_timestamp.txt").read_text())

        file_path = self.file_path / "data" / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        tar = tarfile.open(file_path, "w:gz")

        for file in folder.iterdir():
            if file.name == "base_timestamp.txt":
                continue

            tar.add(file, arcname=file.name)

        tar.close()

        shutil.rmtree(folder)


@dataclasses.dataclass
class NextBikeCrawler:
    cache: Cache
    base_timestamp: datetime.datetime | None = None
    last_file: dict | None = None
    base_file_creation_interval: float = 24 * 60 * 60  # seconds
    wait_interval: float = 10

    def update(self, crawl_time: datetime.datetime):
        print("downloading...")
        response = requests.get('https://maps.nextbike.net/maps/nextbike-live.json')
        data = response.json()

        print("dumping...")
        if self.last_file is None:
            self.cache.save_base_file(crawl_time, bson.dumps(data))
            self.base_timestamp = crawl_time
        else:
            diff = jsonpatch.make_patch(self.last_file, data)
            print(f"changes: {len(diff.patch)}")
            self.cache.save_patch_file(crawl_time, bson.dumps({"": diff.patch}))

            if (self.base_timestamp - datetime.datetime.now(datetime.timezone.utc)).total_seconds():
                self.last_file = None
                self.base_timestamp = None

        self.last_file = data

    def wait_until_next_update(self) -> datetime.datetime:
        next_time = (time.time() // self.wait_interval + 1) * self.wait_interval
        time.sleep(next_time - time.time())
        return datetime.datetime.fromtimestamp(next_time)

    def crawl(self) -> None:
        while True:
            crawl_time = self.wait_until_next_update()
            self.update(crawl_time)


def main():
    cache = Cache(Path("cache"))

    crawler = NextBikeCrawler(cache)

    crawler.crawl()


if __name__ == "__main__":
    main()
