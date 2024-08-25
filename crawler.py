from __future__ import annotations

import concurrent.futures
import copy
import io
import json
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
    last_known_timestamp: datetime.datetime | None = None
    base_file_creation_interval: int = 5 * 60

    def save_base_file(self, timestamp: datetime.datetime, content: dict):
        if (self.file_path / "current" / "base_timestamp.txt").exists():
            self.compress_folder()

        print("saving base file...")
        folder_path = self.file_path / "current"

        folder_path.mkdir(parents=True, exist_ok=True)
        filename = "base.bson"

        (folder_path / filename).write_bytes(bson.dumps(content))
        (self.file_path / "current" / "base_timestamp.txt").write_text(timestamp.astimezone(datetime.timezone.utc).isoformat())

    def save_diff_file(self, timestamp: datetime.datetime, content: dict):
        print("saving patch file...")

        filename = timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S") + ".bson"

        (self.file_path / "current" / filename).write_bytes(bson.dumps(content))

    def save_file(self, timestamp: datetime.datetime, content: dict):
        no_previous_data = self.last_known_timestamp is None
        if not no_previous_data:
            base_file_creation_file_interval_passed = int(timestamp.timestamp()) // self.base_file_creation_interval != int(self.last_known_timestamp.timestamp()) // self.base_file_creation_interval
        else:
            base_file_creation_file_interval_passed = False

        if no_previous_data or base_file_creation_file_interval_passed:
            self.save_base_file(timestamp, content)
        else:
            self.save_diff_file(timestamp, content)
        self.last_known_timestamp = timestamp

    def compress_folder(self):
        print("compressing folder")
        new_folder_name = f"current-{random.randbytes(16).hex()}"
        (self.file_path / "current").rename(self.file_path / new_folder_name)
        folder = self.file_path / new_folder_name
        self.executor.submit(self._compress_folder, folder=folder, file_path=self.file_path)
        # self._compress_folder(folder, self.file_path)

    @staticmethod
    def _compress_folder(folder: Path, file_path: Path):
        print("_compressing folder")
        base_timestamp = datetime.datetime.fromisoformat((folder / "base_timestamp.txt").read_text())

        file_path = file_path / "data" / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        base_file = (folder / "base.bson").read_bytes()
        prev_file = bson.loads(base_file)

        print(f"-> opening tar file at {file_path!s}")
        with tarfile.open(file_path, "w:gz") as tar:
            for file in sorted(folder.iterdir(), key=lambda s: s.name):
                # print(f"-> file {file!s}")
                if file.name == "base_timestamp.txt":
                    continue
                elif file.name == "base.bson":
                    # print("-> storing base")
                    tarinfo = tarfile.TarInfo(file.name)
                    tarinfo.size = len(base_file)
                    tar.addfile(tarinfo, fileobj=io.BytesIO(base_file))
                else:
                    # print("-> storing patch")
                    content = bson.loads(file.read_bytes())
                    diff = jsonpatch.make_patch(prev_file, content)
                    diff_bson = bson.dumps({"": diff.patch})
                    tarinfo = tarfile.TarInfo(file.name)
                    tarinfo.size = len(diff_bson)
                    tar.addfile(tarinfo, fileobj=io.BytesIO(diff_bson))
                    prev_file = content

        shutil.rmtree(folder)

    def iter_compressed(self, timestamp: datetime.datetime) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        base_timestamp = timestamp.astimezone(datetime.timezone.utc)
        file_path = self.file_path / "data" / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")

        with tarfile.open(file_path, "r:gz") as tar:
            base_content = tar.extractfile("base.bson").read()
            base_file = bson.loads(base_content)
            yield base_timestamp, base_file

            for file in sorted(tar.getmembers(), key=lambda m: m.name):
                if file.name == "base.bson":
                    continue

                timestamp_str, _ = file.name.split(".")

                timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S").replace(tzinfo=datetime.timezone.utc)

                with tar.extractfile(file) as f:
                    file_content = f.read()
                    json_patch = list(bson.loads(file_content).values())[0]
                    jsonpatch.JsonPatch(json_patch).apply(base_file, in_place=True)
                    yield timestamp, base_file

    def iter_compressed_patches(self, timestamp: datetime.datetime) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        base_timestamp = timestamp.astimezone(datetime.timezone.utc)
        file_path = self.file_path / "data" / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")

        with tarfile.open(file_path, "r:gz") as tar:
            base_content = tar.extractfile("base.bson").read()
            base_file = bson.loads(base_content)
            yield "base", base_timestamp, base_file

            for file in sorted(tar.getmembers(), key=lambda m: m.name):
                if file.name == "base.bson":
                    continue

                timestamp_str, _ = file.name.split(".")

                timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S").replace(tzinfo=datetime.timezone.utc)

                with tar.extractfile(file) as f:
                    file_content = f.read()
                    json_patch = list(bson.loads(file_content).values())[0]

                    yield "patch", timestamp, json_patch

    def decompress(self, base_timestamp: datetime.datetime):
        for type_, timestamp, data in self.iter_compressed_patches(base_timestamp):
            out_filepath = self.file_path / base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") / timestamp.strftime(type_ + "-%Y-%m-%dT%H-%M-%S.json")

            out_filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(out_filepath, "w") as f:
                json.dump(data, f)

    def iter_files(self, since: datetime.datetime, until: datetime.datetime) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        for file in (self.file_path / "data").iterdir():
            try:
                timestamp = datetime.datetime.strptime(file.name.split(".", 1)[0], "%Y-%m-%dT%H-%M-%S").replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                print(f"invalid file {file!s}")
                continue

            if since < timestamp < until:
                yield from self.iter_compressed(timestamp)


@dataclasses.dataclass
class NextBikeCrawler:
    cache: Cache
    base_timestamp: datetime.datetime | None = None
    base_file_creation_interval: int = 30 * 60
    wait_interval: int = 10

    def update(self, crawl_time: datetime.datetime, session: requests.Session):
        print("downloading...")
        response = session.get('https://maps.nextbike.net/maps/nextbike-live.json', headers={'Accept-Encoding': 'gzip'})
        self.cache.save_file(crawl_time, response.json())

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
            # break


def main():
    cache = Cache(Path("cache"), concurrent.futures.ProcessPoolExecutor(5))

    crawler = NextBikeCrawler(cache)

    crawler.crawl()


if __name__ == "__main__":
    main()

