from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime
import io
import json
import multiprocessing
import os
import random
import shutil
import tarfile
import time
import typing
from pathlib import Path

import bson
import jsonpatch
import psutil


class CouldNotAcquireLockException(Exception):
    pass


@dataclasses.dataclass
class FilePidLock:
    file_path: Path

    def __post_init__(self):
        self.file_path = self.file_path.resolve()

    @staticmethod
    def is_lock_valid(
        file_path: Path
    ) -> typing.Literal[False] | tuple[int, int]:
        items = file_path.name.split("-", 2)
        if len(items) == 3:
            _, pid_str, timestamp_str = items
        else:
            return False

        try:
            locked_pid = int(pid_str)
            timestamp_int = int(timestamp_str)
        except ValueError:
            return False

        if psutil.pid_exists(locked_pid):
            return locked_pid, timestamp_int
        else:
            return False

    def write_lock(self) -> tuple[Path, int, int]:
        timestamp = time.time_ns()
        pid = os.getpid()
        lock_file = self.file_path / f"{random.randbytes(16).hex()}-{pid}-{int(timestamp)}"
        lock_file.touch()
        return lock_file, timestamp, pid

    def check_existing_locks(self, our_lock: tuple[int, int] | None = None):
        for lock_file in self.file_path.iterdir():
            if not (lock_data := self.is_lock_valid(lock_file)):
                lock_file.unlink(missing_ok=True)
            else:
                if our_lock is not None and our_lock[0] == lock_data[0]:
                    if lock_data[1] <= our_lock[1]:
                        continue

                raise CouldNotAcquireLockException(
                    f"Could not acquire lock at {self.file_path!s}. "
                    f"Blocked by PID {lock_data[0]!s} since {datetime.datetime.fromtimestamp(lock_data[1] * 1e-9)!s}. "
                    f"File: {lock_file!s}"
                )

    def acquire(self) -> Path:
        self.file_path.mkdir(parents=True, exist_ok=True)

        self.check_existing_locks()

        file_path, timestamp, pid = self.write_lock()

        self.check_existing_locks(our_lock=(pid, timestamp))

        # redundancy check
        if not file_path.exists():
            raise CouldNotAcquireLockException("Could not acquire lock. Lock file vanished.")

        return file_path


class Store(typing.Protocol):
    def save_file(self, filename: str, content: bytes):
        ...

    def read_file(self, filename: str) -> bytes:
        ...

    def iterdir(self) -> typing.Iterable[str]:
        ...


@dataclasses.dataclass
class FileSystemStore:
    path: Path

    def save_file(self, filename: str, content: bytes):
        self.path.mkdir(parents=True, exist_ok=True)
        (self.path / (filename + ".tmp")).write_bytes(content)
        (self.path / (filename + ".tmp")).rename(self.path / filename)

    def read_file(self, filename: str) -> bytes:
        return (self.path / filename).read_bytes()

    def iterdir(self):
        return (f.name for f in self.path.iterdir())


@dataclasses.dataclass
class Cache:
    file_path: Path
    data_store: Store
    executor: concurrent.futures.Executor
    last_known_timestamp: datetime.datetime | None = None
    base_file_creation_interval: int = 30 * 60
    lock: multiprocessing.Lock = dataclasses.field(init=False, default_factory=multiprocessing.Lock)

    def init(self):
        self.acquire_cache_lock()
        self.recover_lost_compression_tasks()

    def acquire_cache_lock(self):
        FilePidLock(self.file_path / "lock").acquire()

    def recover_lost_compression_tasks(self):
        for folder in (self.file_path / "compression").iterdir():
            if not folder.is_dir():
                print(f"Unexpected file in compression folder: {folder!s}")
                continue
            if not ((folder / "base_timestamp.txt").exists() and (folder / "base.bson").exists()):
                print(f"Missing base.bson or base_timestamp.txt in {folder!s}.")
                continue

            print(f"Recovering lost compression task at {folder!s}.")
            self.executor.submit(self._compress_folder, folder=folder, store=self.data_store)

    def save_base_file(self, timestamp: datetime.datetime, content: dict):
        if (self.file_path / "current" / "base_timestamp.txt").exists():
            self.compress_folder()

        print("saving base file...")
        folder_path = self.file_path / "current"

        folder_path.mkdir(parents=True, exist_ok=True)
        filename = "base.bson"

        (folder_path / filename).write_bytes(bson.dumps(content))
        (self.file_path / "current" / "base_timestamp.txt").write_text(
            timestamp.astimezone(datetime.timezone.utc).isoformat()
        )

    def save_diff_file(self, timestamp: datetime.datetime, content: dict):
        print("saving patch file...")

        filename = timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S") + ".bson"

        (self.file_path / "current" / filename).write_bytes(bson.dumps(content))

    def save_file(self, timestamp: datetime.datetime, content: dict):
        with (self.lock):
            if self.last_known_timestamp is None:
                self.save_base_file(timestamp, content)
                self.last_known_timestamp = timestamp
                return

            assert self.last_known_timestamp < timestamp, (
                f"Timestamp to be saved ({timestamp!s}) older than "
                f"last known timestamp ({self.last_known_timestamp!s})."
            )

            base_file_creation_file_interval_passed = (
                int(timestamp.timestamp()) // self.base_file_creation_interval
                !=
                int(self.last_known_timestamp.timestamp()) // self.base_file_creation_interval
            )

            if base_file_creation_file_interval_passed:
                self.save_base_file(timestamp, content)
            else:
                self.save_diff_file(timestamp, content)

            self.last_known_timestamp = timestamp

    def compress_folder(self):
        print("compressing folder")

        target_path = self.file_path / "compression" / random.randbytes(16).hex()
        target_path.parent.mkdir(parents=True, exist_ok=True)

        (self.file_path / "current").rename(target_path)

        self.executor.submit(self._compress_folder, folder=target_path, store=self.data_store)

    @staticmethod
    def _compress_folder(folder: Path, store: Store):
        t1 = time.perf_counter()
        n_files = 1
        total_data = 0

        base_timestamp = datetime.datetime.fromisoformat((folder / "base_timestamp.txt").read_text())

        base_file = (folder / "base.bson").read_bytes()
        prev_file = bson.loads(base_file)

        filename = base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz"
        print("Compressing folder", folder, "to", filename)

        file_io = io.BytesIO()
        with tarfile.open(fileobj=file_io, mode="w:gz") as tar:
            for file in sorted(folder.iterdir(), key=lambda s: s.name):
                # print(f"-> file {file!s}")
                if file.name == "base_timestamp.txt":
                    continue
                elif file.name == "base.bson":
                    # print("-> storing base")
                    tarinfo = tarfile.TarInfo(file.name)
                    tarinfo.size = len(base_file)
                    tar.addfile(tarinfo, fileobj=io.BytesIO(base_file))

                    n_files += 1
                    total_data += len(base_file)
                else:
                    # print("-> storing patch")
                    content = bson.loads(file.read_bytes())
                    diff = jsonpatch.make_patch(prev_file, content)
                    diff_bson = bson.dumps({"": diff.patch})
                    tarinfo = tarfile.TarInfo(file.name)
                    tarinfo.size = len(diff_bson)
                    tar.addfile(tarinfo, fileobj=io.BytesIO(diff_bson))
                    prev_file = content

                    n_files += 1
                    total_data += len(diff_bson)

        store.save_file(filename, file_io.getvalue())

        shutil.rmtree(folder)

        duration = time.perf_counter() - t1
        print(
            f"Finished archive {filename!r}. "
            f"Took {duration:.2f} s for {n_files} files ({duration / n_files:.2f} s/file). "
            f"Compression ratio: {total_data / file_io.tell():.2f}."
        )

    def iter_archive(
        self,
        timestamp: datetime.datetime
    ) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        it = self.iter_archive_raw(timestamp)

        type_, timestamp, base_data = next(it)
        assert type_ == "base"

        yield timestamp, base_data

        for type_, timestamp, patch in it:
            assert type_ == "patch"

            jsonpatch.JsonPatch(patch).apply(base_data, in_place=True)
            yield timestamp, base_data

    def iter_archive_raw(
        self,
        timestamp: datetime.datetime
    ) -> typing.Generator[tuple[str, datetime.datetime, dict], None, None]:
        base_timestamp = timestamp.astimezone(datetime.timezone.utc)

        file = self.data_store.read_file(base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")

        with tarfile.open(fileobj=io.BytesIO(file), mode="r:gz") as tar:
            base_content = tar.extractfile("base.bson").read()
            base_file = bson.loads(base_content)
            yield "base", base_timestamp, base_file

            for file in sorted(tar.getmembers(), key=lambda m: m.name):
                if file.name == "base.bson":
                    continue

                timestamp_str, _ = file.name.split(".")

                timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S").replace(
                    tzinfo=datetime.timezone.utc)

                with tar.extractfile(file) as f:
                    file_content = f.read()
                    json_patch = list(bson.loads(file_content).values())[0]

                    yield "patch", timestamp, json_patch

    def decompress(self, base_timestamp: datetime.datetime):
        for type_, timestamp, data in self.iter_archive_raw(base_timestamp):
            out_filepath = self.file_path / base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") / timestamp.strftime(
                type_ + "-%Y-%m-%dT%H-%M-%S.json")

            out_filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(out_filepath, "w") as f:
                json.dump(data, f)

    def iter_files(
        self,
        since: datetime.datetime,
        until: datetime.datetime
    ) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        for file_name in self.data_store.iterdir():
            try:
                timestamp_str, _ = file_name.split(".", 1)
                timestamp = datetime.datetime.strptime(
                    timestamp_str, "%Y-%m-%dT%H-%M-%S"
                ).replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                print(f"Cache data dir contains invalid file {file_name!s}. Skipping.")
                continue

            if since < timestamp < until:
                try:
                    yield from self.iter_archive(timestamp)
                except (tarfile.ReadError, EOFError):
                    print(f"Could not read file {file_name!s}. Skipping.")


def main():
    lock = FilePidLock(Path("cache/lock2"))
    lock.acquire()
    lock.acquire()


if __name__ == "__main__":
    main()