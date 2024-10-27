from __future__ import annotations

import concurrent.futures
import contextlib
import dataclasses
import datetime
import io
import json
import logging
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

__all__ = [
    "CouldNotAcquireLockException",
    "FilePidLock",
    "Store",
    "FileSystemStore",
    "Cache"
]

log = logging.getLogger("Cache")


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
        items = file_path.name.split("-", 3)
        if len(items) == 4:
            _, pid_str, create_time_str, timestamp_str = items
        else:
            return False

        try:
            locked_pid = int(pid_str)
            create_time_ns = int(create_time_str)
            timestamp_int = int(timestamp_str)
        except ValueError:
            return False

        if psutil.pid_exists(locked_pid) and int(psutil.Process(locked_pid).create_time() * 1e9) == create_time_ns:
            return locked_pid, timestamp_int
        else:
            return False

    def write_lock(self) -> tuple[Path, int]:
        timestamp = time.time_ns()
        pid = os.getpid()
        create_time_str = str(int(psutil.Process(pid).create_time() * 1e9))
        lock_file = self.file_path / f"{random.randbytes(16).hex()}-{pid}-{create_time_str}-{int(timestamp)}"
        lock_file.touch()
        return lock_file, timestamp

    def check_existing_locks(self, our_lock: tuple[str, int] | None = None):
        for lock_file in self.file_path.iterdir():
            if our_lock is not None and lock_file.name == our_lock[0]:
                continue

            lock_data = self.is_lock_valid(lock_file)

            if not lock_data:
                # remove invalid lock files
                lock_file.unlink(missing_ok=True)
                continue

            locked_pid, timestamp = lock_data

            if our_lock is not None and timestamp > our_lock[1]:
                # our lock is older than the existing lock
                lock_file.unlink(missing_ok=True)
                continue

            raise CouldNotAcquireLockException(
                f"Could not acquire lock at {self.file_path!s}. "
                f"Blocked by PID {lock_data[0]!s} since {datetime.datetime.fromtimestamp(lock_data[1] * 1e-9)!s}. "
                f"File: {lock_file.name!r}"
            )

    def acquire(self) -> Path:
        self.file_path.mkdir(parents=True, exist_ok=True)

        self.check_existing_locks()

        file_path, timestamp = self.write_lock()

        self.check_existing_locks(our_lock=(file_path.name, timestamp))

        # redundancy check
        if not file_path.exists():
            raise CouldNotAcquireLockException("Could not acquire lock. Lock file vanished.")

        return file_path


class Store(typing.Protocol):
    """
    This store only saves and loads the compressed files. All cache file operations are handled by the Cache directly.
    """

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

        # with contextlib.suppress(OSError):
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
        file_path = self.file_path / "compression"
        file_path.mkdir(parents=True, exist_ok=True)
        for folder in file_path.iterdir():
            if not folder.is_dir():
                log.error(f"Unexpected file in compression folder: {folder!s}")
                continue
            if not ((folder / "base_timestamp.txt").exists() and (folder / "base.bson").exists()):
                log.error(f"Missing base.bson or base_timestamp.txt in {folder!s}.")
                continue

            log.info(f"Recovering lost compression task at {folder!s}.")
            self.executor.submit(self._compress_folder, folder=folder, store=self.data_store)

    def save_base_file(self, timestamp: datetime.datetime, content: dict):
        if (self.file_path / "current" / "base_timestamp.txt").exists():
            self.compress_folder()

        log.info("saving base file.")
        folder_path = self.file_path / "current"

        folder_path.mkdir(parents=True, exist_ok=True)
        filename = "base.bson"

        (folder_path / filename).write_bytes(bson.dumps(content))
        (self.file_path / "current" / "base_timestamp.txt").write_text(
            timestamp.astimezone(datetime.timezone.utc).isoformat()
        )

    def save_diff_file(self, timestamp: datetime.datetime, content: dict):
        log.info("saving patch file.")

        filename = timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S") + ".bson"

        (self.file_path / "current" / filename).write_bytes(bson.dumps(content))

    def save_file(self, timestamp: datetime.datetime, content: dict):
        with self.lock:
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
        log.debug("Starting to compress current folder.")
        target_path = self.file_path / "compression" / random.randbytes(16).hex()
        target_path.parent.mkdir(parents=True, exist_ok=True)

        (self.file_path / "current").rename(target_path)

        self.executor.submit(self._compress_folder, folder=target_path, store=self.data_store)

    @staticmethod
    def _log_exceptions(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                log.critical("Exception in %s.", func.__qualname__, exc_info=True)
                raise

        return wrapper

    @staticmethod
    @_log_exceptions
    def _compress_folder(folder: Path, store: Store):
        logger = log.getChild(folder.stem)

        logger.debug("Starting compression task at %s.", folder)

        t1 = time.perf_counter()
        n_files = 1
        total_data = 0

        base_timestamp = datetime.datetime.fromisoformat((folder / "base_timestamp.txt").read_text())

        base_file = (folder / "base.bson").read_bytes()
        prev_file = bson.loads(base_file)

        filename = base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz"
        logger.info("Compressing to %s.", filename)

        try:
            compressed_file = (folder / "compressed.tar.xz").read_bytes()
        except FileNotFoundError:
            pass
        else:
            logger.info("Folder was already compressed.")
            try:
                store.save_file(filename, compressed_file)
            except:
                logger.error("Could not save compressed file.")
                raise
            else:
                shutil.rmtree(folder)
                logger.info("Done. Deleted folder.")
                return

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

        duration = time.perf_counter() - t1
        logger.info(
            f"Finished archive {filename!r}. "
            f"Took {duration:.2f} s for {n_files} files ({duration / n_files:.2f} s/file). "
            f"Compression ratio: {total_data / file_io.tell():.2f}"
        )

        try:
            logger.info("Storing compressed file.")
            store.save_file(filename, file_io.getvalue())
        except Exception:
            logger.error("Could not store compressed file. Saving into compression task.")
            (folder / "compressed.tar.xz").write_bytes(file_io.getvalue())
            raise

        shutil.rmtree(folder)
        logger.info("Done. Deleted folder.")

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
    lock_file = lock.acquire()
    # lock_file.unlink()
    try:
        lock.acquire()
    except CouldNotAcquireLockException as e:
        print(e)
    else:
        raise AssertionError("Lock acquired twice.")

    print("Releasing lock.")
    lock_file.unlink()

    try:
        lock.acquire()
    except CouldNotAcquireLockException as e:
        raise AssertionError("Could not acquire lock.") from e
    else:
        print("Lock acquired.")


if __name__ == "__main__":
    main()
