from __future__ import annotations

import concurrent.futures
import contextlib
import copy
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

import jsonpatch
import jsonpath_ng
import orjson
import psutil
import fs.base
import fastjsonpatch

__all__ = [
    "CouldNotAcquireLockException",
    "FilePidLock",
    "Store",
    "FileSystemStore",
    "FsStore",
    "DirtyFileSystemStore",
    "JsonDictionarizer",
    "Cache",
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

        (self.path / (filename + ".tmp")).write_bytes(content)
        (self.path / (filename + ".tmp")).rename(self.path / filename)

    def read_file(self, filename: str) -> bytes:
        return (self.path / filename).read_bytes()

    def iterdir(self):
        return (f.name for f in self.path.iterdir())


@dataclasses.dataclass
class FsStore:
    fs: fs.base.FS

    def save_file(self, filename: str, content: bytes):
        self.fs.writebytes(filename, content)

    def read_file(self, filename: str) -> bytes:
        return self.fs.readbytes(filename)

    def iterdir(self):
        return self.fs.listdir("/")

    @classmethod
    def from_url(cls, url: str):
        return cls(fs.open_fs(url))


class DirtyFileSystemStore(FileSystemStore):
    def save_file(self, filename: str, content: bytes):
        with contextlib.suppress(OSError):
            super().save_file(filename, content)


class Preprocessor(typing.Protocol):
    def do(self, data: dict) -> None:
        ...

    def undo(self, data: dict) -> None:
        ...


@dataclasses.dataclass
class JsonDictionarizer:
    rules: list[tuple[str, str]]
    rules_parsed: list[tuple[jsonpath_ng.JSONPath, str]] = dataclasses.field(init=False)

    def __post_init__(self):
        self.rules_parsed = [(jsonpath_ng.parse(jsonpath), key_field) for jsonpath, key_field in self.rules]

    def do(self, data):
        """
        :param rules: A list of (JSONPath, key_field) tuples.
                      For each tuple:
                        - JSONPath is used to find an array in `data`.
                        - key_field is the property name to use as the dict key.
        :param data:  The dictionary to transform in-place.
        :return:      The same dictionary `data` with matched arrays replaced by dicts.
        """
        for jsonpath_expr, key_field in self.rules_parsed:
            matches = jsonpath_expr.find(data)

            for match in matches:
                array_val = match.value

                if isinstance(array_val, list):
                    keyed_dict = {str(item[key_field]): item for item in array_val}

                    match.path.update(match.context.value, keyed_dict)

    def undo(self, data):
        """
        :param rules: A list of (JSONPath, key_field) tuples.
                      For each tuple:
                        - JSONPath indicates where in `data` we expect to find a dict
                          that was previously an array keyed by key_field.
                        - key_field is the property name that was used as the dict key.
        :param data:  The dictionary to transform in-place.
        :return:      The same dictionary `data` with matched dicts replaced by arrays of objects.

        For each match:
          1) We assume the matched value is a dict of shape {key_field_value: item_dict, ...}.
          2) Convert that dict into a list of item_dicts,
             ensuring item_dict[key_field] = key_field_value.
        """

        # data = copy.copy(data)

        for jsonpath_expr, key_field in reversed(self.rules_parsed):
            matches: list[jsonpath_ng.DatumInContext] = jsonpath_expr.find(data)

            for match in matches:
                dict_val = match.value  # .copy()

                array_val = list(dict_val.values())

                # # Replace the dict in the original structure with the new array
                # parent_path = match.path.child(jsonpath_ng.Parent())
                # parent_val = parent_path.find(data)
                # assert len(parent_val) == 1
                #
                # if parent_val[0].path == jsonpath_ng.This():
                #     pass
                # else:
                #     parent_path.update(data, parent_val.context.copy())

                match.path.update(match.context.value, array_val)


@dataclasses.dataclass
class Cache[* T]:
    file_path: Path
    data_store_factory: tuple[typing.Callable[[*T], Store], tuple[*T]]
    executor: concurrent.futures.Executor
    last_known_timestamp: datetime.datetime | None = None
    base_file_creation_interval: int = 30 * 60
    lock: multiprocessing.Lock = dataclasses.field(init=False, default_factory=multiprocessing.Lock)
    future_wait_executor: concurrent.futures.ThreadPoolExecutor = dataclasses.field(
        init=False,
        default_factory=lambda: concurrent.futures.ThreadPoolExecutor(),
    )
    preprocessors: list[Preprocessor] = dataclasses.field(default_factory=list)

    _data_store: Store | None = dataclasses.field(init=False, default=None)

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
            if not ((folder / "base_timestamp.txt").exists() and (folder / "base.json").exists()):
                log.error(f"Missing base.json or base_timestamp.txt in {folder!s}.")
                continue

            log.info(f"Recovering lost compression task at {folder!s}.")
            fut = self.executor.submit(self._compress_folder, folder=folder, store_factory=self.data_store_factory,
                                       preprocessors=self.preprocessors)
            self.future_wait_executor.submit(
                self._wait_for_compression_task,
                future=fut,
            )

    @staticmethod
    def _wait_for_compression_task(future: concurrent.futures.Future):
        try:
            future.result()
        except Exception:
            log.error("Compression task failed.", exc_info=True)
        else:
            log.info("Compression task finished successfully.")

    def save_base_file(self, timestamp: datetime.datetime, content: dict):
        if (self.file_path / "current" / "base_timestamp.txt").exists():
            self.compress_folder()

        log.info("saving base file.")
        folder_path = self.file_path / "current"

        folder_path.mkdir(parents=True, exist_ok=True)
        filename = "base.json"

        (folder_path / filename).write_bytes(orjson.dumps(content))
        (self.file_path / "current" / "base_timestamp.txt").write_text(
            timestamp.astimezone(datetime.timezone.utc).isoformat()
        )
        log.debug("..done.")

    def save_diff_file(self, timestamp: datetime.datetime, content: dict):
        log.info("saving patch file.")

        filename = timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S") + ".json"

        (self.file_path / "current" / filename).write_bytes(orjson.dumps(content))

        log.debug("..done.")

    def save_file(self, timestamp: datetime.datetime, content: dict):
        log.debug("save_file: acquiring lock.")
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

        fut = self.executor.submit(self._compress_folder, folder=target_path, store_factory=self.data_store_factory,
                                   preprocessors=self.preprocessors)
        self.future_wait_executor.submit(
            self._wait_for_compression_task,
            future=fut,
        )

    @staticmethod
    def _compress_folder(folder: Path, store_factory: tuple[typing.Callable[[], Store], tuple],
                         preprocessors: list[Preprocessor]):
        logger = log.getChild(folder.stem)

        logger.debug("Starting compression task at %s.", folder)
        store = store_factory[0](*store_factory[1])

        t1 = time.perf_counter()
        n_files = 1
        total_data = 0

        base_timestamp = datetime.datetime.fromisoformat((folder / "base_timestamp.txt").read_text())

        base_file = (folder / "base.json").read_bytes()

        base_file_data = orjson.loads(base_file)
        for preprocessor in preprocessors:
            preprocessor.do(base_file_data)

        prev_file: str = json.dumps(base_file_data)

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
            except Exception:
                logger.error("Could not save compressed file.")
                raise
            else:
                shutil.rmtree(folder)
                logger.info("Done. Deleted folder.")
                return

        file_io = io.BytesIO()
        files = list(folder.iterdir())
        with tarfile.open(fileobj=file_io, mode="w:gz") as tar:
            for i, file in enumerate(sorted(files, key=lambda s: s.name)):
                logger.debug(f"-> %s (%s/%s)", file.name, i + 1, len(files))
                # print(f"-> file {file!s}")
                if file.name == "base_timestamp.txt":
                    continue
                elif file.name == "base.json":
                    # print("-> storing base")
                    data = orjson.dumps(base_file_data)

                    tarinfo = tarfile.TarInfo(file.name)
                    tarinfo.size = len(data)
                    tar.addfile(tarinfo, fileobj=io.BytesIO(data))

                    n_files += 1
                    total_data += len(base_file)
                else:
                    # print("-> storing patch")
                    t1 = time.perf_counter()
                    file_bytes = file.read_bytes()

                    t2 = time.perf_counter()
                    content = orjson.loads(file_bytes)

                    t3 = time.perf_counter()
                    for preprocessor in preprocessors:
                        preprocessor.do(content)

                    t4 = time.perf_counter()
                    # diff = jsonpatch.make_patch(prev_file, content)
                    # diff_bson = orjson.dumps({"": diff.patch})

                    content_dumped = json.dumps(content)
                    diff = fastjsonpatch.compute_patch(prev_file, content_dumped)
                    prev_file = content_dumped

                    t5 = time.perf_counter()
                    # diff_dumped = orjson.dumps({"": diff})
                    diff_dumped = diff.encode("utf-8")

                    t6 = time.perf_counter()
                    tarinfo = tarfile.TarInfo(file.name)
                    tarinfo.size = len(diff_dumped)
                    tar.addfile(tarinfo, fileobj=io.BytesIO(diff_dumped))

                    n_files += 1
                    total_data += len(diff_dumped)

                    t7 = time.perf_counter()
                    logger.debug(
                        f"\n"
                        f"Read: {t2 - t1:.2f} s, \n"
                        f"Load: {t3 - t2:.2f} s, \n"
                        f"Preprocess: {t4 - t3:.2f} s, \n"
                        f"Diff: {t5 - t4:.2f} s, \n"
                        # f"Dump: {t6 - t5:.2f} s, \n"
                        f"Store: {t7 - t6:.2f} s, \n"
                        f"Total: {t7 - t1:.2f} s"
                    )

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

        type_, timestamp, out = next(it)
        assert type_ == "base"

        out_dict = orjson.loads(out)
        for preprocessor in reversed(self.preprocessors):
            preprocessor.undo(out_dict)

        yield timestamp, out_dict

        for preprocessor in reversed(self.preprocessors):
            preprocessor.do(out_dict)

        for type_, timestamp, patch in it:
            assert type_ == "patch"

            jsonpatch.JsonPatch(orjson.loads(patch)).apply(out_dict, in_place=True)
            # out = fastjsonpatch.apply_patch(out, patch)

            # out_dict = orjson.loads(out)
            for preprocessor in reversed(self.preprocessors):
                preprocessor.undo(out_dict)

            yield timestamp, out_dict

            for preprocessor in reversed(self.preprocessors):
                preprocessor.do(out_dict)

    def iter_archive_raw(
        self,
        timestamp: datetime.datetime
    ) -> typing.Generator[tuple[str, datetime.datetime, str], None, None]:
        base_timestamp = timestamp.astimezone(datetime.timezone.utc)

        file = self._get_data_store().read_file(base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")

        with tarfile.open(fileobj=io.BytesIO(file), mode="r:gz") as tar:
            base_content = tar.extractfile("base.json").read().decode("utf-8")
            # base_file = orjson.loads(base_content)
            yield "base", base_timestamp, base_content

            for file in sorted(tar.getmembers(), key=lambda m: m.name):
                if file.name == "base.json":
                    continue

                timestamp_str, _ = file.name.split(".")

                timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S").replace(
                    tzinfo=datetime.timezone.utc)

                with tar.extractfile(file) as f:
                    file_content = f.read().decode("utf-8")
                    # json_patch = list(orjson.loads(file_content).values())[0]
                    # json_patch = orjson.loads(file_content)

                    yield "patch", timestamp, file_content

    def decompress(self, base_timestamp: datetime.datetime):
        for type_, timestamp, data in self.iter_archive_raw(base_timestamp):
            out_filepath = self.file_path / "decompressed" / base_timestamp.strftime(
                "%Y-%m-%dT%H-%M-%S") / timestamp.strftime(
                type_ + "-%Y-%m-%dT%H-%M-%S.json")

            out_filepath.parent.mkdir(parents=True, exist_ok=True)

            out_filepath.write_text(data)

    def _get_data_store(self):
        if self._data_store is None:
            self._data_store = self.data_store_factory[0](*self.data_store_factory[1])

        return self._data_store

    def iter_base_files_timestamps(self):
        for file_name in self._get_data_store().iterdir():
            try:
                timestamp_str, _ = file_name.split(".", 1)
                yield datetime.datetime.strptime(
                    timestamp_str, "%Y-%m-%dT%H-%M-%S"
                ).replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                print(f"Cache data dir contains invalid file {file_name!s}. Skipping.")
                continue

    def iter_files(
        self,
        since: datetime.datetime,
        until: datetime.datetime
    ) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        for timestamp in self.iter_base_files_timestamps():
            if since < timestamp < until:
                try:
                    yield from self.iter_archive(timestamp)
                except (tarfile.ReadError, EOFError):
                    print(f"Could not read archive for {timestamp!s}. Skipping.")


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
