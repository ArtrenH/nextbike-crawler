from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime
import io
import json
import logging
import multiprocessing
import random
import shutil
import tarfile
import time
import typing
from pathlib import Path

import jsonpatch
import orjson
import fastjsonpatch

import paramiko

from upath import UPath

import pipifax_io.file_pid_lock
from .preprocessors import Preprocessor

log = logging.getLogger("Cache")

__all__ = [
    "JsonStore",
    "WritableJsonStore",
    "JsonCache",
]


@dataclasses.dataclass
class JsonStore:
    data_store: UPath
    preprocessors: list[Preprocessor] = dataclasses.field(default_factory=list)

    def iter_archive(
        self,
        timestamp: datetime.datetime
    ) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        it = self.iter_archive_raw(timestamp)

        yield from self._iter_archive(it)

    def _iter_archive(
        self,
        it: typing.Iterator[tuple[str, datetime.datetime, str]]
    ) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
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

        for _i in range(4):
            try:
                # file = self._get_file(self.data_store / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz"))
                file = (self.data_store / (base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz")).read_bytes()
            except paramiko.ssh_exception.SSHException:
                if _i == 3:
                    raise
                log.error(f"Could not read archive. Retrying... ({_i + 1}/3")
                time.sleep(2)
            else:
                break

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

    def _iter_archive_raw_list(self, timestamp: datetime.datetime):
        return list(self.iter_archive_raw(timestamp))

    def iter_base_files_timestamps(self):
        for file_path in self.data_store.iterdir():
            try:
                timestamp_str, _ = file_path.name.split(".", 1)
                yield datetime.datetime.strptime(
                    timestamp_str, "%Y-%m-%dT%H-%M-%S"
                ).replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                print(f"Cache data dir contains invalid file {file_path.name!s}. Skipping.")
                continue

    def iter_files(
        self,
        since: datetime.datetime,
        until: datetime.datetime,
        executor: concurrent.futures.Executor | None = None,
    ) -> typing.Generator[tuple[datetime.datetime, dict], None, None]:
        if executor is None:
            for timestamp in self.iter_base_files_timestamps():
                if since < timestamp < until:
                    try:
                        yield from self.iter_archive(timestamp)
                    except (tarfile.ReadError, EOFError):
                        print(f"Could not read archive for {timestamp!s}. Skipping.")
        else:
            stamps = [t for t in self.iter_base_files_timestamps() if since < t < until]

            futs: list[tuple[datetime.datetime, concurrent.futures.Future]] = []

            for timestamp in stamps:
                futs.append(
                    (
                        timestamp,
                        executor.submit(self._iter_archive_raw_list, timestamp)
                    )
                )

            for timestamp, fut in futs:
                try:
                    raw_files = fut.result()
                except (tarfile.ReadError, EOFError):
                    print(f"Could not read archive for {timestamp!s}. Skipping.")
                    continue
                except Exception:
                    executor.shutdown(cancel_futures=True)
                    raise
                finally:
                    fut._result = None

                yield from self._iter_archive(iter(raw_files))

    def decompress(self, base_timestamp: datetime.datetime, out_path: UPath):
        for type_, timestamp, data in self.iter_archive_raw(base_timestamp):
            out_filepath = out_path / base_timestamp.strftime(
                "%Y-%m-%dT%H-%M-%S") / timestamp.strftime(
                type_ + "-%Y-%m-%dT%H-%M-%S.json")

            out_filepath.parent.mkdir(parents=True, exist_ok=True)

            out_filepath.write_text(data)

    def _compress_folder(
        self,
        folder: Path,
    ):
        logger = log.getChild(folder.stem)

        logger.debug("Starting compression task at %s.", folder)

        t1 = time.perf_counter()
        n_files = 0
        total_data = 0

        try:
            base_timestamp = datetime.datetime.fromisoformat((folder / "base_timestamp.txt").read_text())
        except FileNotFoundError:
            log.error(f"Missing base_timestamp.txt in {folder!s}.")
            return

        filename = base_timestamp.strftime("%Y-%m-%dT%H-%M-%S") + ".tar.gz"
        logger.info("Compressing to %s.", filename)

        try:
            compressed_file = (folder / "compressed.tar.xz").read_bytes()
        except FileNotFoundError:
            pass
        else:
            logger.info("Folder was already compressed.")
            try:
                (self.data_store / filename).write_bytes(compressed_file)
            except Exception:
                logger.error("Could not save compressed file.")
                raise
            else:
                shutil.rmtree(folder)
                logger.info("Done. Deleted folder.")
                return

        try:
            base_file = (folder / "base.json").read_bytes()
        except FileNotFoundError:
            logger.error(f"Missing base.json in {folder!s}.")
            return

        base_file_data = orjson.loads(base_file)
        for preprocessor in self.preprocessors:
            preprocessor.do(base_file_data)

        prev_file: str = json.dumps(base_file_data)

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
                    for preprocessor in self.preprocessors:
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
                    total_data += len(file_bytes)

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
            (self.data_store / filename).write_bytes(file_io.getvalue())
        except Exception:
            logger.error("Could not store compressed file. Trying to save into compression task.")

            (folder / "compressed.tar.xz").write_bytes(file_io.getvalue())

            logger.debug("Deleting json files.")
            for file in folder.iterdir():
                if file.name.endswith(".json"):
                    file.unlink()

            raise

        shutil.rmtree(folder)
        logger.info("Done. Deleted folder.")


@dataclasses.dataclass
class WritableJsonStore:
    json_store: JsonStore
    cache_dir_path: Path
    executor: concurrent.futures.Executor

    def compress_folder(self) -> concurrent.futures.Future:
        log.debug("Starting to compress current folder.")
        target_path = self.cache_dir_path / "compression" / random.randbytes(16).hex()
        target_path.parent.mkdir(parents=True, exist_ok=True)

        (self.cache_dir_path / "current").rename(target_path)

        return self.executor.submit(
            self.json_store._compress_folder,
            folder=target_path,
        )

    def recover_lost_compression_tasks(
        self,
    ) -> list[concurrent.futures.Future]:
        file_path = self.cache_dir_path / "compression"
        file_path.mkdir(parents=True, exist_ok=True)
        out_futs = []
        for folder in file_path.iterdir():
            if not folder.is_dir():
                log.error(f"Unexpected file in compression folder: {folder!s}")
                continue

            log.info(f"Recovering lost compression task at {folder!s}.")
            out_futs.append(self.executor.submit(
                self.json_store._compress_folder,
                folder=folder,
            ))

        return out_futs

    def save_base_file(
        self,
        timestamp: datetime.datetime,
        content: dict,
    ) -> concurrent.futures.Future:
        if (self.cache_dir_path / "current" / "base_timestamp.txt").exists():
            out = self.compress_folder()
        else:
            out = concurrent.futures.Future()
            out.set_result(None)

        log.info("saving base file.")
        folder_path = self.cache_dir_path / "current"

        folder_path.mkdir(parents=True, exist_ok=True)
        filename = "base.json"

        (folder_path / filename).write_bytes(orjson.dumps(content))
        (self.cache_dir_path / "current" / "base_timestamp.txt").write_text(
            timestamp.astimezone(datetime.timezone.utc).isoformat()
        )
        log.debug("..done.")

        return out

    def save_diff_file(
        self,
        timestamp: datetime.datetime,
        content: dict,
        # cache_dir_path: Path,
    ):
        log.info("saving patch file.")

        filename = timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S") + ".json"

        (self.cache_dir_path / "current" / filename).write_bytes(orjson.dumps(content))

        log.debug("..done.")


@dataclasses.dataclass
class JsonCache[* T]:
    writable_json_store: WritableJsonStore
    future_wait_executor: concurrent.futures.ThreadPoolExecutor

    base_file_creation_interval: int = 30 * 60
    _last_known_timestamp: datetime.datetime | None = dataclasses.field(
        default=None,
        init=False,
    )

    _fs_lock: multiprocessing.Lock = dataclasses.field(init=False, default_factory=multiprocessing.Lock)

    def init(self):
        self.acquire_cache_lock()
        futs = self.writable_json_store.recover_lost_compression_tasks()
        for fut in futs:
            self.future_wait_executor.submit(
                self._wait_for_compression_task,
                fut,
            )

    def acquire_cache_lock(self):
        pipifax_io.file_pid_lock.FilePidLock(self.writable_json_store.cache_dir_path / "lock").acquire()

    @staticmethod
    def _wait_for_compression_task(future: concurrent.futures.Future):
        try:
            future.result()
        except Exception:
            log.error("Compression task failed.", exc_info=True)
            raise
        else:
            log.info("Compression task finished successfully.")

    def save_file(
        self,
        timestamp: datetime.datetime,
        content: dict
    ):
        if self._last_known_timestamp is None:
            fut = self.writable_json_store.save_base_file(timestamp, content)
            self.future_wait_executor.submit(
                self._wait_for_compression_task,
                fut,
            )
            self._last_known_timestamp = timestamp
            return

        assert self._last_known_timestamp < timestamp, (
            f"Timestamp to be saved ({timestamp!s}) older than "
            f"last known timestamp ({self._last_known_timestamp!s})."
        )

        base_file_creation_file_interval_passed = (
            int(timestamp.timestamp()) // self.base_file_creation_interval
            !=
            int(self._last_known_timestamp.timestamp()) // self.base_file_creation_interval
        )

        if base_file_creation_file_interval_passed:
            fut = self.writable_json_store.save_base_file(timestamp, content)
            self.future_wait_executor.submit(
                self._wait_for_compression_task,
                fut,
            )
        else:
            self.writable_json_store.save_diff_file(timestamp, content)

        self._last_known_timestamp = timestamp
