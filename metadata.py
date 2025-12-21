# Tools to analyze delays, filesizes and incomplete data

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from upath import UPath

load_dotenv()


def get_filenames():
    return [entry.name for entry in UPath(os.getenv("CACHE_FS_URL_ANALYZER")).iterdir()]


class MissingIntervalFinder:
    @staticmethod
    def parse_start_time_from_filename(name: str) -> datetime:
        """
        Extract start datetime from a filename like
        '2025-06-23T16-30-00.tar.gz'.
        """
        base = name.split(".tar.gz")[0]  # strip extension
        # base is e.g. '2025-06-23T16-30-00'
        return datetime.strptime(base, "%Y-%m-%dT%H-%M-%S")

    @staticmethod
    def next_full_half_hour(dt: datetime) -> datetime:
        """
        Given a datetime, return the next full half-hour boundary.
        Example:
        16:30:00 -> 17:00:00
        16:42:00 -> 17:00:00
        16:05:00 -> 16:30:00
        """
        minute = dt.minute
        if minute == 0 or minute == 30:
            # already on a boundary -> next one
            minute_block = minute + 30
        else:
            minute_block = 30 if minute < 30 else 60

        if minute_block == 60:
            # roll to next hour
            dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            dt = dt.replace(minute=minute_block, second=0, microsecond=0)

        return dt

    @staticmethod
    def intervals_from_filenames(filenames):
        """
        Turn filenames into a list of (start, end) intervals.
        """
        intervals = []
        for name in filenames:
            if not name.endswith(".tar.gz"):
                continue
            start = MissingIntervalFinder.parse_start_time_from_filename(name)
            end = MissingIntervalFinder.next_full_half_hour(start)
            intervals.append((start, end))
        # sort by start time
        intervals.sort(key=lambda x: x[0])
        return intervals

    @staticmethod
    def find_missing_intervals(filenames, overall_start=None, overall_end=None):
        """
        Given a list of filenames, return a list of gaps (start, end) where
        no file covers the time.

        overall_start / overall_end:
        Optional datetime bounds. If not given, the range is determined
        from the earliest start and latest end found.
        """
        intervals = MissingIntervalFinder.intervals_from_filenames(filenames)
        if not intervals:
            return []

        # establish global coverage bounds
        first_start = intervals[0][0]
        last_end = max(e for _, e in intervals)

        if overall_start is None:
            overall_start = first_start
        if overall_end is None:
            overall_end = last_end

        # clamp intervals to [overall_start, overall_end]
        clamped = []
        for s, e in intervals:
            if e <= overall_start or s >= overall_end:
                continue
            clamped.append((max(s, overall_start), min(e, overall_end)))

        if not clamped:
            return [(overall_start, overall_end)]

        clamped.sort(key=lambda x: x[0])

        missing = []
        current_end = overall_start

        for s, e in clamped:
            if s > current_end:
                # gap detected
                missing.append((current_end, s))
            current_end = max(current_end, e)

        if current_end < overall_end:
            missing.append((current_end, overall_end))

        return missing


def main():
    filenames = get_filenames()
    gaps = MissingIntervalFinder.find_missing_intervals(filenames)
    for start, end in gaps:
        print(start, "->", end)


if __name__ == "__main__":
    main()
