import os
import json
from io import BytesIO
from datetime import datetime, timedelta

import requests
import numpy as np
import pytz
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dotenv import load_dotenv
from fs import open_fs


load_dotenv()

CACHE_FS_URL = os.getenv("CACHE_FS_URL")
german_tz = pytz.timezone("Europe/Berlin")
utc = pytz.utc


def get_delay_map(min_time=None, max_time=None):
    delay_map = {}

    fs = open_fs(CACHE_FS_URL)
    for path in fs.listdir("/"):
        if not path.endswith(".tar.gz"):
            continue

        try:
            filename_time = datetime.strptime(path[:19], "%Y-%m-%dT%H-%M-%S").replace(tzinfo=utc)
        except ValueError:
            continue

        if min_time is not None and filename_time < min_time:
            continue
        if max_time is not None and filename_time > max_time:
            continue

        info = fs.getinfo(path, namespaces=["details"])
        mtime = info.get("details", "modified", default=None)

        if mtime is None:
            continue

        upload_time_utc = german_tz.localize(datetime.fromtimestamp(mtime)).astimezone(utc)

        delay = (upload_time_utc - filename_time - timedelta(minutes=30)).total_seconds() / 60
        delay_map[filename_time.isoformat()] = delay

    return delay_map


def plot_delay_map(delay_map, title="FTP Upload Delay Over Time with Linear Trend"):
    plt.clf()  # Clears the current figure
    plt.cla()  # Clears the current axes
    plt.close()  # Closes the current figure
    timestamps = [datetime.fromisoformat(ts) for ts in sorted(delay_map.keys())]
    delays = [delay_map[ts.isoformat()] for ts in timestamps]

    x = mdates.date2num(timestamps)
    y = np.array(delays)

    slope, intercept = np.polyfit(x, y, 1)
    regression_line = slope * x + intercept

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(timestamps, delays, marker='o', label='Actual Delay')
    ax.plot(timestamps, regression_line, linestyle='--', color='blue', label='Linear Trend')

    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))

    equation_text = f"Slope: {slope:.4f} min/day"
    ax.text(0.02, 0.15, equation_text, transform=ax.transAxes, fontsize=10, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    ax.axhline(30, color='red', linestyle='--', label='critical threshold')
    ax.axhline(0, color='gray', linestyle='--', label='minimum')
    ax.set_title(title)
    ax.set_xlabel("File Timestamp (UTC)")
    ax.set_ylabel("Upload Delay (minutes)")
    ax.grid(True)
    fig.autofmt_xdate()
    ax.legend(loc='lower right')

    plt.tight_layout()


def send_current_picture():
    buf = BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)  # Move to start of buffer

    webhook_url = os.getenv("DELAY_MONITOR_WEBHOOK")

    embed = {
        "title": "📊 Plot Results",
        "description": "Here's your latest generated plot.",
        "color": 0x3498db,  # Nice blue
        "image": {
            "url": "attachment://plot.png"
        }
    }

    payload = {
        "username": "PlotBot",
        "content": "Today's plot",
        "embeds": [embed]
    }

    files = {
        "file": ("plot.png", buf, "image/png")
    }

    response = requests.post(
        webhook_url,
        data={
            "payload_json": json.dumps(payload)
        },
        files=files
    )


def plot_last_day():
    now = datetime.now(utc)
    delay_map = get_delay_map(now - timedelta(hours=24, minutes=29), now)
    twelve_hours_ago = datetime.now() - timedelta(hours=12)
    formatted = twelve_hours_ago.strftime("%Y-%m-%d")
    title = f"Upload delay for {formatted}"
    plot_delay_map(delay_map, title)


if __name__ == "__main__":
    plot_last_day()
    send_current_picture()

