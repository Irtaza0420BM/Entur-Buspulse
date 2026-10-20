"""Static timetable (GTFS): the planned schedule — stops, routes, trips, stop times.
Needed to give names and coordinates to the IDs in the real-time feeds.

Downloads the raw GTFS zip once a day (at startup, then every INTERVAL) to
/data/static_timetable/date=YYYY-MM-DD/static_timetable_<UTC time>.zip

Run: python -m collectors.static_timetable
"""
import signal
import sys
import time

from common import config
from common.entur_client import EnturClient
from common.logger import get_logger

NAME = "static_timetable"
CLIENT_NAME = "uis-buspulse-static-timetable"
INTERVAL = 24 * 60 * 60  # once a day

log = get_logger(NAME)


def collect(client: EnturClient):
    payload = client.download(config.GTFS_URL)
    config.bronze_path(NAME, "zip").write_bytes(payload)


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # docker stop -> "stopped" gets logged
    client = EnturClient(CLIENT_NAME)
    log.info("started")
    try:
        while True:
            try:
                collect(client)
            except Exception:
                log.exception("collect failed")
            time.sleep(INTERVAL)
    finally:
        log.info("stopped")


if __name__ == "__main__":
    main()
