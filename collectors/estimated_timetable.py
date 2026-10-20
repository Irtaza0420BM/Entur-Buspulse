"""Estimated timetable (SIRI ET): when each bus arrives/departs at each stop,
planned vs. actual/expected times.

Saves the raw XML response every INTERVAL seconds to
/data/estimated_timetable/date=YYYY-MM-DD/estimated_timetable_<UTC time>.xml

Run: python -m collectors.estimated_timetable
"""
import re
import signal
import sys
import time

from common import config
from common.entur_client import EnturClient
from common.logger import get_logger

NAME = "estimated_timetable"
CLIENT_NAME = "uis-buspulse-estimated-timetable"
INTERVAL = 60  # seconds

log = get_logger(NAME)


def collect(client: EnturClient):
    payload = client.get_siri(config.ET_URL)
    config.bronze_path(NAME, "xml").write_bytes(payload)
    if re.search(rb"MoreData>\s*true\s*<", payload):
        log.error("snapshot was cut off: raise SIRI_MAX_SIZE in common/config.py")


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
