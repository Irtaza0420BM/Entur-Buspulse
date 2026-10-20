"""Vehicle positions: where every Kolumbus bus is right now (Entur Vehicles GraphQL).

Saves the raw JSON response every INTERVAL seconds to
/data/vehicle_positions/date=YYYY-MM-DD/vehicle_positions_<UTC time>.json

Run: python -m collectors.vehicle_positions
"""
import signal
import sys
import time

from common import config
from common.entur_client import EnturClient
from common.logger import get_logger

NAME = "vehicle_positions"
CLIENT_NAME = "uis-buspulse-vehicle-positions"
INTERVAL = 30  # seconds

QUERY = """
{
  vehicles(codespaceId: "%s") {
    vehicleId lastUpdated
    line { lineRef lineName publicCode }
    location { latitude longitude }
    bearing speed delay occupancyStatus
  }
}
""" % config.CODESPACE

log = get_logger(NAME)


def collect(client: EnturClient):
    payload = client.post_graphql(config.VEHICLES_URL, QUERY)
    config.bronze_path(NAME, "json").write_bytes(payload)


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
