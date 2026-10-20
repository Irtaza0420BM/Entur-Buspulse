"""Bronze ingest: live bus positions (GraphQL) -> raw JSON.
Usage: python bronze_vehicles.py [--loop]"""
import argparse
import time

from common import CODESPACE, VEHICLES_URL, get_logger, get_spark, session, write_bronze

log = get_logger("bronze_vehicles")

QUERY = """
{
  vehicles(codespaceId: "%s") {
    vehicleId lastUpdated
    line { lineRef lineName publicCode }
    location { latitude longitude }
    bearing speed delay occupancyStatus
  }
}
""" % CODESPACE


def ingest(spark):
    try:
        resp = session.post(VEHICLES_URL, json={"query": QUERY}, timeout=60)
        resp.raise_for_status()
        write_bronze(spark, "entur_vehicles", resp.text, VEHICLES_URL, resp.status_code)
        log.info("entur_vehicles ingested (%d bytes)", len(resp.content))
    except Exception:
        log.exception("entur_vehicles FAILED")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    args = ap.parse_args()
    spark = get_spark("bronze_vehicles")

    while True:
        ingest(spark)
        if not args.loop:
            break
        time.sleep(60)


if __name__ == "__main__":
    main()