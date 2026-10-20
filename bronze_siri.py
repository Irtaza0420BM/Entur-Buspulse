"""Bronze ingest: Entur SIRI ET (delays) and SX (alerts) -> raw XML.
Usage: python bronze_siri.py [--loop]"""
import argparse
import time
import uuid

from common import CODESPACE, ET_URL, SX_URL, get_logger, get_spark, session, write_bronze

log = get_logger("bronze_siri")

FEEDS = {"entur_siri_et": ET_URL, "entur_siri_sx": SX_URL}


def ingest(spark, table: str, url: str):
    try:
        # fresh requestorId = full snapshot instead of only changes
        params = {"datasetId": CODESPACE, "requestorId": str(uuid.uuid4()), "maxSize": 10000}
        resp = session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        write_bronze(spark, table, resp.text, url, resp.status_code)
        log.info("%s ingested (%d bytes)", table, len(resp.content))
    except Exception:
        log.exception("%s FAILED", table)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    args = ap.parse_args()
    spark = get_spark("bronze_siri")

    i = 0
    while True:
        ingest(spark, "entur_siri_et", ET_URL)
        if i % 5 == 0:  # SX every 5 min, ET every 60 s
            ingest(spark, "entur_siri_sx", SX_URL)
        if not args.loop:
            break
        i += 1
        time.sleep(60)  # Entur limit: 4 requests/min per feed


if __name__ == "__main__":
    main()