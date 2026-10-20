"""Bronze ingest: static GTFS timetable. Each .txt in the ZIP becomes one bronze table
(all columns kept as strings, nothing cleaned). Usage: python bronze_gtfs.py"""
import io
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from common import BRONZE_DIR, GTFS_URL, get_logger, get_spark, session

log = get_logger("bronze_gtfs")


def main():
    spark = get_spark("bronze_gtfs")
    now = datetime.now(timezone.utc)

    try:
        resp = session.get(GTFS_URL, timeout=300)
        resp.raise_for_status()
    except Exception:
        log.exception("gtfs download FAILED")
        return

    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            z.extractall(tmp)

        for txt in sorted(Path(tmp).glob("*.txt")):
            table = f"entur_gtfs_{txt.stem}"  # e.g. entur_gtfs_stops
            try:
                df = (spark.read.option("header", True).option("inferSchema", False)
                      .csv(str(txt))
                      .withColumn("ingest_ts", F.lit(now.isoformat()).cast("timestamp"))
                      .withColumn("ingest_date", F.lit(now.strftime("%Y-%m-%d")))
                      .withColumn("source_url", F.lit(GTFS_URL)))
                df.write.mode("append").partitionBy("ingest_date").parquet(str(BRONZE_DIR / table))
                log.info("%s ingested (%d rows)", table, df.count())
            except Exception:
                log.exception("%s FAILED", table)


if __name__ == "__main__":
    main()