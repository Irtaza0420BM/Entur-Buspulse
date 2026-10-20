import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType, IntegerType

CLIENT_NAME = "uis-buspulse"
CODESPACE = "KOL"  # Kolumbus

ET_URL = "https://api.entur.io/realtime/v1/rest/et"
SX_URL = "https://api.entur.io/realtime/v1/rest/sx"
VEHICLES_URL = "https://api.entur.io/realtime/v2/vehicles/graphql"
GTFS_URL = "https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_kol-aggregated-gtfs.zip"

BASE_DIR = Path(__file__).parent
BRONZE_DIR = BASE_DIR / "data" 
LOG_DIR = BASE_DIR / "logging"

session = requests.Session()
session.headers.update({"ET-Client-Name": CLIENT_NAME})


def get_logger(script_name: str) -> logging.Logger:
    """One log file per script + a shared errors.log that only receives ERRORs."""
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(script_name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")

    handlers = [
        logging.FileHandler(LOG_DIR / f"{script_name}.log", encoding="utf-8"),
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "errors.log", encoding="utf-8"),
    ]
    handlers[2].setLevel(logging.ERROR)
    for h in handlers:
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


def get_spark(app_name: str) -> SparkSession:
    spark = (SparkSession.builder.appName(app_name).master("local[*]")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_bronze(spark, table: str, payload: str, source_url: str, http_status: int):
    """Append one raw payload as one row: untouched payload + ingestion metadata."""
    now = datetime.now(timezone.utc)
    schema = StructType([
        StructField("ingest_ts", TimestampType()),
        StructField("ingest_date", StringType()),
        StructField("source_url", StringType()),
        StructField("http_status", IntegerType()),
        StructField("payload", StringType()),
    ])
    df = spark.createDataFrame(
        [(now, now.strftime("%Y-%m-%d"), source_url, http_status, payload)], schema)
    df.write.mode("append").partitionBy("ingest_date").parquet(str(BRONZE_DIR / table))

import csv
import json

STATUS_FILE = LOG_DIR / "feed_status.json"
EVENTS_FILE = LOG_DIR / "feed_events.csv"


def record_status(feed: str, ok: bool, log: logging.Logger, reason: str = ""):
    """Call after every poll. Writes an event only when a feed goes DOWN or comes back UP."""
    LOG_DIR.mkdir(exist_ok=True)
    now = datetime.now(timezone.utc)
    state = json.loads(STATUS_FILE.read_text()) if STATUS_FILE.exists() else {}
    prev = state.get(feed, {"status": "UP", "since": now.isoformat()})
    new_status = "UP" if ok else "DOWN"

    event = downtime = None
    if prev["status"] == "UP" and new_status == "DOWN":
        event = "STOPPED"
        state[feed] = {"status": "DOWN", "since": now.isoformat()}
        log.error("%s STOPPED getting data: %s", feed, reason)
    elif prev["status"] == "DOWN" and new_status == "UP":
        event = "RESUMED"
        downtime = int((now - datetime.fromisoformat(prev["since"])).total_seconds())
        state[feed] = {"status": "UP", "since": now.isoformat()}
        log.info("%s RESUMED after %d s", feed, downtime)
    else:
        state.setdefault(feed, prev)  # no change, nothing to record

    STATUS_FILE.write_text(json.dumps(state, indent=2))

    if event:
        is_new = not EVENTS_FILE.exists()
        with open(EVENTS_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if is_new:
                w.writerow(["timestamp_utc", "feed", "event", "downtime_seconds", "reason"])
            w.writerow([now.isoformat(), feed, event, downtime, reason])