"""
BusPulse - fetch Kolumbus bus data from Entur, clean it, save it in a folder.

Feeds (all open, no API key, only the ET-Client-Name header is required):
  et        SIRI ET  (XML)   real-time arrival/departure estimates per stop -> delays
  vehicles  GraphQL  (JSON)  live bus positions, delay, occupancy           -> map
  sx        SIRI SX  (XML)   service alerts / disruptions                   -> alerts
  gtfs      GTFS     (ZIP)   static timetable: stops, routes, trips, times  -> names + coords

Usage:
  python entur_fetch.py                 # fetch et + vehicles + sx once
  python entur_fetch.py --gtfs          # also download the static timetable
  python entur_fetch.py --loop          # keep fetching (et/vehicles every 60 s, sx every 5 min)
  python entur_fetch.py --out mydata    # choose output folder (default: data)

Output:
  data/raw/<feed>/<timestamp>.xml|json   exactly what Entur returned
  data/clean/<feed>/<timestamp>.csv      flat table, one row per stop call / bus / alert
  data/gtfs/*.txt                        GTFS tables (these are already CSV files)

Only needs: pip install requests   (Python 3.11+)
"""

import argparse
import csv
import io
import json
import time
import uuid
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

CLIENT_NAME = "uis-buspulse"
CODESPACE = "KOL"  # Kolumbus

ET_URL = "https://api.entur.io/realtime/v1/rest/et"
SX_URL = "https://api.entur.io/realtime/v1/rest/sx"
VEHICLES_URL = "https://api.entur.io/realtime/v2/vehicles/graphql"
GTFS_URL = "https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_kol-aggregated-gtfs.zip"

NS = {"s": "http://www.siri.org.uk/siri"}

HEADERS = {"ET-Client-Name": CLIENT_NAME}

session = requests.Session()
session.headers.update(HEADERS)



def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def text(elem, path: str):
    """Text of the first matching child, or None."""
    found = elem.find(path, NS)
    return found.text.strip() if found is not None and found.text else None


def seconds_between(aimed, actual):
    """actual - aimed in seconds (positive = late)."""
    if not aimed or not actual:
        return None
    return int((datetime.fromisoformat(actual) - datetime.fromisoformat(aimed)).total_seconds())


def save(out: Path, feed: str, stamp: str, raw: bytes, ext: str, rows: list[dict]):
    raw_dir = out / "raw" / feed
    clean_dir = out / "clean" / feed
    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)

    (raw_dir / f"{stamp}.{ext}").write_bytes(raw)

    csv_path = clean_dir / f"{stamp}.csv"
    if rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    print(f"  {feed:9s} {len(rows):6d} rows -> {csv_path}")


def get_siri(url: str) -> bytes:
    """GET a full SIRI Lite snapshot for Kolumbus (fresh requestorId = full data, not just changes)."""
    params = {"datasetId": CODESPACE, "requestorId": str(uuid.uuid4()), "maxSize": 10000}
    resp = session.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.content


# ---------------------------------------------------------------- ET: delays per stop

def parse_et(xml_bytes: bytes) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    rows = []
    for j in root.iter(f"{{{NS['s']}}}EstimatedVehicleJourney"):
        journey = {
            "line_ref": text(j, "s:LineRef"),
            "line_name": text(j, "s:PublishedLineName"),
            "direction": text(j, "s:DirectionRef"),
            "journey_ref": text(j, "s:FramedVehicleJourneyRef/s:DatedVehicleJourneyRef")
                           or text(j, "s:DatedVehicleJourneyRef"),
            "operating_date": text(j, "s:FramedVehicleJourneyRef/s:DataFrameRef"),
            "vehicle_ref": text(j, "s:VehicleRef"),
            "journey_cancelled": text(j, "s:Cancellation") == "true",
        }
        # RecordedCalls = stops already passed (actual times)
        for c in j.findall("s:RecordedCalls/s:RecordedCall", NS):
            aimed_dep, actual_dep = text(c, "s:AimedDepartureTime"), text(c, "s:ActualDepartureTime")
            aimed_arr, actual_arr = text(c, "s:AimedArrivalTime"), text(c, "s:ActualArrivalTime")
            rows.append({
                **journey,
                "stop_ref": text(c, "s:StopPointRef"),
                "stop_order": text(c, "s:Order"),
                "status": "recorded",
                "aimed_arrival": aimed_arr, "real_arrival": actual_arr,
                "aimed_departure": aimed_dep, "real_departure": actual_dep,
                "delay_s": seconds_between(aimed_dep, actual_dep) if actual_dep
                           else seconds_between(aimed_arr, actual_arr),
                "stop_cancelled": text(c, "s:Cancellation") == "true",
            })
        # EstimatedCalls = stops still ahead (predicted times)
        for c in j.findall("s:EstimatedCalls/s:EstimatedCall", NS):
            aimed_dep, exp_dep = text(c, "s:AimedDepartureTime"), text(c, "s:ExpectedDepartureTime")
            aimed_arr, exp_arr = text(c, "s:AimedArrivalTime"), text(c, "s:ExpectedArrivalTime")
            rows.append({
                **journey,
                "stop_ref": text(c, "s:StopPointRef"),
                "stop_order": text(c, "s:Order"),
                "status": "estimated",
                "aimed_arrival": aimed_arr, "real_arrival": exp_arr,
                "aimed_departure": aimed_dep, "real_departure": exp_dep,
                "delay_s": seconds_between(aimed_dep, exp_dep) if exp_dep
                           else seconds_between(aimed_arr, exp_arr),
                "stop_cancelled": text(c, "s:Cancellation") == "true",
            })
    return rows


# ---------------------------------------------------------------- SX: service alerts

def parse_sx(xml_bytes: bytes) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    rows = []
    for s in root.iter(f"{{{NS['s']}}}PtSituationElement"):
        lines = sorted({e.text for e in s.iter(f"{{{NS['s']}}}LineRef") if e.text})
        stops = sorted({e.text for e in s.iter(f"{{{NS['s']}}}StopPointRef") if e.text})
        rows.append({
            "situation_id": text(s, "s:SituationNumber"),
            "created": text(s, "s:CreationTime"),
            "progress": text(s, "s:Progress"),
            "severity": text(s, "s:Severity"),
            "report_type": text(s, "s:ReportType"),
            "valid_from": text(s, "s:ValidityPeriod/s:StartTime"),
            "valid_to": text(s, "s:ValidityPeriod/s:EndTime"),
            "summary": text(s, "s:Summary"),
            "description": text(s, "s:Description"),
            "affected_lines": ";".join(lines),
            "affected_stops": ";".join(stops),
        })
    return rows


# ---------------------------------------------------------------- Vehicles: live positions

VEHICLES_QUERY = """
{
  vehicles(codespaceId: "%s") {
    vehicleId
    lastUpdated
    line { lineRef lineName publicCode }
    location { latitude longitude }
    bearing
    speed
    delay
    occupancyStatus
  }
}
""" % CODESPACE


def get_vehicles() -> bytes:
    resp = session.post(VEHICLES_URL, json={"query": VEHICLES_QUERY}, timeout=60)
    resp.raise_for_status()
    return resp.content


def parse_vehicles(json_bytes: bytes) -> list[dict]:
    data = json.loads(json_bytes)
    rows = []
    for v in (data.get("data") or {}).get("vehicles") or []:
        line = v.get("line") or {}
        loc = v.get("location") or {}
        rows.append({
            "vehicle_id": v.get("vehicleId"),
            "last_updated": v.get("lastUpdated"),
            "line_ref": line.get("lineRef"),
            "line_name": line.get("lineName"),
            "line_number": line.get("publicCode"),
            "lat": loc.get("latitude"),
            "lon": loc.get("longitude"),
            "bearing": v.get("bearing"),
            "speed": v.get("speed"),
            "delay_s": v.get("delay"),
            "occupancy": v.get("occupancyStatus"),
        })
    return rows


# ---------------------------------------------------------------- GTFS: static timetable

def download_gtfs(out: Path):
    resp = session.get(GTFS_URL, timeout=300)
    resp.raise_for_status()
    gtfs_dir = out / "gtfs"
    gtfs_dir.mkdir(parents=True, exist_ok=True)
    (out / "raw").mkdir(parents=True, exist_ok=True)
    (out / "raw" / f"gtfs_{now_stamp()}.zip").write_bytes(resp.content)
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        z.extractall(gtfs_dir)
        names = z.namelist()
    print(f"  gtfs      {len(names)} files -> {gtfs_dir} ({', '.join(names)})")


# ---------------------------------------------------------------- main

def fetch_once(out: Path, include_sx: bool):
    stamp = now_stamp()
    print(f"[{stamp}]")
    jobs = [("et", lambda: get_siri(ET_URL), parse_et, "xml"),
            ("vehicles", get_vehicles, parse_vehicles, "json")]
    if include_sx:
        jobs.append(("sx", lambda: get_siri(SX_URL), parse_sx, "xml"))
    for feed, fetch, parse, ext in jobs:
        try:
            raw = fetch()
            save(out, feed, stamp, raw, ext, parse(raw))
        except Exception as e:  # keep going if one feed fails
            print(f"  {feed:9s} FAILED: {e}")


def main():
    ap = argparse.ArgumentParser(description="Fetch Kolumbus data from Entur")
    ap.add_argument("--out", default="data", help="output folder (default: data)")
    ap.add_argument("--gtfs", action="store_true", help="also download the static GTFS timetable")
    ap.add_argument("--loop", action="store_true", help="keep fetching until Ctrl+C")
    args = ap.parse_args()
    out = Path(args.out)

    if args.gtfs:
        try:
            download_gtfs(out)
        except Exception as e:
            print(f"  gtfs      FAILED: {e}")

    if not args.loop:
        fetch_once(out, include_sx=True)
        return

    # Entur allows max 4 requests/min per SIRI Lite feed: 60 s for ET is safe.
    i = 0
    try:
        while True:
            fetch_once(out, include_sx=(i % 5 == 0))
            i += 1
            time.sleep(60)
    except KeyboardInterrupt:
        print("Stopped.")


if __name__ == "__main__":
    main()
