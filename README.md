# Entur-Buspulse
Medallion architecture on getting analyzing and delivering results from Entur Data Apis (public).

## Bronze collectors

Three independent collectors for Kolumbus (codespace `KOL`), each in its own
container with its own `ET-Client-Name`:

| Collector | Feed | Every | Saves |
|---|---|---|---|
| `vehicle_positions` | Vehicles GraphQL – live bus positions | 30 s | every response (`.json`) |
| `estimated_timetable` | SIRI ET – arrivals/departures per stop | 60 s | every response (`.xml`) |
| `service_alerts` | SIRI SX – disruptions | 60 s | only when alerts change (`.xml`) |

```
collectors/   one script per feed (client name + interval at the top of each file)
common/
  config.py        shared settings + file naming convention
  entur_client.py  the only code that calls Entur
  logger.py        start/stop + error logging
```

### Run

```bash
docker compose up -d --build
```

Locally without Docker:

```bash
pip install -r requirements.txt
DATA_DIR=./data LOG_DIR=./logs python -m collectors.vehicle_positions
```

### Data (bronze = raw responses, untouched)

```
data/<feed>/date=YYYY-MM-DD/<feed>_YYYYMMDDTHHMMSSZ.<json|xml>     (UTC)
```

### Logs

`logs/<feed>.log` contains only `started` / `stopped` lines and errors, so the
time between a `stopped` and the next `started` is downtime. Two `started` lines
in a row mean the collector was killed hard (crash, `kill -9`, power loss); the
last data file before the second `started` shows when it stopped.
