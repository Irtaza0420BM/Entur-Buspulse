"""Logger for the collectors.

Rule: INFO is used ONLY for "started" / "stopped", everything else is ERROR.
So a log file is a list of start/stop times plus the errors in between.
Output: stdout (docker compose logs) and /logs/<name>.log, timestamps in UTC.
"""
import logging
import time

from common import config


def get_logger(name: str) -> logging.Logger:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
                            datefmt="%Y-%m-%dT%H:%M:%SZ")
    fmt.converter = time.gmtime  # UTC, same as the data file names

    for handler in (logging.StreamHandler(),
                    logging.FileHandler(config.LOG_DIR / f"{name}.log", encoding="utf-8")):
        handler.setFormatter(fmt)
        log.addHandler(handler)
    return log
