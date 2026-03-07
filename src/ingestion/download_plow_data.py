"""Download DSNY PlowNYC street-segment plowing data and produce to Kafka.

Fetches plowing timestamps per street segment (PHYSICAL_ID) from NYC Open Data.
Each record represents when a street segment was last plowed by a DSNY vehicle.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

import pandas as pd
import requests
from kafka import KafkaProducer

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PLOWNYC_DATASET_ID = os.environ.get("PLOWNYC_DATASET_ID", "rmhc-afj9")
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{PLOWNYC_DATASET_ID}.json"
PAGE_SIZE = 50_000


def _discover_columns() -> dict:
    """Fetch one record to discover the actual column names."""
    params: dict = {"$limit": 1}
    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        return {}
    return rows[0]


def _find_column(sample: dict, candidates: list[str]) -> str | None:
    """Return the first column name from *candidates* found in *sample*."""
    keys_lower = {k.lower(): k for k in sample}
    for c in candidates:
        if c.lower() in keys_lower:
            return keys_lower[c.lower()]
    return None


def fetch_all_records() -> pd.DataFrame:
    """Fetch PlowNYC records via SoQL (latest plowing per segment)."""
    # Discover schema first so we use the real column names.
    sample = _discover_columns()
    if not sample:
        logger.warning("Could not fetch sample record — dataset may be empty")
        return pd.DataFrame()

    logger.info("Discovered columns: %s", list(sample.keys()))

    id_col = _find_column(sample, ["physical_id", "physicalid", "segment_id"])
    ts_col = _find_column(
        sample, ["timestamp", "last_updated", "last_plowed", "date_time", "datetime"]
    )

    if id_col and ts_col:
        # Server-side aggregation: latest plow timestamp per segment.
        params: dict = {
            "$select": f"{id_col}, max({ts_col}) as last_plowed",
            "$group": id_col,
            "$limit": PAGE_SIZE,
        }
    else:
        # Fallback: just grab raw records.
        logger.warning(
            "Could not identify id/timestamp columns (id=%s, ts=%s). "
            "Falling back to raw fetch.",
            id_col,
            ts_col,
        )
        params = {"$limit": PAGE_SIZE}

    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN

    logger.info("Fetching PlowNYC data (params=%s) ...", params)
    resp = requests.get(BASE_URL, params=params, timeout=300)
    resp.raise_for_status()
    records = resp.json()

    logger.info("Total segment records fetched: %d", len(records))
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and types."""
    df.columns = [c.lower().strip() for c in df.columns]

    # Standardise the segment-id column to "physical_id".
    for alias in ("physicalid", "segment_id", "physical_id"):
        if alias in df.columns:
            df = df.rename(columns={alias: "physical_id"})
            break
    if "physical_id" in df.columns:
        df["physical_id"] = df["physical_id"].astype(str)

    # Standardise any timestamp column to "last_plowed".
    for alias in ("last_plowed", "last_updated", "timestamp", "date_time", "datetime"):
        if alias in df.columns:
            df = df.rename(columns={alias: "last_plowed"})
            break
    if "last_plowed" in df.columns:
        df["last_plowed"] = pd.to_datetime(df["last_plowed"], utc=True, errors="coerce")

    return df


def produce(df: pd.DataFrame) -> None:
    """Send each row to Kafka."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    topic = TOPICS["plow"]

    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if hasattr(val, "isoformat"):
                record[col] = val.isoformat()
            else:
                record[col] = val
        producer.send(topic, value=serialize(record))

    producer.send(topic, value=END_OF_STREAM.encode("utf-8"))
    producer.flush()
    producer.close()
    logger.info("Produced %d records to %s", len(df), topic)


def main() -> None:
    logger.info("Starting PlowNYC data download")
    df = fetch_all_records()
    if df.empty:
        logger.warning("No records returned from API")
        return

    df = normalize(df)
    produce(df)

    logger.info(
        "Done. %d street segments with plow data.",
        len(df),
    )


if __name__ == "__main__":
    main()
