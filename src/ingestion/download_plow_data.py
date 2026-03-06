"""Download PlowNYC historical GPS breadcrumb data and produce to Kafka.

Fetches all available PlowNYC GPS data, identifies storm events by clustering
activity with >24h gaps, and streams records to Kafka.
"""

import logging
import os
from datetime import timedelta

import pandas as pd
import requests
from kafka import KafkaProducer

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PLOWNYC_DATASET_ID = os.environ.get("PLOWNYC_DATASET_ID", "e75s-5bpq")
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{PLOWNYC_DATASET_ID}.json"
PAGE_SIZE = 50_000
STORM_GAP_HOURS = 24


def fetch_page(offset: int, limit: int = PAGE_SIZE) -> list[dict]:
    """Fetch a single page of plow GPS records from Socrata SODA API."""
    params = {"$limit": limit, "$offset": offset, "$order": "timestamp ASC"}
    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN

    resp = requests.get(BASE_URL, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def fetch_all_records() -> pd.DataFrame:
    """Paginate through the full PlowNYC dataset."""
    all_records: list[dict] = []
    offset = 0

    while True:
        logger.info("Fetching offset %d ...", offset)
        page = fetch_page(offset)
        if not page:
            break
        all_records.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    logger.info("Total records fetched: %d", len(all_records))
    if not all_records:
        return pd.DataFrame()
    return pd.DataFrame(all_records)


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and types."""
    col_map = {col: col.lower().strip() for col in df.columns}
    df = df.rename(columns=col_map)

    if "timestamp" not in df.columns and "datetime" in df.columns:
        df = df.rename(columns={"datetime": "timestamp"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    for col in ("latitude", "longitude", "speed"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def identify_storms(df: pd.DataFrame) -> pd.DataFrame:
    """Assign each record to a storm event."""
    df = df.sort_values("timestamp").reset_index(drop=True)
    gap = df["timestamp"].diff() > timedelta(hours=STORM_GAP_HOURS)
    df["storm_id"] = gap.cumsum()

    storm_starts = df.groupby("storm_id")["timestamp"].min().dt.date
    df["storm_date"] = df["storm_id"].map(storm_starts)
    df["storm_date"] = pd.to_datetime(df["storm_date"])

    storm_summary = df.groupby("storm_date").size()
    logger.info("Storms identified: %d", len(storm_summary))
    for date, count in storm_summary.items():
        logger.info("  %s: %d records", date, count)

    return df


def produce(df: pd.DataFrame) -> None:
    """Send each row to Kafka."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    topic = TOPICS["plow"]

    for _, row in df.iterrows():
        producer.send(topic, value=serialize(row.drop("storm_id", errors="ignore").to_dict()))

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
    df = identify_storms(df)
    produce(df)

    logger.info(
        "Done. Date range: %s to %s, Total records: %d, Storms: %d",
        df["timestamp"].min(), df["timestamp"].max(),
        len(df), df["storm_date"].nunique(),
    )


if __name__ == "__main__":
    main()
