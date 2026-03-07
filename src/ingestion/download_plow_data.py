"""Download DSNY PlowNYC street-segment plowing data and produce to Kafka.

Fetches plowing timestamps per street segment (PHYSICAL_ID) from NYC Open Data.
Each record represents when a street segment was last plowed by a DSNY vehicle.
"""

import logging
import os

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


def fetch_all_records() -> pd.DataFrame:
    """Fetch PlowNYC records via SoQL (latest plowing per segment)."""
    # The dataset can be huge — use server-side aggregation to get
    # the most recent plow timestamp per physical_id.
    params: dict = {
        "$select": "physical_id, max(timestamp) as last_plowed",
        "$group": "physical_id",
        "$limit": PAGE_SIZE,
    }
    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN

    logger.info("Fetching PlowNYC aggregated data ...")
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

    if "physical_id" in df.columns:
        df["physical_id"] = df["physical_id"].astype(str)

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
