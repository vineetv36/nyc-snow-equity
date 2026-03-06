"""Download MTA bus ridership data and produce to Kafka.

Fetches annual bus ridership aggregated by stop from data.ny.gov
and streams records to Kafka.
"""

import logging
import os

import pandas as pd
import requests
from kafka import KafkaProducer

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MTA_RIDERSHIP_DATASET_ID = os.environ.get("MTA_RIDERSHIP_DATASET_ID", "kv7t-n8in")
SOCRATA_DOMAIN = "data.ny.gov"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{MTA_RIDERSHIP_DATASET_ID}.json"
PAGE_SIZE = 50_000


def fetch_all_records() -> pd.DataFrame:
    """Paginate through the MTA ridership dataset."""
    all_records: list[dict] = []
    offset = 0

    while True:
        logger.info("Fetching offset %d ...", offset)
        params: dict = {"$limit": PAGE_SIZE, "$offset": offset}
        if SOCRATA_APP_TOKEN:
            params["$$app_token"] = SOCRATA_APP_TOKEN

        resp = requests.get(BASE_URL, params=params, timeout=120)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break
        all_records.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    logger.info("Total ridership records fetched: %d", len(all_records))
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns and compute average daily riders."""
    df.columns = [c.lower().strip() for c in df.columns]

    stop_id_col = None
    for candidate in ["stop_id", "stopid", "stop_code", "gtfs_stop_id"]:
        if candidate in df.columns:
            stop_id_col = candidate
            break
    if stop_id_col and stop_id_col != "stop_id":
        df = df.rename(columns={stop_id_col: "stop_id"})

    rider_col = None
    for candidate in [
        "average_weekday_ridership", "avg_weekday_ridership",
        "average_ridership", "ridership", "avg_daily_riders", "total_ridership",
    ]:
        if candidate in df.columns:
            rider_col = candidate
            break

    if rider_col:
        df["avg_daily_riders"] = pd.to_numeric(df[rider_col], errors="coerce")
    else:
        logger.warning("No ridership column found. Columns: %s", list(df.columns))
        df["avg_daily_riders"] = 0.0

    if rider_col and "annual" in rider_col.lower():
        df["avg_daily_riders"] = df["avg_daily_riders"] / 365.0

    year_col = None
    for candidate in ["year", "fiscal_year", "report_year"]:
        if candidate in df.columns:
            year_col = candidate
            break
    if year_col:
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
        df = df.sort_values(year_col, ascending=False).drop_duplicates(
            subset="stop_id", keep="first"
        )

    return df


def produce(df: pd.DataFrame) -> None:
    """Send each row to Kafka."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    topic = TOPICS["mta_ridership"]

    for _, row in df.iterrows():
        producer.send(topic, value=serialize(row.to_dict()))

    producer.send(topic, value=END_OF_STREAM.encode("utf-8"))
    producer.flush()
    producer.close()
    logger.info("Produced %d records to %s", len(df), topic)


def main() -> None:
    logger.info("Starting MTA bus ridership download")
    df = fetch_all_records()
    if df.empty:
        logger.warning("No records returned")
        return

    df = normalize(df)
    produce(df)

    logger.info(
        "Done. %d stops, avg daily riders range: %.0f - %.0f",
        len(df), df["avg_daily_riders"].min(), df["avg_daily_riders"].max(),
    )


if __name__ == "__main__":
    main()
