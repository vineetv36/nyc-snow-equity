"""Download MTA bus ridership data and produce to Kafka.

Fetches bus ridership from data.ny.gov, aggregates by route,
and streams records to Kafka.

Uses server-side SoQL aggregation to avoid downloading millions of
hourly rows.  Falls back to CSV bulk download if SoQL fails.
"""

import io
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

SODA_URL = f"https://{SOCRATA_DOMAIN}/resource/{MTA_RIDERSHIP_DATASET_ID}.csv"
CSV_DOWNLOAD_URL = (
    f"https://{SOCRATA_DOMAIN}/api/views/{MTA_RIDERSHIP_DATASET_ID}"
    "/rows.csv?accessType=DOWNLOAD"
)


def fetch_aggregated() -> pd.DataFrame:
    """Fetch ridership pre-aggregated by route using SoQL (fast)."""
    params: dict = {
        "$select": "bus_route, sum(ridership) as total_ridership, sum(transfers) as total_transfers",
        "$group": "bus_route",
        "$limit": 50000,
    }
    if SOCRATA_APP_TOKEN:
        params["$$app_token"] = SOCRATA_APP_TOKEN

    logger.info("Fetching aggregated ridership via SoQL ...")
    resp = requests.get(SODA_URL, params=params, timeout=300)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    if df.empty:
        raise ValueError("SoQL returned empty result")
    logger.info("Got %d routes via SoQL aggregation", len(df))
    return df


def fetch_csv_bulk() -> pd.DataFrame:
    """Stream CSV download in chunks and aggregate locally (fallback)."""
    logger.info("Falling back to CSV bulk download (streaming) ...")

    agg = {}
    row_count = 0
    chunk_iter = pd.read_csv(
        CSV_DOWNLOAD_URL,
        usecols=["bus_route", "ridership", "transfers"],
        dtype={"bus_route": str, "ridership": float, "transfers": float},
        chunksize=100_000,
    )
    for chunk in chunk_iter:
        row_count += len(chunk)
        if row_count % 500_000 == 0 or row_count == len(chunk):
            logger.info("  streamed %d rows so far ...", row_count)
        partial = chunk.groupby("bus_route", as_index=False).agg(
            total_ridership=("ridership", "sum"),
            total_transfers=("transfers", "sum"),
        )
        for _, r in partial.iterrows():
            route = r["bus_route"]
            if route in agg:
                agg[route][0] += r["total_ridership"]
                agg[route][1] += r["total_transfers"]
            else:
                agg[route] = [r["total_ridership"], r["total_transfers"]]

    logger.info("Streamed %d total rows, aggregated to %d routes", row_count, len(agg))
    df = pd.DataFrame(
        [(k, v[0], v[1]) for k, v in agg.items()],
        columns=["bus_route", "total_ridership", "total_transfers"],
    )
    return df


def fetch_all_records() -> pd.DataFrame:
    """Fetch MTA ridership, preferring server-side aggregation."""
    try:
        return fetch_aggregated()
    except Exception as exc:
        logger.warning("SoQL aggregation failed: %s: %s", type(exc).__name__, exc)
        logger.info("Using CSV streaming fallback ...")
        return fetch_csv_bulk()


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize columns and compute average daily riders."""
    df.columns = [c.lower().strip() for c in df.columns]

    # Rename route column
    for candidate in ["bus_route", "route", "route_id"]:
        if candidate in df.columns:
            df = df.rename(columns={candidate: "route_id"})
            break

    # Compute avg daily riders from total (dataset spans ~4 years ≈ 1461 days)
    if "total_ridership" in df.columns:
        df["avg_daily_riders"] = pd.to_numeric(
            df["total_ridership"], errors="coerce"
        ).fillna(0.0) / 1461.0
    elif "ridership" in df.columns:
        df["avg_daily_riders"] = pd.to_numeric(
            df["ridership"], errors="coerce"
        ).fillna(0.0)
    else:
        logger.warning("No ridership column found. Columns: %s", list(df.columns))
        df["avg_daily_riders"] = 0.0

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
        "Done. %d routes, avg daily riders range: %.0f - %.0f",
        len(df), df["avg_daily_riders"].min(), df["avg_daily_riders"].max(),
    )


if __name__ == "__main__":
    main()
