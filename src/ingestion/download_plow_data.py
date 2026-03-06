"""Download PlowNYC historical GPS breadcrumb data from NYC Open Data Socrata API.

Fetches all available PlowNYC GPS data, identifies storm events by clustering
activity with >24h gaps, and saves to data/raw/plow/ as Parquet partitioned
by storm date.
"""

import logging
import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# NYC Open Data Socrata dataset identifier for PlowNYC
# The actual dataset ID may vary; this is the known endpoint.
PLOWNYC_DATASET_ID = os.environ.get("PLOWNYC_DATASET_ID", "e75s-5bpq")
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{PLOWNYC_DATASET_ID}.json"
RAW_DIR = Path("data/raw/plow")
PAGE_SIZE = 50_000
STORM_GAP_HOURS = 24


def fetch_page(offset: int, limit: int = PAGE_SIZE) -> list[dict]:
    """Fetch a single page of plow GPS records from Socrata SODA API."""
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "timestamp ASC",
    }
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

    df = pd.DataFrame(all_records)
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and types."""
    # Socrata field names may vary; map known variations
    col_map = {}
    for col in df.columns:
        lower = col.lower().strip()
        col_map[col] = lower
    df = df.rename(columns=col_map)

    if "timestamp" not in df.columns and "datetime" in df.columns:
        df = df.rename(columns={"datetime": "timestamp"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    for col in ("latitude", "longitude", "speed"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def identify_storms(df: pd.DataFrame) -> pd.DataFrame:
    """Assign each record to a storm event.

    A storm event is a continuous cluster of plow activity where no gap
    exceeds STORM_GAP_HOURS hours. The storm_date is the first date of
    each cluster.
    """
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


def save_partitioned(df: pd.DataFrame, output_dir: Path) -> None:
    """Save as Parquet files partitioned by storm_date."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for storm_date, group in df.groupby("storm_date"):
        date_str = pd.Timestamp(storm_date).strftime("%Y-%m-%d")
        out_path = output_dir / f"plow_gps_{date_str}.parquet"
        group.drop(columns=["storm_id"], errors="ignore").to_parquet(
            out_path, index=False, engine="pyarrow"
        )
        logger.info("Wrote %s (%d rows)", out_path, len(group))


def main() -> None:
    logger.info("Starting PlowNYC data download")
    df = fetch_all_records()
    if df.empty:
        logger.warning("No records returned from API")
        return

    df = normalize(df)
    df = identify_storms(df)

    save_partitioned(df, RAW_DIR)

    logger.info(
        "Done. Date range: %s to %s, Total records: %d, Storms: %d",
        df["timestamp"].min(),
        df["timestamp"].max(),
        len(df),
        df["storm_date"].nunique(),
    )


if __name__ == "__main__":
    main()
