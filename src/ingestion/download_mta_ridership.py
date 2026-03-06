"""Download MTA bus ridership data per stop from NY State Open Data.

Fetches annual bus ridership aggregated by stop from data.ny.gov,
saves to data/raw/mta_ridership/ as Parquet, and computes average
daily riders per stop.
"""

import logging
import os
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# MTA Bus Ridership dataset on data.ny.gov (Socrata)
MTA_RIDERSHIP_DATASET_ID = os.environ.get("MTA_RIDERSHIP_DATASET_ID", "kv7t-n8in")
SOCRATA_DOMAIN = "data.ny.gov"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{MTA_RIDERSHIP_DATASET_ID}.json"
RAW_DIR = Path("data/raw/mta_ridership")
PAGE_SIZE = 50_000


def fetch_all_records() -> pd.DataFrame:
    """Paginate through the MTA ridership dataset."""
    all_records: list[dict] = []
    offset = 0

    while True:
        logger.info("Fetching offset %d ...", offset)
        params: dict = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
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

    # Identify the stop ID column
    stop_id_col = None
    for candidate in ["stop_id", "stopid", "stop_code", "gtfs_stop_id"]:
        if candidate in df.columns:
            stop_id_col = candidate
            break
    if stop_id_col and stop_id_col != "stop_id":
        df = df.rename(columns={stop_id_col: "stop_id"})

    # Identify ridership columns
    rider_col = None
    for candidate in [
        "average_weekday_ridership",
        "avg_weekday_ridership",
        "average_ridership",
        "ridership",
        "avg_daily_riders",
        "total_ridership",
    ]:
        if candidate in df.columns:
            rider_col = candidate
            break

    if rider_col:
        df["avg_daily_riders"] = pd.to_numeric(df[rider_col], errors="coerce")
    else:
        logger.warning("No ridership column found. Columns: %s", list(df.columns))
        df["avg_daily_riders"] = 0.0

    # If we have annual totals, convert to daily average
    if rider_col and "annual" in rider_col.lower():
        df["avg_daily_riders"] = df["avg_daily_riders"] / 365.0

    # Keep the most recent year of data per stop
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


def save(df: pd.DataFrame, output_dir: Path) -> None:
    """Save ridership data as Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "mta_bus_ridership.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info("Saved %s (%d rows)", out_path, len(df))


def main() -> None:
    logger.info("Starting MTA bus ridership download")
    df = fetch_all_records()
    if df.empty:
        logger.warning("No records returned")
        return

    df = normalize(df)
    save(df, RAW_DIR)

    logger.info(
        "Done. %d stops, avg daily riders range: %.0f - %.0f",
        len(df),
        df["avg_daily_riders"].min(),
        df["avg_daily_riders"].max(),
    )


if __name__ == "__main__":
    main()
