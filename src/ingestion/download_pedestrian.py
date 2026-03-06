"""Download NYC DOT Automated Pedestrian Count data and load into PostGIS.

Fetches hourly pedestrian counts from NYC Open Data, aggregates to daily
averages per counter location, saves as Parquet and GeoJSON, and loads
into the pedestrian_counters PostGIS table.
"""

import logging
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# NYC Open Data dataset ID for Automated Pedestrian Counts
PED_DATASET_ID = os.environ.get("PED_DATASET_ID", "m9t2-jxst")
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{PED_DATASET_ID}.json"
RAW_DIR = Path("data/raw/pedestrian")
PAGE_SIZE = 50_000

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://snow_user:snow_pass@localhost:5432/nyc_snow_equity",
)


def fetch_all_records() -> pd.DataFrame:
    """Paginate through the pedestrian counts dataset."""
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

    logger.info("Total pedestrian count records fetched: %d", len(all_records))
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and types."""
    df.columns = [c.lower().strip() for c in df.columns]

    # Parse counts
    count_col = None
    for candidate in ["counts", "pedestrian_count", "vol", "volume", "count"]:
        if candidate in df.columns:
            count_col = candidate
            break
    if count_col:
        df["count"] = pd.to_numeric(df[count_col], errors="coerce")
    else:
        logger.warning("No count column found. Columns: %s", list(df.columns))
        df["count"] = 0

    # Parse location coordinates
    for col in ("latitude", "lat"):
        if col in df.columns:
            df["latitude"] = pd.to_numeric(df[col], errors="coerce")
            break
    for col in ("longitude", "lon", "long"):
        if col in df.columns:
            df["longitude"] = pd.to_numeric(df[col], errors="coerce")
            break

    # Counter/location ID
    for candidate in ["counter_id", "detector_id", "id", "location_id"]:
        if candidate in df.columns:
            df["counter_id"] = df[candidate].astype(str)
            break

    # Location name
    for candidate in ["location_name", "location", "street_name", "cross_streets"]:
        if candidate in df.columns:
            df["location_name"] = df[candidate].astype(str)
            break

    return df


def aggregate_daily(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Aggregate hourly counts to average daily count per counter location."""
    # Group by counter location
    required = ["counter_id", "location_name", "latitude", "longitude"]
    for col in required:
        if col not in df.columns:
            logger.error("Missing required column: %s", col)
            return gpd.GeoDataFrame()

    agg = (
        df.groupby(["counter_id", "location_name", "latitude", "longitude"])
        .agg(
            total_count=("count", "sum"),
            num_records=("count", "count"),
        )
        .reset_index()
    )

    # Estimate avg daily count: total / (num_records / 24 hours per day)
    # If data is hourly, num_records ≈ hours of data
    agg["days_of_data"] = (agg["num_records"] / 24).clip(lower=1)
    agg["avg_daily_count"] = agg["total_count"] / agg["days_of_data"]

    agg = agg.dropna(subset=["latitude", "longitude"])
    geometry = [Point(lon, lat) for lon, lat in zip(agg["longitude"], agg["latitude"])]
    gdf = gpd.GeoDataFrame(agg, geometry=geometry, crs="EPSG:4326")

    gdf = gdf.rename(columns={"geometry": "geom"}).set_geometry("geom")

    logger.info("Aggregated to %d counter locations", len(gdf))
    return gdf


def load_to_postgis(gdf: gpd.GeoDataFrame) -> None:
    """Load pedestrian counters into PostGIS."""
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE pedestrian_counters CASCADE"))

    out = gdf[["counter_id", "location_name", "avg_daily_count", "geom"]].copy()
    out["segment_id"] = None  # Populated by pedestrian_joiner later

    out.to_postgis(
        "pedestrian_counters",
        engine,
        if_exists="append",
        index=False,
        dtype={"geom": "Geometry(Point, 4326)"},
    )
    logger.info("Loaded %d pedestrian counters into PostGIS", len(out))


def save_raw(df: pd.DataFrame, gdf: gpd.GeoDataFrame, output_dir: Path) -> None:
    """Save raw counts as Parquet and aggregated locations as GeoJSON."""
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / "pedestrian_counts.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info("Saved raw counts: %s (%d rows)", parquet_path, len(df))

    if not gdf.empty:
        geojson_path = output_dir / "pedestrian_counters.geojson"
        gdf.to_file(geojson_path, driver="GeoJSON")
        logger.info("Saved counter locations: %s", geojson_path)


def main() -> None:
    logger.info("Starting NYC pedestrian count download")
    df = fetch_all_records()
    if df.empty:
        logger.warning("No records returned")
        return

    df = normalize(df)
    save_raw(df, gpd.GeoDataFrame(), RAW_DIR)

    gdf = aggregate_daily(df)
    if gdf.empty:
        logger.warning("No counter locations could be aggregated")
        return

    save_raw(df, gdf, RAW_DIR)
    load_to_postgis(gdf)
    logger.info("Done. %d counter locations loaded.", len(gdf))


if __name__ == "__main__":
    main()
