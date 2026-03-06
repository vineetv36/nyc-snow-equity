"""Download MTA GTFS bus stop locations and load into PostGIS.

Fetches the MTA GTFS feed, extracts stops.txt for bus stop coordinates,
joins with ridership data if available, saves as GeoJSON, and loads into
the bus_stops PostGIS table.
"""

import csv
import io
import logging
import os
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# MTA GTFS Bus feed URL
GTFS_URL = os.environ.get(
    "MTA_GTFS_BUS_URL",
    "http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip",
)

# Multiple borough GTFS feeds
GTFS_FEEDS = {
    "bronx": "http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip",
    "brooklyn": "http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip",
    "manhattan": "http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip",
    "queens": "http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip",
    "staten_island": "http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip",
}

RAW_DIR = Path("data/raw/bus_stops")
RIDERSHIP_PATH = Path("data/raw/mta_ridership/mta_bus_ridership.parquet")

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://snow_user:snow_pass@localhost:5432/nyc_snow_equity",
)


def download_gtfs_stops(url: str) -> pd.DataFrame:
    """Download a GTFS ZIP and extract stops.txt."""
    logger.info("Downloading GTFS feed from %s", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("stops.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            stops = list(reader)

    df = pd.DataFrame(stops)
    logger.info("Extracted %d stops", len(df))
    return df


def fetch_all_stops() -> pd.DataFrame:
    """Fetch stops from all borough GTFS feeds."""
    all_stops = []
    for borough, url in GTFS_FEEDS.items():
        try:
            df = download_gtfs_stops(url)
            df["borough"] = borough
            all_stops.append(df)
        except Exception as e:
            logger.warning("Failed to download %s feed: %s", borough, e)

    if not all_stops:
        raise RuntimeError("Could not download any GTFS feeds")

    combined = pd.concat(all_stops, ignore_index=True)
    # Deduplicate stops that appear in multiple feeds
    combined = combined.drop_duplicates(subset="stop_id", keep="first")
    logger.info("Total unique stops across all boroughs: %d", len(combined))
    return combined


def transform(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Transform GTFS stops into GeoDataFrame matching bus_stops schema."""
    df["stop_lat"] = pd.to_numeric(df["stop_lat"], errors="coerce")
    df["stop_lon"] = pd.to_numeric(df["stop_lon"], errors="coerce")
    df = df.dropna(subset=["stop_lat", "stop_lon"])

    geometry = [Point(lon, lat) for lon, lat in zip(df["stop_lon"], df["stop_lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # Rename to match schema
    gdf = gdf.rename(columns={"stop_name": "stop_name"})
    gdf["stop_id"] = gdf["stop_id"].astype(str).str.strip()

    # Count routes per stop if routes.txt / stop_times.txt info is available
    # For now, default to 1; this will be enriched later
    gdf["route_count"] = 1

    # Join ridership data if available
    gdf["avg_daily_riders"] = 0.0
    if RIDERSHIP_PATH.exists():
        logger.info("Joining ridership data from %s", RIDERSHIP_PATH)
        ridership = pd.read_parquet(RIDERSHIP_PATH)
        if "stop_id" in ridership.columns and "avg_daily_riders" in ridership.columns:
            ridership["stop_id"] = ridership["stop_id"].astype(str).str.strip()
            rider_map = ridership.set_index("stop_id")["avg_daily_riders"].to_dict()
            gdf["avg_daily_riders"] = gdf["stop_id"].map(rider_map).fillna(0.0)
            matched = (gdf["avg_daily_riders"] > 0).sum()
            logger.info("Matched ridership for %d / %d stops", matched, len(gdf))

    # segment_id will be populated during the spatial join processing step
    result = gdf[["stop_id", "stop_name", "avg_daily_riders", "route_count"]].copy()
    result = result.set_index(result.index)
    result = gpd.GeoDataFrame(result, geometry=gdf.geometry.rename("geom"), crs="EPSG:4326")

    return result


def load_to_postgis(gdf: gpd.GeoDataFrame) -> None:
    """Load bus stops into PostGIS.

    Note: segment_id FK is NULL initially — it gets populated by the
    ridership_joiner processing step after street segments are loaded.
    """
    engine = create_engine(DB_URL)

    # We can't use the FK constraint on initial load since segment_id is null
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE bus_stops CASCADE"))

    # Add segment_id column as NULL
    gdf["segment_id"] = None

    gdf.to_postgis(
        "bus_stops",
        engine,
        if_exists="append",
        index=False,
        dtype={"geom": "Geometry(Point, 4326)"},
    )
    logger.info("Loaded %d bus stops into PostGIS", len(gdf))


def save_raw(gdf: gpd.GeoDataFrame, output_dir: Path) -> None:
    """Save as GeoJSON for reproducibility."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "bus_stops.geojson"
    gdf.to_file(out_path, driver="GeoJSON")
    logger.info("Saved %s", out_path)


def main() -> None:
    logger.info("Starting MTA GTFS bus stops download")
    df = fetch_all_stops()
    gdf = transform(df)
    save_raw(gdf, RAW_DIR)
    load_to_postgis(gdf)
    logger.info("Done. %d bus stops loaded.", len(gdf))


if __name__ == "__main__":
    main()
