"""Download NYC DOT Bi-Annual Pedestrian Count data and produce to Kafka.

Fetches pedestrian counts from NYC Open Data (dataset 2de2-6x2h),
averages across available count periods per location, and streams to Kafka.
"""

import logging
import os

import geopandas as gpd
import pandas as pd
import requests
from kafka import KafkaProducer
from shapely.geometry import Point, shape

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PED_DATASET_ID = os.environ.get("PED_DATASET_ID", "2de2-6x2h")
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{PED_DATASET_ID}.json"
PAGE_SIZE = 50_000


def fetch_all_records() -> pd.DataFrame:
    """Fetch all pedestrian count locations."""
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
        logger.info("  got %d records (total: %d)", len(page), len(all_records))
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    logger.info("Total pedestrian count records fetched: %d", len(all_records))
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def normalize(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Normalize and build GeoDataFrame with average pedestrian counts."""
    df.columns = [c.lower().strip() for c in df.columns]
    logger.info("Columns found: %s", list(df.columns))

    # Extract geometry from the_geom (GeoJSON) or lat/lon
    geometries = []
    if "the_geom" in df.columns:
        for _, row in df.iterrows():
            geom_val = row["the_geom"]
            if isinstance(geom_val, dict):
                geometries.append(shape(geom_val))
            elif isinstance(geom_val, str):
                from shapely import wkt
                geometries.append(wkt.loads(geom_val))
            else:
                geometries.append(None)
    else:
        lat_col = next((c for c in ["latitude", "lat"] if c in df.columns), None)
        lon_col = next((c for c in ["longitude", "lon", "long"] if c in df.columns), None)
        if lat_col and lon_col:
            for _, row in df.iterrows():
                try:
                    geometries.append(
                        Point(float(row[lon_col]), float(row[lat_col]))
                    )
                except (ValueError, TypeError):
                    geometries.append(None)
        else:
            logger.error("No geometry columns found")
            return gpd.GeoDataFrame()

    # Build counter_id
    counter_id_col = next(
        (c for c in ["counter_id", "loc_id", "location_id", "objectid"] if c in df.columns),
        None,
    )
    if counter_id_col:
        df["counter_id"] = df[counter_id_col].astype(str)
    else:
        df["counter_id"] = [str(i) for i in range(len(df))]

    # Build location_name
    name_col = next(
        (c for c in ["location_name", "location", "street_name", "street"] if c in df.columns),
        None,
    )
    if name_col:
        df["location_name"] = df[name_col].astype(str)
    else:
        df["location_name"] = "Unknown"

    # Average all numeric count-like columns (e.g., may_2019, sept_2020, ...)
    # Identify columns that look like counts (numeric, not IDs/geo)
    skip_cols = {
        "counter_id", "location_name", "the_geom", "latitude", "longitude",
        "lat", "lon", "long", "borough", "street_name", "street",
        "from_", "to_", "loc_id", "location_id", "objectid", "location",
    }
    count_cols = []
    for col in df.columns:
        if col in skip_cols:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().sum() > 0 and numeric.mean() > 0:
            count_cols.append(col)
            df[col] = numeric

    if count_cols:
        logger.info("Count columns identified: %s", count_cols)
        df["avg_daily_count"] = df[count_cols].mean(axis=1).fillna(0.0)
    else:
        logger.warning("No count columns found, defaulting to 0")
        df["avg_daily_count"] = 0.0

    gdf = gpd.GeoDataFrame(
        df[["counter_id", "location_name", "avg_daily_count"]],
        geometry=geometries,
        crs="EPSG:4326",
    )
    gdf = gdf.dropna(subset=["geometry"])
    gdf = gdf.rename_geometry("geom")

    logger.info("Processed %d counter locations", len(gdf))
    return gdf


def produce(gdf: gpd.GeoDataFrame) -> None:
    """Send each row to Kafka."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    topic = TOPICS["pedestrian"]

    for _, row in gdf.iterrows():
        record = {
            "counter_id": row["counter_id"],
            "location_name": row["location_name"],
            "avg_daily_count": row["avg_daily_count"],
            "geom": row.geom.wkt,
        }
        producer.send(topic, value=serialize(record))

    producer.send(topic, value=END_OF_STREAM.encode("utf-8"))
    producer.flush()
    producer.close()
    logger.info("Produced %d records to %s", len(gdf), topic)


def main() -> None:
    logger.info("Starting NYC pedestrian count download")
    df = fetch_all_records()
    if df.empty:
        logger.warning("No records returned")
        return

    gdf = normalize(df)
    if gdf.empty:
        logger.warning("No counter locations could be processed")
        return

    produce(gdf)
    logger.info("Done. %d counter locations produced.", len(gdf))


if __name__ == "__main__":
    main()
