"""Download NYC DOT Automated Pedestrian Count data and produce to Kafka.

Fetches hourly pedestrian counts from NYC Open Data, aggregates to daily
averages per counter location, and streams to Kafka for PostGIS loading.
"""

import logging
import os

import geopandas as gpd
import pandas as pd
import requests
from kafka import KafkaProducer
from shapely.geometry import Point

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PED_DATASET_ID = os.environ.get("PED_DATASET_ID", "m9t2-jxst")
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{PED_DATASET_ID}.json"
PAGE_SIZE = 50_000


def fetch_all_records() -> pd.DataFrame:
    """Paginate through the pedestrian counts dataset."""
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

    logger.info("Total pedestrian count records fetched: %d", len(all_records))
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and types."""
    df.columns = [c.lower().strip() for c in df.columns]

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

    for col in ("latitude", "lat"):
        if col in df.columns:
            df["latitude"] = pd.to_numeric(df[col], errors="coerce")
            break
    for col in ("longitude", "lon", "long"):
        if col in df.columns:
            df["longitude"] = pd.to_numeric(df[col], errors="coerce")
            break

    for candidate in ["counter_id", "detector_id", "id", "location_id"]:
        if candidate in df.columns:
            df["counter_id"] = df[candidate].astype(str)
            break

    for candidate in ["location_name", "location", "street_name", "cross_streets"]:
        if candidate in df.columns:
            df["location_name"] = df[candidate].astype(str)
            break

    return df


def aggregate_daily(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Aggregate hourly counts to average daily count per counter location."""
    required = ["counter_id", "location_name", "latitude", "longitude"]
    for col in required:
        if col not in df.columns:
            logger.error("Missing required column: %s", col)
            return gpd.GeoDataFrame()

    agg = (
        df.groupby(["counter_id", "location_name", "latitude", "longitude"])
        .agg(total_count=("count", "sum"), num_records=("count", "count"))
        .reset_index()
    )

    agg["days_of_data"] = (agg["num_records"] / 24).clip(lower=1)
    agg["avg_daily_count"] = agg["total_count"] / agg["days_of_data"]

    agg = agg.dropna(subset=["latitude", "longitude"])
    geometry = [Point(lon, lat) for lon, lat in zip(agg["longitude"], agg["latitude"])]
    gdf = gpd.GeoDataFrame(agg, geometry=geometry, crs="EPSG:4326")
    gdf = gdf.rename(columns={"geometry": "geom"}).set_geometry("geom")

    logger.info("Aggregated to %d counter locations", len(gdf))
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

    df = normalize(df)
    gdf = aggregate_daily(df)
    if gdf.empty:
        logger.warning("No counter locations could be aggregated")
        return

    produce(gdf)
    logger.info("Done. %d counter locations produced.", len(gdf))


if __name__ == "__main__":
    main()
