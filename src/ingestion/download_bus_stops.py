"""Download MTA GTFS bus stop locations and produce to Kafka.

Fetches the MTA GTFS feed, extracts stops.txt for bus stop coordinates,
joins with ridership data from the ridership Kafka topic, and streams
to Kafka for PostGIS loading.
"""

import csv
import io
import logging
import os
import zipfile

import geopandas as gpd
import pandas as pd
import requests
from kafka import KafkaConsumer, KafkaProducer
from shapely.geometry import Point

from src.ingestion.kafka_config import (
    END_OF_STREAM,
    KAFKA_BOOTSTRAP,
    TOPICS,
    deserialize,
    serialize,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MTA_GTFS_BUS_URL = os.environ.get(
    "MTA_GTFS_BUS_URL",
    "http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip",
)

GTFS_FEEDS = {
    "bronx": "http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip",
    "brooklyn": "http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip",
    "manhattan": "http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip",
    "queens": "http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip",
    "staten_island": "http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip",
}


def download_gtfs_stops(url: str) -> pd.DataFrame:
    """Download a GTFS ZIP in memory and extract stops.txt."""
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
    combined = combined.drop_duplicates(subset="stop_id", keep="first")
    logger.info("Total unique stops across all boroughs: %d", len(combined))
    return combined


def read_ridership_from_kafka() -> dict[str, float]:
    """Read ridership records from Kafka topic to build stop_id → avg_daily_riders map."""
    rider_map: dict[str, float] = {}
    try:
        consumer = KafkaConsumer(
            TOPICS["mta_ridership"],
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            group_id="bus-stops-ridership-reader",
            consumer_timeout_ms=15_000,
        )
        for msg in consumer:
            raw = msg.value.decode("utf-8")
            if raw == END_OF_STREAM:
                break
            record = deserialize(msg.value)
            stop_id = str(record.get("stop_id", "")).strip()
            riders = float(record.get("avg_daily_riders", 0))
            if stop_id:
                rider_map[stop_id] = riders
        consumer.close()
        logger.info("Read %d ridership entries from Kafka", len(rider_map))
    except Exception as e:
        logger.warning("Could not read ridership from Kafka: %s", e)
    return rider_map


def transform(df: pd.DataFrame, rider_map: dict[str, float]) -> gpd.GeoDataFrame:
    """Transform GTFS stops into GeoDataFrame matching bus_stops schema."""
    df["stop_lat"] = pd.to_numeric(df["stop_lat"], errors="coerce")
    df["stop_lon"] = pd.to_numeric(df["stop_lon"], errors="coerce")
    df = df.dropna(subset=["stop_lat", "stop_lon"])

    geometry = [Point(lon, lat) for lon, lat in zip(df["stop_lon"], df["stop_lat"])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    gdf["stop_id"] = gdf["stop_id"].astype(str).str.strip()
    gdf["route_count"] = 1
    gdf["avg_daily_riders"] = gdf["stop_id"].map(rider_map).fillna(0.0)

    matched = (gdf["avg_daily_riders"] > 0).sum()
    logger.info("Matched ridership for %d / %d stops", matched, len(gdf))

    result = gdf[["stop_id", "stop_name", "avg_daily_riders", "route_count"]].copy()
    result = gpd.GeoDataFrame(result, geometry=gdf.geometry.rename("geom"), crs="EPSG:4326")
    return result


def produce(gdf: gpd.GeoDataFrame) -> None:
    """Send each row to Kafka."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    topic = TOPICS["bus_stops"]

    geom_col = gdf.geometry.name
    for _, row in gdf.iterrows():
        record = row.drop(geom_col).to_dict()
        record["geom"] = row.geometry.wkt
        producer.send(topic, value=serialize(record))

    producer.send(topic, value=END_OF_STREAM.encode("utf-8"))
    producer.flush()
    producer.close()
    logger.info("Produced %d records to %s", len(gdf), topic)


def main() -> None:
    logger.info("Starting MTA GTFS bus stops download")
    df = fetch_all_stops()
    rider_map = read_ridership_from_kafka()
    gdf = transform(df, rider_map)
    produce(gdf)
    logger.info("Done. %d bus stops produced.", len(gdf))


if __name__ == "__main__":
    main()
