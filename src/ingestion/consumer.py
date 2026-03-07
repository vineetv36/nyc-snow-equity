"""Kafka consumer that reads ingestion topics and loads data into PostGIS.

Usage: python -m src.ingestion.consumer [--topics lion,plow,...]
"""

import argparse
import logging
import os
from collections import defaultdict

import geopandas as gpd
import pandas as pd
from kafka import KafkaConsumer
from shapely import wkt
from shapely.geometry import shape
from sqlalchemy import create_engine, text

from src.ingestion.kafka_config import (
    END_OF_STREAM,
    KAFKA_BOOTSTRAP,
    TOPICS,
    deserialize,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://snow_user:snow_pass@localhost:5432/nyc_snow_equity",
)


# ---------------------------------------------------------------------------
# Per-topic loaders
# ---------------------------------------------------------------------------

def _load_lion(records: list[dict]) -> None:
    """Load street segments into PostGIS."""
    engine = create_engine(DB_URL)
    df = pd.DataFrame(records)
    geom = gpd.GeoSeries.from_wkt(df.pop("geom"), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE street_segments CASCADE"))
    gdf.to_postgis(
        "street_segments", engine, if_exists="append", index=False,
        dtype={"geom": "Geometry(LineString, 4326)"},
    )
    logger.info("Loaded %d street segments into PostGIS", len(gdf))


def _load_mta_ridership(records: list[dict]) -> None:
    """MTA ridership is consumed by bus_stops producer; just log receipt."""
    logger.info("Received %d MTA ridership records (used by bus_stops join)", len(records))


def _load_bus_stops(records: list[dict]) -> None:
    engine = create_engine(DB_URL)
    df = pd.DataFrame(records)
    geom = gpd.GeoSeries.from_wkt(df.pop("geom"), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE bus_stops CASCADE"))
    gdf["segment_id"] = None
    gdf.to_postgis(
        "bus_stops", engine, if_exists="append", index=False,
        dtype={"geom": "Geometry(Point, 4326)"},
    )
    logger.info("Loaded %d bus stops into PostGIS", len(gdf))


def _load_plow(records: list[dict]) -> None:
    """Plow GPS data — no PostGIS table in schema, just confirm receipt."""
    logger.info("Received %d plow GPS records (ready for map matching)", len(records))


def _load_pedestrian(records: list[dict]) -> None:
    engine = create_engine(DB_URL)
    df = pd.DataFrame(records)
    geom = gpd.GeoSeries.from_wkt(df.pop("geom"), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE pedestrian_counters CASCADE"))
    gdf["segment_id"] = None
    gdf.to_postgis(
        "pedestrian_counters", engine, if_exists="append", index=False,
        dtype={"geom": "Geometry(Point, 4326)"},
    )
    logger.info("Loaded %d pedestrian counters into PostGIS", len(gdf))


def _load_census(records: list[dict]) -> None:
    engine = create_engine(DB_URL)
    df = pd.DataFrame(records)
    geom = gpd.GeoSeries.from_wkt(df.pop("geom"), crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geom, crs="EPSG:4326")
    gdf = gdf.rename_geometry("geom")

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE census_tracts CASCADE"))
    gdf.to_postgis(
        "census_tracts", engine, if_exists="append", index=False,
        dtype={"geom": "Geometry(MultiPolygon, 4326)"},
    )
    logger.info("Loaded %d census tracts into PostGIS", len(gdf))


LOADERS = {
    TOPICS["lion"]: _load_lion,
    TOPICS["mta_ridership"]: _load_mta_ridership,
    TOPICS["bus_stops"]: _load_bus_stops,
    TOPICS["plow"]: _load_plow,
    TOPICS["pedestrian"]: _load_pedestrian,
    TOPICS["census"]: _load_census,
}


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run_consumer(topic_names: list[str], timeout_ms: int = 30_000) -> None:
    """Consume from the given topics until all send END_OF_STREAM."""
    consumer = KafkaConsumer(
        *topic_names,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="ingestion-loader",
        value_deserializer=lambda v: v,
        consumer_timeout_ms=timeout_ms,
    )

    buffers: dict[str, list[dict]] = defaultdict(list)
    finished: set[str] = set()

    logger.info("Consumer listening on topics: %s", topic_names)

    for msg in consumer:
        raw = msg.value.decode("utf-8")
        if raw == END_OF_STREAM:
            logger.info("End-of-stream on %s (%d records buffered)", msg.topic, len(buffers[msg.topic]))
            finished.add(msg.topic)
            if finished >= set(topic_names):
                break
            continue
        buffers[msg.topic].append(deserialize(msg.value))

    consumer.close()

    # Flush all buffers through their loaders
    for topic, records in buffers.items():
        if not records:
            continue
        loader = LOADERS.get(topic)
        if loader:
            logger.info("Loading %d records from %s", len(records), topic)
            loader(records)
        else:
            logger.warning("No loader for topic %s", topic)


def main():
    parser = argparse.ArgumentParser(description="Kafka consumer → PostGIS loader")
    parser.add_argument(
        "--topics", default=",".join(TOPICS.values()),
        help="Comma-separated topic names to consume",
    )
    parser.add_argument(
        "--timeout", type=int, default=60_000,
        help="Consumer timeout in ms after last message (default 60s)",
    )
    args = parser.parse_args()

    topic_list = [t.strip() for t in args.topics.split(",")]
    run_consumer(topic_list, timeout_ms=args.timeout)


if __name__ == "__main__":
    main()
