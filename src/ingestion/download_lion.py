"""Download NYC Street Centerline (CSCL) data and produce to Kafka.

Fetches street segments from the NYC Open Data Socrata SODA API (dataset
exjm-f27b), transforms to match the street_segments schema, and streams
records to Kafka for PostGIS loading. No local files are written.
"""

import logging
import os

import geopandas as gpd
import pandas as pd
import requests
from kafka import KafkaProducer
from shapely.geometry import LineString, shape

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# NYC Street Centerline (CSCL) on NYC Open Data — Socrata SODA API
CSCL_DATASET_ID = os.environ.get("CSCL_DATASET_ID", "exjm-f27b")
SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")

BASE_URL = f"https://{SOCRATA_DOMAIN}/resource/{CSCL_DATASET_ID}.json"
PAGE_SIZE = 50_000

BOROUGH_MAP = {
    "1": "Manhattan", "2": "Bronx", "3": "Brooklyn",
    "4": "Queens", "5": "Staten Island",
    "MN": "Manhattan", "BX": "Bronx", "BK": "Brooklyn",
    "QN": "Queens", "SI": "Staten Island",
}

ROAD_CLASS_MAP = {
    "1": "highway", "2": "arterial", "3": "collector",
    "4": "local", "5": "private", "6": "alley", "9": "other",
}


def fetch_all_records() -> pd.DataFrame:
    """Paginate through the CSCL dataset via SODA API."""
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

        resp = requests.get(BASE_URL, params=params, timeout=300)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break
        all_records.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    logger.info("Total CSCL records fetched: %d", len(all_records))
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def to_geodataframe(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Convert Socrata JSON rows (with nested geometry) to GeoDataFrame."""
    df.columns = [c.lower().strip() for c in df.columns]

    # Socrata returns geometry in a column like 'the_geom' as a dict
    geom_col = None
    for candidate in ["the_geom", "geometry", "geom", "shape"]:
        if candidate in df.columns:
            geom_col = candidate
            break

    if geom_col is None:
        raise ValueError(f"No geometry column found. Columns: {list(df.columns)}")

    geometries = []
    valid_mask = []
    for g in df[geom_col]:
        try:
            if isinstance(g, dict):
                geometries.append(shape(g))
                valid_mask.append(True)
            else:
                geometries.append(None)
                valid_mask.append(False)
        except Exception:
            geometries.append(None)
            valid_mask.append(False)

    df = df.drop(columns=[geom_col])
    gdf = gpd.GeoDataFrame(df, geometry=geometries, crs="EPSG:4326")
    gdf = gdf[valid_mask].copy()

    logger.info("Converted %d records to GeoDataFrame", len(gdf))
    return gdf


def transform(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform CSCL data to match the street_segments schema."""
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    col_candidates = {
        "segment_id": ["physicalid", "segmentid", "segment_id", "segid", "lion_id"],
        "street_name": ["full_stree", "st_label", "street", "stname", "street_name"],
        "from_street": ["l_from_st", "from_st", "frm_st", "from_street"],
        "to_street": ["l_to_st", "to_st", "to_street"],
        "borough": ["borocode", "boro", "borough", "rw_type"],
        "road_class": ["rw_type", "roadway_type", "feature_ty", "featuretyp", "road_class"],
    }

    out = gpd.GeoDataFrame()
    out["geometry"] = gdf.geometry

    for target, candidates in col_candidates.items():
        matched = False
        for c in candidates:
            if c in gdf.columns:
                out[target] = gdf[c].astype(str)
                matched = True
                break
        if not matched:
            out[target] = ""
            logger.warning("No column found for %s (tried %s)", target, candidates)

    out["segment_id"] = out["segment_id"].str.strip()
    out["borough"] = out["borough"].map(BOROUGH_MAP).fillna(out["borough"])

    if out["road_class"].str.match(r"^\d$").any():
        out["road_class"] = out["road_class"].map(ROAD_CLASS_MAP).fillna("other")

    snow_cols = [c for c in gdf.columns if "snow" in c.lower()]
    if snow_cols:
        out["snow_emergency"] = gdf[snow_cols[0]].astype(bool)
    else:
        out["snow_emergency"] = False

    # Compute segment length in feet using a projected CRS (EPSG:2263 = NY State Plane)
    projected = out.to_crs(epsg=2263)
    out["length_ft"] = projected.geometry.length

    # Keep only LineString geometries
    out = out[out.geometry.geom_type == "LineString"].copy()
    out = out.drop_duplicates(subset="segment_id", keep="first")
    out = out[out["segment_id"].str.len() > 0]
    out = out.rename(columns={"geometry": "geom"}).set_geometry("geom")

    logger.info("Transformed to %d street segments", len(out))
    return out


def produce(gdf: gpd.GeoDataFrame) -> None:
    """Send each row to Kafka as JSON with WKT geometry."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        max_request_size=10_485_760,
    )
    topic = TOPICS["lion"]

    for _, row in gdf.iterrows():
        record = row.drop("geom").to_dict()
        record["geom"] = row.geom.wkt
        producer.send(topic, value=serialize(record))

    producer.send(topic, value=END_OF_STREAM.encode("utf-8"))
    producer.flush()
    producer.close()
    logger.info("Produced %d records to %s", len(gdf), topic)


def main() -> None:
    logger.info("Starting LION street centerline download")
    df = fetch_all_records()
    if df.empty:
        logger.warning("No records returned from CSCL API")
        return
    gdf = to_geodataframe(df)
    gdf = transform(gdf)
    produce(gdf)
    logger.info("Done. %d street segments produced.", len(gdf))


if __name__ == "__main__":
    main()
