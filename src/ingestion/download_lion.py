"""Download NYC LION street centerline network and produce to Kafka.

Fetches the LION geodatabase from NYC Open Data / DCP, extracts street
segments, computes lengths, and streams records to Kafka for PostGIS loading.
"""

import io
import logging
import os
import zipfile

import geopandas as gpd
import requests
from kafka import KafkaProducer

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LION_URL = os.environ.get(
    "LION_URL",
    "https://data.cityofnewyork.us/api/geospatial/exjm-f27b?method=export&type=GeoJSON",
)

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


def download_lion() -> gpd.GeoDataFrame:
    """Download LION data into memory and return as GeoDataFrame."""
    logger.info("Downloading LION street centerline from %s", LION_URL)
    resp = requests.get(LION_URL, timeout=300, stream=True)
    resp.raise_for_status()

    content = resp.content
    content_type = resp.headers.get("Content-Type", "")

    if "zip" in content_type or LION_URL.endswith(".zip"):
        logger.info("Extracting ZIP archive in memory")
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            shp_names = [n for n in zf.namelist() if n.endswith(".shp")]
            geojson_names = [n for n in zf.namelist() if n.endswith(".geojson")]
            if shp_names:
                gdf = gpd.read_file(io.BytesIO(content), layer=shp_names[0])
            elif geojson_names:
                gdf = gpd.read_file(io.BytesIO(zf.read(geojson_names[0])))
            else:
                raise FileNotFoundError("No shapefile or GeoJSON found in ZIP")
    else:
        gdf = gpd.read_file(io.BytesIO(content))

    logger.info("Loaded %d features", len(gdf))
    return gdf


def transform(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform LION data to match the street_segments schema."""
    gdf.columns = [c.lower() for c in gdf.columns]

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

    projected = out.to_crs(epsg=2263)
    out["length_ft"] = projected.geometry.length

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
    gdf = download_lion()
    gdf = transform(gdf)
    produce(gdf)
    logger.info("Done. %d street segments produced.", len(gdf))


if __name__ == "__main__":
    main()
