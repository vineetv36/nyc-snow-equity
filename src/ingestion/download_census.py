"""Download Census ACS demographic data for NYC tracts and produce to Kafka.

Fetches ACS 5-Year estimates (income, race, poverty, age) and tract
geometries from the Census Bureau API / TIGER shapefiles, and streams
records to Kafka for PostGIS loading.
"""

import io
import logging
import os

import geopandas as gpd
import pandas as pd
import requests
from kafka import KafkaProducer

from src.ingestion.kafka_config import END_OF_STREAM, KAFKA_BOOTSTRAP, TOPICS, serialize

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY", "")
ACS_YEAR = os.environ.get("ACS_YEAR", "2022")

NYC_COUNTIES = ["005", "047", "061", "081", "085"]

ACS_VARIABLES = [
    "B19013_001E",  # median income
    "B02001_001E",  # total pop
    "B02001_002E",  # white
    "B02001_003E",  # black
    "B03003_003E",  # hispanic
    "B17001_002E",  # poverty
    "B01001_020E", "B01001_021E", "B01001_022E",
    "B01001_023E", "B01001_024E", "B01001_025E",
    "B01001_044E", "B01001_045E", "B01001_046E",
    "B01001_047E", "B01001_048E", "B01001_049E",
    "C18108_001E", "C18108_007E", "C18108_011E",
]


def fetch_acs_data() -> pd.DataFrame:
    """Fetch ACS 5-Year data for all NYC census tracts."""
    variables = ",".join(ACS_VARIABLES)
    all_data = []

    for county in NYC_COUNTIES:
        url = (
            f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
            f"?get=NAME,{variables}"
            f"&for=tract:*"
            f"&in=state:36&in=county:{county}"
        )
        if CENSUS_API_KEY:
            url += f"&key={CENSUS_API_KEY}"

        logger.info("Fetching ACS data for county %s", county)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)
        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)
    logger.info("Fetched ACS data for %d tracts", len(combined))
    return combined


def fetch_tract_geometries() -> gpd.GeoDataFrame:
    """Fetch census tract boundaries from TIGER/Line shapefiles into memory."""
    tiger_url = (
        f"https://www2.census.gov/geo/tiger/TIGER{ACS_YEAR}/TRACT/"
        f"tl_{ACS_YEAR}_36_tract.zip"
    )
    logger.info("Downloading tract geometries from %s", tiger_url)

    try:
        gdf = gpd.read_file(tiger_url)
    except Exception:
        carto_url = (
            f"https://www2.census.gov/geo/tiger/GENZ{ACS_YEAR}/shp/"
            f"cb_{ACS_YEAR}_36_tract_500k.zip"
        )
        logger.info("Falling back to cartographic boundary: %s", carto_url)
        gdf = gpd.read_file(carto_url)

    gdf["COUNTYFP"] = gdf["COUNTYFP"].astype(str).str.zfill(3)
    gdf = gdf[gdf["COUNTYFP"].isin(NYC_COUNTIES)].copy()

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    logger.info("Loaded %d NYC tract geometries", len(gdf))
    return gdf


def transform(acs_df: pd.DataFrame, geo_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Merge ACS demographics with tract geometries."""
    acs_df["tract_id"] = (
        acs_df["state"].str.zfill(2)
        + acs_df["county"].str.zfill(3)
        + acs_df["tract"].str.zfill(6)
    )

    for col in ACS_VARIABLES:
        if col in acs_df.columns:
            acs_df[col] = pd.to_numeric(acs_df[col], errors="coerce")

    total_pop = acs_df["B02001_001E"].clip(lower=1)

    result = pd.DataFrame()
    result["tract_id"] = acs_df["tract_id"]
    result["median_income"] = acs_df["B19013_001E"]
    result["total_population"] = acs_df["B02001_001E"]
    result["pct_white"] = (acs_df["B02001_002E"] / total_pop * 100).round(1)
    result["pct_black"] = (acs_df["B02001_003E"] / total_pop * 100).round(1)
    result["pct_hispanic"] = (acs_df["B03003_003E"] / total_pop * 100).round(1)
    result["pct_minority"] = (100 - result["pct_white"]).clip(lower=0)
    result["pct_poverty"] = (acs_df["B17001_002E"] / total_pop * 100).round(1)

    elderly_cols = [
        "B01001_020E", "B01001_021E", "B01001_022E",
        "B01001_023E", "B01001_024E", "B01001_025E",
        "B01001_044E", "B01001_045E", "B01001_046E",
        "B01001_047E", "B01001_048E", "B01001_049E",
    ]
    elderly_sum = acs_df[elderly_cols].sum(axis=1)
    result["pct_elderly"] = (elderly_sum / total_pop * 100).round(1)

    disability_pop = acs_df["C18108_001E"].clip(lower=1)
    disability_count = acs_df["C18108_007E"].fillna(0) + acs_df["C18108_011E"].fillna(0)
    result["pct_disability"] = (disability_count / disability_pop * 100).round(1)

    geo_gdf["tract_id"] = geo_gdf["GEOID"].astype(str).str.zfill(11)
    merged = geo_gdf[["tract_id", "geometry"]].merge(result, on="tract_id", how="inner")
    gdf = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")
    gdf = gdf.rename(columns={"geometry": "geom"}).set_geometry("geom")

    logger.info("Merged %d tracts with demographics", len(gdf))
    return gdf


def produce(gdf: gpd.GeoDataFrame) -> None:
    """Send each row to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        max_request_size=10_485_760,
    )
    topic = TOPICS["census"]

    for _, row in gdf.iterrows():
        record = row.drop("geom").to_dict()
        record["geom"] = row.geom.wkt
        producer.send(topic, value=serialize(record))

    producer.send(topic, value=END_OF_STREAM.encode("utf-8"))
    producer.flush()
    producer.close()
    logger.info("Produced %d records to %s", len(gdf), topic)


def main() -> None:
    logger.info("Starting Census ACS download for NYC")
    acs_df = fetch_acs_data()
    geo_gdf = fetch_tract_geometries()
    gdf = transform(acs_df, geo_gdf)
    produce(gdf)
    logger.info("Done. %d census tracts produced.", len(gdf))


if __name__ == "__main__":
    main()
