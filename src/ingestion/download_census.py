"""Download Census ACS demographic data for NYC tracts and load into PostGIS.

Fetches ACS 5-Year estimates (income, race, poverty, age) and tract
geometries from the Census Bureau API / TIGER shapefiles, saves as
Parquet and GeoJSON, and loads into the census_tracts PostGIS table.
"""

import logging
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/census")
CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY", "")
ACS_YEAR = os.environ.get("ACS_YEAR", "2022")

# NYC FIPS codes: 36 = NY state; county codes: 005=Bronx, 047=Brooklyn,
# 061=Manhattan, 081=Queens, 085=Staten Island
NYC_COUNTIES = ["005", "047", "061", "081", "085"]

# ACS 5-Year variables
# B19013_001E = Median household income
# B02001_001E = Total population
# B02001_002E = White alone
# B02001_003E = Black alone
# B03003_003E = Hispanic/Latino
# B17001_002E = Below poverty level
# B01001_020E-025E + B01001_044E-049E = Population 65+
# C18108_001E = Total civilian pop for disability
# C18108_007E + C18108_011E = With disability (18-64 + 65+)
ACS_VARIABLES = [
    "B19013_001E",  # median income
    "B02001_001E",  # total pop
    "B02001_002E",  # white
    "B02001_003E",  # black
    "B03003_003E",  # hispanic
    "B17001_002E",  # poverty
    # 65+ male: 020-025, 65+ female: 044-049
    "B01001_020E",
    "B01001_021E",
    "B01001_022E",
    "B01001_023E",
    "B01001_024E",
    "B01001_025E",
    "B01001_044E",
    "B01001_045E",
    "B01001_046E",
    "B01001_047E",
    "B01001_048E",
    "B01001_049E",
    # Disability
    "C18108_001E",
    "C18108_007E",
    "C18108_011E",
]

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://snow_user:snow_pass@localhost:5432/nyc_snow_equity",
)


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
    """Fetch census tract boundaries from TIGER/Line shapefiles."""
    # Use the Census TIGER web service for tract boundaries
    tiger_url = (
        f"https://www2.census.gov/geo/tiger/TIGER{ACS_YEAR}/TRACT/"
        f"tl_{ACS_YEAR}_36_tract.zip"
    )
    logger.info("Downloading tract geometries from %s", tiger_url)

    try:
        gdf = gpd.read_file(tiger_url)
    except Exception:
        # Fallback: try the cartographic boundary file
        carto_url = (
            f"https://www2.census.gov/geo/tiger/GENZ{ACS_YEAR}/shp/"
            f"cb_{ACS_YEAR}_36_tract_500k.zip"
        )
        logger.info("Falling back to cartographic boundary: %s", carto_url)
        gdf = gpd.read_file(carto_url)

    # Filter to NYC counties
    gdf["COUNTYFP"] = gdf["COUNTYFP"].astype(str).str.zfill(3)
    gdf = gdf[gdf["COUNTYFP"].isin(NYC_COUNTIES)].copy()

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    logger.info("Loaded %d NYC tract geometries", len(gdf))
    return gdf


def transform(acs_df: pd.DataFrame, geo_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Merge ACS demographics with tract geometries."""
    # Build tract FIPS: state (2) + county (3) + tract (6) = 11 chars
    acs_df["tract_id"] = (
        acs_df["state"].str.zfill(2)
        + acs_df["county"].str.zfill(3)
        + acs_df["tract"].str.zfill(6)
    )

    # Convert numeric columns
    for col in ACS_VARIABLES:
        if col in acs_df.columns:
            acs_df[col] = pd.to_numeric(acs_df[col], errors="coerce")

    # Compute derived fields
    total_pop = acs_df["B02001_001E"].clip(lower=1)  # avoid div by zero

    result = pd.DataFrame()
    result["tract_id"] = acs_df["tract_id"]
    result["median_income"] = acs_df["B19013_001E"]
    result["total_population"] = acs_df["B02001_001E"]

    result["pct_white"] = (acs_df["B02001_002E"] / total_pop * 100).round(1)
    result["pct_black"] = (acs_df["B02001_003E"] / total_pop * 100).round(1)
    result["pct_hispanic"] = (acs_df["B03003_003E"] / total_pop * 100).round(1)
    result["pct_minority"] = (100 - result["pct_white"]).clip(lower=0)
    result["pct_poverty"] = (acs_df["B17001_002E"] / total_pop * 100).round(1)

    # Elderly: sum of 65+ age groups (male + female)
    elderly_cols = [
        "B01001_020E", "B01001_021E", "B01001_022E",
        "B01001_023E", "B01001_024E", "B01001_025E",
        "B01001_044E", "B01001_045E", "B01001_046E",
        "B01001_047E", "B01001_048E", "B01001_049E",
    ]
    elderly_sum = acs_df[elderly_cols].sum(axis=1)
    result["pct_elderly"] = (elderly_sum / total_pop * 100).round(1)

    # Disability
    disability_pop = acs_df["C18108_001E"].clip(lower=1)
    disability_count = acs_df["C18108_007E"].fillna(0) + acs_df["C18108_011E"].fillna(0)
    result["pct_disability"] = (disability_count / disability_pop * 100).round(1)

    # Merge with geometries
    geo_gdf["tract_id"] = geo_gdf["GEOID"].astype(str).str.zfill(11)
    merged = geo_gdf[["tract_id", "geometry"]].merge(result, on="tract_id", how="inner")
    gdf = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")
    gdf = gdf.rename(columns={"geometry": "geom"}).set_geometry("geom")

    logger.info("Merged %d tracts with demographics", len(gdf))
    return gdf


def load_to_postgis(gdf: gpd.GeoDataFrame) -> None:
    """Load census tracts into PostGIS."""
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE census_tracts CASCADE"))

    gdf.to_postgis(
        "census_tracts",
        engine,
        if_exists="append",
        index=False,
        dtype={"geom": "Geometry(MultiPolygon, 4326)"},
    )
    logger.info("Loaded %d census tracts into PostGIS", len(gdf))


def save_raw(acs_df: pd.DataFrame, gdf: gpd.GeoDataFrame, output_dir: Path) -> None:
    """Save raw ACS data as Parquet and merged tracts as GeoJSON."""
    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir / "acs_demographics.parquet"
    acs_df.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info("Saved %s", parquet_path)

    if not gdf.empty:
        geojson_path = output_dir / "census_tracts.geojson"
        gdf.to_file(geojson_path, driver="GeoJSON")
        logger.info("Saved %s", geojson_path)


def main() -> None:
    logger.info("Starting Census ACS download for NYC")
    acs_df = fetch_acs_data()
    geo_gdf = fetch_tract_geometries()
    gdf = transform(acs_df, geo_gdf)
    save_raw(acs_df, gdf, RAW_DIR)
    load_to_postgis(gdf)
    logger.info("Done. %d census tracts loaded.", len(gdf))


if __name__ == "__main__":
    main()
