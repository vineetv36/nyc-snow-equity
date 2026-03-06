"""Download NYC LION street centerline network and load into PostGIS.

Fetches the LION geodatabase from NYC Open Data / DCP, extracts street
segments, computes lengths, saves as GeoJSON, and loads into the
street_segments PostGIS table.
"""

import logging
import os
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import requests
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# NYC DCP LION dataset — available as shapefile from Bytes of the Big Apple
# Also available on NYC Open Data as "NYC Street Centerline (CSCL)"
LION_URL = os.environ.get(
    "LION_URL",
    "https://data.cityofnewyork.us/api/geospatial/exjm-f27b?method=export&type=GeoJSON",
)

RAW_DIR = Path("data/raw/lion")

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://snow_user:snow_pass@localhost:5432/nyc_snow_equity",
)

# Borough code mapping
BOROUGH_MAP = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island",
    "MN": "Manhattan",
    "BX": "Bronx",
    "BK": "Brooklyn",
    "QN": "Queens",
    "SI": "Staten Island",
}

# Road class mapping from LION RW_TYPE or FeatureTyp codes
ROAD_CLASS_MAP = {
    "1": "highway",
    "2": "arterial",
    "3": "collector",
    "4": "local",
    "5": "private",
    "6": "alley",
    "9": "other",
}


def download_lion(output_dir: Path) -> gpd.GeoDataFrame:
    """Download LION data and return as GeoDataFrame."""
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Downloading LION street centerline from %s", LION_URL)
    resp = requests.get(LION_URL, timeout=300, stream=True)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")

    if "zip" in content_type or LION_URL.endswith(".zip"):
        zip_path = output_dir / "lion.zip"
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info("Extracting ZIP archive")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(output_dir / "lion_extracted")
        # Find shapefile or geojson inside
        shp_files = list((output_dir / "lion_extracted").rglob("*.shp"))
        geojson_files = list((output_dir / "lion_extracted").rglob("*.geojson"))
        if shp_files:
            gdf = gpd.read_file(shp_files[0])
        elif geojson_files:
            gdf = gpd.read_file(geojson_files[0])
        else:
            raise FileNotFoundError("No shapefile or GeoJSON found in ZIP")
    else:
        # Direct GeoJSON
        geojson_path = output_dir / "lion.geojson"
        with open(geojson_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        gdf = gpd.read_file(geojson_path)

    logger.info("Loaded %d features", len(gdf))
    return gdf


def transform(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Transform LION data to match the street_segments schema."""
    # Normalize column names to lowercase
    gdf.columns = [c.lower() for c in gdf.columns]

    # Ensure CRS is WGS84
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Map columns — LION/CSCL field names vary across versions
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

    # Convert segment_id to string, ensure uniqueness
    out["segment_id"] = out["segment_id"].str.strip()

    # Map borough codes to names
    out["borough"] = out["borough"].map(BOROUGH_MAP).fillna(out["borough"])

    # Map road class codes
    if out["road_class"].str.match(r"^\d$").any():
        out["road_class"] = out["road_class"].map(ROAD_CLASS_MAP).fillna("other")

    # Snow emergency route flag
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

    # Drop duplicates by segment_id, keep first
    out = out.drop_duplicates(subset="segment_id", keep="first")

    # Filter out empty/invalid segment IDs
    out = out[out["segment_id"].str.len() > 0]

    out = out.rename(columns={"geometry": "geom"}).set_geometry("geom")

    logger.info("Transformed to %d street segments", len(out))
    return out


def load_to_postgis(gdf: gpd.GeoDataFrame) -> None:
    """Bulk load street segments into PostGIS."""
    engine = create_engine(DB_URL)

    # Truncate existing data
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE street_segments CASCADE"))

    gdf.to_postgis(
        "street_segments",
        engine,
        if_exists="append",
        index=False,
        dtype={"geom": "Geometry(LineString, 4326)"},
    )
    logger.info("Loaded %d segments into PostGIS", len(gdf))


def save_raw(gdf: gpd.GeoDataFrame, output_dir: Path) -> None:
    """Save processed segments as GeoJSON for reproducibility."""
    out_path = output_dir / "street_segments.geojson"
    gdf.to_file(out_path, driver="GeoJSON")
    logger.info("Saved %s", out_path)


def main() -> None:
    logger.info("Starting LION street centerline download")
    gdf = download_lion(RAW_DIR)
    gdf = transform(gdf)
    save_raw(gdf, RAW_DIR)
    load_to_postgis(gdf)
    logger.info("Done. %d street segments loaded.", len(gdf))


if __name__ == "__main__":
    main()
