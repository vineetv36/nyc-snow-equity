"""Compute per-segment per-storm plow response times.

Reads from ``plow_gps_matched`` (raw GPS hits already map-matched to LION
segments) and writes one row per segment × storm into ``plow_events``.

Usage::

    python -m src.processing.plow_response_time [--db DATABASE_URL]
"""

from __future__ import annotations

import argparse
import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://snow_user:snow_pass@localhost:5432/nyc_snow_equity",
)

# If no NOAA snowfall_start is available, assume snow began this many minutes
# before the earliest plow deployment in a storm.
DEFAULT_LEAD_MINUTES = 60


def _estimate_snowfall_starts(engine) -> pd.DataFrame:
    """Return a DataFrame with columns [storm_date, snowfall_start].

    Uses the earliest plow GPS timestamp per storm minus a fixed lead time as a
    proxy when real NOAA data is unavailable.
    """
    query = text("""
        SELECT storm_date,
               MIN(timestamp) - INTERVAL ':lead_min minutes' AS snowfall_start
        FROM   plow_gps_matched
        GROUP  BY storm_date
        ORDER  BY storm_date
    """.replace(":lead_min", str(DEFAULT_LEAD_MINUTES)))

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    logger.info("Estimated snowfall starts for %d storms", len(df))
    return df


def compute_response_times(db_url: str | None = None) -> int:
    """Compute per-segment per-storm response metrics and write to plow_events.

    Returns the number of rows written.
    """
    engine = create_engine(db_url or DB_URL)

    # 1. Estimate snowfall start per storm
    storm_starts = _estimate_snowfall_starts(engine)
    if storm_starts.empty:
        logger.warning("No storms found in plow_gps_matched — nothing to compute")
        return 0

    # 2. Aggregate GPS hits → per-segment per-storm metrics
    agg_query = text("""
        SELECT segment_id,
               storm_date,
               MIN(timestamp) AS first_plow,
               COUNT(*)       AS total_passes,
               CASE WHEN COUNT(*) > 1
                    THEN EXTRACT(EPOCH FROM MAX(timestamp) - MIN(timestamp))
                         / 60.0 / (COUNT(*) - 1)
                    ELSE NULL
               END            AS avg_pass_interval_min
        FROM   plow_gps_matched
        GROUP  BY segment_id, storm_date
    """)

    with engine.connect() as conn:
        gps_agg = pd.read_sql(agg_query, conn)
    logger.info("Aggregated GPS data: %d segment-storm pairs", len(gps_agg))

    # 3. Get all segment × storm combos so we capture segments never plowed
    cross_query = text("""
        SELECT s.segment_id, d.storm_date
        FROM   street_segments s
        CROSS  JOIN (SELECT DISTINCT storm_date FROM plow_gps_matched) d
    """)
    with engine.connect() as conn:
        all_combos = pd.read_sql(cross_query, conn)

    # 4. Merge: left join so un-plowed segments get NULLs
    merged = all_combos.merge(gps_agg, on=["segment_id", "storm_date"], how="left")

    # 5. Attach snowfall_start and compute response_minutes
    storm_starts["storm_date"] = pd.to_datetime(storm_starts["storm_date"]).dt.date
    merged["storm_date_key"] = pd.to_datetime(merged["storm_date"]).dt.date
    merged = merged.merge(
        storm_starts, left_on="storm_date_key", right_on="storm_date", suffixes=("", "_drop"),
    )
    merged.drop(columns=[c for c in merged.columns if c.endswith("_drop")], inplace=True)
    merged.drop(columns=["storm_date_key"], inplace=True)

    merged["first_plow"] = pd.to_datetime(merged["first_plow"], utc=True, errors="coerce")
    merged["snowfall_start"] = pd.to_datetime(merged["snowfall_start"], utc=True, errors="coerce")
    merged["response_minutes"] = (
        (merged["first_plow"] - merged["snowfall_start"]).dt.total_seconds() / 60.0
    )

    # Ensure integer passes; NaN → 0 for never-plowed segments
    merged["total_passes"] = merged["total_passes"].fillna(0).astype(int)

    # 6. Write to plow_events
    out = merged[
        ["segment_id", "storm_date", "snowfall_start", "first_plow",
         "response_minutes", "total_passes", "avg_pass_interval_min"]
    ].copy()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE plow_events"))
    out.to_sql("plow_events", engine, if_exists="append", index=False)
    logger.info("Wrote %d rows to plow_events", len(out))
    return len(out)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute plow response times")
    parser.add_argument("--db", default=None, help="Database URL override")
    args = parser.parse_args()
    compute_response_times(db_url=args.db)


if __name__ == "__main__":
    main()
