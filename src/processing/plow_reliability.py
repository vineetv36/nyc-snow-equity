"""Aggregate plow response times into per-segment reliability scores.

Reads from ``plow_events`` (populated by ``plow_response_time``) and writes
reliability columns into ``segment_scores``.

Reliability score formula (0 = never plowed, 100 = always plowed quickly)::

    norm_response = min(avg_response_minutes, 1440) / 1440
    miss_penalty  = storms_never_plowed / total_storms
    reliability_score = round((1 - (0.6 * norm_response + 0.4 * miss_penalty)) * 100)

Usage::

    python -m src.processing.plow_reliability [--db DATABASE_URL]
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

MAX_RESPONSE_MINUTES = 1440  # 24 hours cap for normalization
WEIGHT_RESPONSE = 0.6
WEIGHT_MISS = 0.4


def compute_reliability(db_url: str | None = None) -> int:
    """Aggregate plow_events into segment_scores reliability columns.

    Returns the number of segments scored.
    """
    engine = create_engine(db_url or DB_URL)

    # 1. Read plow_events
    query = text("""
        SELECT segment_id,
               storm_date,
               response_minutes,
               total_passes
        FROM   plow_events
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        logger.warning("plow_events is empty — run plow_response_time first")
        return 0

    total_storms = df["storm_date"].nunique()
    logger.info("Computing reliability across %d storms", total_storms)

    # 2. Per-segment aggregation
    agg = df.groupby("segment_id").agg(
        avg_response_min=("response_minutes", "mean"),
        worst_response_min=("response_minutes", "max"),
        storms_covered=("total_passes", lambda x: (x > 0).sum()),
        storms_missed=("total_passes", lambda x: (x == 0).sum()),
    ).reset_index()

    # 3. Compute reliability score
    norm_response = agg["avg_response_min"].clip(upper=MAX_RESPONSE_MINUTES).fillna(MAX_RESPONSE_MINUTES) / MAX_RESPONSE_MINUTES
    miss_penalty = agg["storms_missed"] / total_storms
    agg["reliability_score"] = (
        (1 - (WEIGHT_RESPONSE * norm_response + WEIGHT_MISS * miss_penalty)) * 100
    ).round()

    # 4. Upsert into segment_scores
    scores = agg[
        ["segment_id", "avg_response_min", "worst_response_min",
         "storms_covered", "storms_missed", "reliability_score"]
    ].copy()

    with engine.begin() as conn:
        # Use upsert: insert or update on conflict
        for _, row in scores.iterrows():
            conn.execute(text("""
                INSERT INTO segment_scores
                    (segment_id, avg_response_min, worst_response_min,
                     storms_covered, storms_missed, reliability_score)
                VALUES
                    (:segment_id, :avg_response_min, :worst_response_min,
                     :storms_covered, :storms_missed, :reliability_score)
                ON CONFLICT (segment_id) DO UPDATE SET
                    avg_response_min   = EXCLUDED.avg_response_min,
                    worst_response_min = EXCLUDED.worst_response_min,
                    storms_covered     = EXCLUDED.storms_covered,
                    storms_missed      = EXCLUDED.storms_missed,
                    reliability_score  = EXCLUDED.reliability_score
            """), {
                "segment_id": row["segment_id"],
                "avg_response_min": None if pd.isna(row["avg_response_min"]) else float(row["avg_response_min"]),
                "worst_response_min": None if pd.isna(row["worst_response_min"]) else float(row["worst_response_min"]),
                "storms_covered": int(row["storms_covered"]),
                "storms_missed": int(row["storms_missed"]),
                "reliability_score": float(row["reliability_score"]),
            })

    logger.info("Wrote reliability scores for %d segments to segment_scores", len(scores))
    return len(scores)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute plow reliability scores")
    parser.add_argument("--db", default=None, help="Database URL override")
    args = parser.parse_args()
    compute_reliability(db_url=args.db)


if __name__ == "__main__":
    main()
