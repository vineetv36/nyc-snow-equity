"""Run all ingestion scripts in the correct order.

Usage: python src/ingestion/run_all.py [--skip-db-check]
"""

import argparse
import importlib
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Ordered: LION first (FK target), ridership before bus_stops (joined during load)
SCRIPTS = [
    ("download_lion", "LION street centerline"),
    ("download_mta_ridership", "MTA bus ridership"),
    ("download_bus_stops", "GTFS bus stops"),
    ("download_plow_data", "PlowNYC GPS"),
    ("download_pedestrian", "Pedestrian counts"),
    ("download_census", "Census ACS demographics"),
]


def check_db():
    """Verify PostGIS is reachable."""
    try:
        from sqlalchemy import create_engine, text
        import os

        url = os.environ.get(
            "DATABASE_URL",
            "postgresql://snow_user:snow_pass@localhost:5432/nyc_snow_equity",
        )
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT PostGIS_Version()"))
        logger.info("Database connection OK")
        return True
    except Exception as e:
        logger.error("Cannot connect to PostGIS: %s", e)
        return False


def run_script(module_name: str, label: str) -> bool:
    """Import and run a single ingestion script's main()."""
    logger.info("=" * 60)
    logger.info("Starting: %s", label)
    logger.info("=" * 60)
    start = time.time()
    try:
        mod = importlib.import_module(f"src.ingestion.{module_name}")
        mod.main()
        elapsed = time.time() - start
        logger.info("Completed: %s (%.1fs)", label, elapsed)
        return True
    except Exception:
        elapsed = time.time() - start
        logger.exception("FAILED: %s (%.1fs)", label, elapsed)
        return False


def main():
    parser = argparse.ArgumentParser(description="Run all ingestion scripts")
    parser.add_argument("--skip-db-check", action="store_true", help="Skip database connectivity check")
    parser.add_argument("--only", nargs="+", help="Run only these scripts (e.g. download_lion download_census)")
    args = parser.parse_args()

    if not args.skip_db_check and not check_db():
        logger.error("Start the database first: docker compose up -d")
        sys.exit(1)

    scripts = SCRIPTS
    if args.only:
        scripts = [(name, label) for name, label in SCRIPTS if name in args.only]
        if not scripts:
            logger.error("No matching scripts found. Available: %s", [s[0] for s in SCRIPTS])
            sys.exit(1)

    results = {}
    total_start = time.time()

    for module_name, label in scripts:
        ok = run_script(module_name, label)
        results[label] = ok

    total_elapsed = time.time() - total_start

    logger.info("=" * 60)
    logger.info("SUMMARY (%.1fs total)", total_elapsed)
    logger.info("=" * 60)
    for label, ok in results.items():
        status = "OK" if ok else "FAILED"
        logger.info("  %-30s %s", label, status)

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
