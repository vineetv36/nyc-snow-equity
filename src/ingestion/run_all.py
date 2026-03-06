"""Run all ingestion producers then the Kafka consumer to load into PostGIS.

Usage: python -m src.ingestion.run_all [--skip-db-check] [--only ...]
"""

import argparse
import importlib
import logging
import sys
import threading
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Ordered: LION first (FK target), ridership before bus_stops (Kafka join)
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


def check_kafka():
    """Verify Kafka is reachable."""
    try:
        from kafka import KafkaProducer
        from src.ingestion.kafka_config import KAFKA_BOOTSTRAP

        producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        producer.close()
        logger.info("Kafka connection OK")
        return True
    except Exception as e:
        logger.error("Cannot connect to Kafka: %s", e)
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


def run_consumer(topic_names: list[str]) -> bool:
    """Run the Kafka consumer to load all topics into PostGIS."""
    logger.info("=" * 60)
    logger.info("Starting: Kafka consumer → PostGIS")
    logger.info("=" * 60)
    start = time.time()
    try:
        from src.ingestion.consumer import run_consumer as _run
        _run(topic_names, timeout_ms=60_000)
        elapsed = time.time() - start
        logger.info("Completed: Kafka consumer (%.1fs)", elapsed)
        return True
    except Exception:
        elapsed = time.time() - start
        logger.exception("FAILED: Kafka consumer (%.1fs)", elapsed)
        return False


def main():
    parser = argparse.ArgumentParser(description="Run all ingestion scripts via Kafka")
    parser.add_argument("--skip-db-check", action="store_true", help="Skip database connectivity check")
    parser.add_argument("--skip-kafka-check", action="store_true", help="Skip Kafka connectivity check")
    parser.add_argument("--only", nargs="+", help="Run only these scripts (e.g. download_lion download_census)")
    args = parser.parse_args()

    if not args.skip_db_check and not check_db():
        logger.error("Start services first: docker compose up -d")
        sys.exit(1)

    if not args.skip_kafka_check and not check_kafka():
        logger.error("Start Kafka first: docker compose up -d kafka")
        sys.exit(1)

    scripts = SCRIPTS
    if args.only:
        scripts = [(name, label) for name, label in SCRIPTS if name in args.only]
        if not scripts:
            logger.error("No matching scripts found. Available: %s", [s[0] for s in SCRIPTS])
            sys.exit(1)

    # Determine which Kafka topics the selected scripts will produce to
    from src.ingestion.kafka_config import TOPICS
    script_to_topic = {
        "download_lion": TOPICS["lion"],
        "download_mta_ridership": TOPICS["mta_ridership"],
        "download_bus_stops": TOPICS["bus_stops"],
        "download_plow_data": TOPICS["plow"],
        "download_pedestrian": TOPICS["pedestrian"],
        "download_census": TOPICS["census"],
    }
    active_topics = [script_to_topic[name] for name, _ in scripts if name in script_to_topic]

    # Start consumer in background thread
    consumer_result = [None]

    def consumer_thread():
        consumer_result[0] = run_consumer(active_topics)

    t = threading.Thread(target=consumer_thread, daemon=True)
    t.start()

    # Give consumer a moment to subscribe
    time.sleep(2)

    # Run producers sequentially
    results = {}
    total_start = time.time()

    for module_name, label in scripts:
        ok = run_script(module_name, label)
        results[label] = ok

    # Wait for consumer to finish
    t.join(timeout=120)
    results["Kafka consumer → PostGIS"] = consumer_result[0] or False

    total_elapsed = time.time() - total_start

    logger.info("=" * 60)
    logger.info("SUMMARY (%.1fs total)", total_elapsed)
    logger.info("=" * 60)
    for label, ok in results.items():
        status = "OK" if ok else "FAILED"
        logger.info("  %-35s %s", label, status)

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
