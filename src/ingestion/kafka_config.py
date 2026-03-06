"""Shared Kafka configuration for ingestion producers and consumer."""

import json
import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# One topic per dataset
TOPICS = {
    "lion": "ingestion.lion",
    "mta_ridership": "ingestion.mta_ridership",
    "bus_stops": "ingestion.bus_stops",
    "plow": "ingestion.plow",
    "pedestrian": "ingestion.pedestrian",
    "census": "ingestion.census",
}

# Sentinel value sent after all records to signal end-of-stream
END_OF_STREAM = "__END__"


def serialize(obj: dict) -> bytes:
    """Serialize a dict to JSON bytes, handling common types."""
    return json.dumps(obj, default=str).encode("utf-8")


def deserialize(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))
