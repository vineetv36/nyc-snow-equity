PYTHON ?= python3
PIP ?= pip3

.PHONY: help setup services services-stop db db-stop db-reset db-logs kafka-logs \
        download-all consume download-lion download-ridership download-stops \
        download-plow download-pedestrian download-census pipeline api live test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup: ## Install Python dependencies
	$(PIP) install -r requirements.txt

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
services: ## Start PostgreSQL + Kafka + Zookeeper
	docker compose up -d
	@echo "Waiting for database to be healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' nyc-snow-equity-db 2>/dev/null | grep -q healthy; do sleep 1; done
	@echo "Waiting for Kafka to be healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' nyc-snow-equity-kafka 2>/dev/null | grep -q healthy; do sleep 1; done
	@echo "All services ready."

services-stop: ## Stop all services
	docker compose down

db: ## Start PostgreSQL + PostGIS only
	docker compose up -d postgres
	@echo "Waiting for database to be healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' nyc-snow-equity-db 2>/dev/null | grep -q healthy; do sleep 1; done
	@echo "Database is ready."

db-stop: ## Stop all services
	docker compose down

db-reset: ## Drop and recreate all tables (empty the database)
	docker compose exec postgres psql -U snow_user -d nyc_snow_equity -c " \
		DO \$$\$$ DECLARE r RECORD; BEGIN \
			FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP \
				EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE'; \
			END LOOP; \
		END \$$\$$;"
	@echo "All tables dropped. Database is empty."

db-logs: ## Tail database logs
	docker compose logs -f postgres

kafka-logs: ## Tail Kafka logs
	docker compose logs -f kafka

# ---------------------------------------------------------------------------
# Data ingestion (all data streams through Kafka → PostGIS, no local files)
# ---------------------------------------------------------------------------
download-all: ## Fetch all datasets, stream through Kafka into PostGIS
	$(PYTHON) -m src.ingestion.run_all

consume: ## Run Kafka consumer standalone (loads topics into PostGIS)
	$(PYTHON) -m src.ingestion.consumer

download-lion: ## Produce LION street centerlines to Kafka
	$(PYTHON) -m src.ingestion.download_lion

download-ridership: ## Produce MTA bus ridership to Kafka
	$(PYTHON) -m src.ingestion.download_mta_ridership

download-stops: ## Produce GTFS bus stops to Kafka
	$(PYTHON) -m src.ingestion.download_bus_stops

download-plow: ## Produce PlowNYC GPS data to Kafka
	$(PYTHON) -m src.ingestion.download_plow_data

download-pedestrian: ## Produce pedestrian counts to Kafka
	$(PYTHON) -m src.ingestion.download_pedestrian

download-census: ## Produce Census ACS demographics to Kafka
	$(PYTHON) -m src.ingestion.download_census

# ---------------------------------------------------------------------------
# Processing pipeline
# ---------------------------------------------------------------------------
pipeline: ## Run the full batch processing pipeline
	@echo "TODO: implement batch pipeline (map matching → scoring → equity overlay)"

# ---------------------------------------------------------------------------
# API & frontend
# ---------------------------------------------------------------------------
api: ## Start the FastAPI server
	uvicorn src.api.main:app --host 0.0.0.0 --port 8080 --reload

# ---------------------------------------------------------------------------
# Live mode
# ---------------------------------------------------------------------------
live: ## Start live snow-event streaming pipeline
	@echo "TODO: implement live Kafka/Redis pipeline"

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
test: ## Run the test suite
	pytest tests/ -v
