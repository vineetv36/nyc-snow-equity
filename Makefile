.PHONY: help setup db db-stop db-logs download-all pipeline api live test clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
setup: ## Install Python dependencies
	pip install -r requirements.txt

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
db: ## Start PostgreSQL + PostGIS
	docker compose up -d postgres
	@echo "Waiting for database to be healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' nyc-snow-equity-db 2>/dev/null | grep -q healthy; do sleep 1; done
	@echo "Database is ready."

db-stop: ## Stop the database
	docker compose down

db-logs: ## Tail database logs
	docker compose logs -f postgres

# ---------------------------------------------------------------------------
# Data ingestion
# ---------------------------------------------------------------------------
download-all: ## Download all datasets (~15 min first time)
	python -m src.ingestion.run_all

download-lion: ## Download LION street centerlines only
	python -m src.ingestion.download_lion

download-ridership: ## Download MTA bus ridership only
	python -m src.ingestion.download_mta_ridership

download-stops: ## Download GTFS bus stops only
	python -m src.ingestion.download_bus_stops

download-plow: ## Download PlowNYC GPS data only
	python -m src.ingestion.download_plow_data

download-pedestrian: ## Download pedestrian counts only
	python -m src.ingestion.download_pedestrian

download-census: ## Download Census ACS demographics only
	python -m src.ingestion.download_census

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

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean: ## Remove downloaded raw data files
	rm -rf data/raw/*
	@echo "Cleaned data/raw/"
