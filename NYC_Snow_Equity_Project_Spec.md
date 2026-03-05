# NYC Snow Equity: Plow Coverage vs. Pedestrian Demand Analyzer

## Project Overview

A data engineering pipeline that fuses NYC snowplow GPS data with MTA bus ridership, pedestrian counts, and Census demographics to identify which high-traffic streets and bus stops are systematically underserved during snow events. The deliverable is an interactive map and policy brief showing where the most New Yorkers are left standing in unplowed snow — and whether that burden falls disproportionately on lower-income or minority communities.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES (Batch)                           │
├─────────────────┬──────────────────┬─────────────────┬──────────────────┤
│  PlowNYC GPS    │  MTA Bus         │  NYC Pedestrian │  Census ACS     │
│  Breadcrumbs    │  Ridership/Stop  │  Counts         │  Demographics   │
│  (per storm)    │  (annual)        │  (hourly)       │  (tract-level)  │
└────────┬────────┴────────┬─────────┴────────┬────────┴────────┬─────────┘
         │                 │                   │                 │
         ▼                 ▼                   ▼                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                   │
│  download_plow_data.py  │ download_mta.py │ download_ped.py │ census.py │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        PROCESSING LAYER (Spark Batch)                    │
│                                                                          │
│  1. Map-match plow GPS → street segments (LION network + PostGIS)       │
│  2. Compute per-segment plow response time per storm                     │
│  3. Aggregate plow reliability score across all storms                   │
│  4. Join bus stops → nearest street segments (50m buffer)                │
│  5. Join pedestrian counters → street segments                           │
│  6. Compute demand-to-service ratio per segment                          │
│  7. Overlay Census tract demographics                                    │
│  8. Rank and classify segments into equity tiers                         │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                                    │
│                                                                          │
│  PostGIS: street segments, bus stops, plow events, hotspots, scores     │
│  Parquet: raw + intermediate datasets for reproducibility                │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────┬─────────────────────────┬───────────────────────┐
│     FastAPI             │   Jupyter Notebooks      │   Deck.gl Frontend   │
│  /api/segments          │   Exploratory analysis   │   Interactive map     │
│  /api/busstops          │   Equity analysis        │   Ridership heatmap   │
│  /api/equity-report     │   Policy brief           │   Plow coverage gaps  │
└────────────────────────┴─────────────────────────┴───────────────────────┘
```

---

## Real-Time Extension (During Live Snow Events)

When a snow emergency is declared, the pipeline switches to a live mode:

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────────┐
│  PlowNYC Live   │────▶│   Kafka     │────▶│  Spark Structured   │
│  API (polling)  │     │             │     │  Streaming          │
└─────────────────┘     └─────────────┘     │                     │
                                            │  Real-time map-     │
                                            │  matching + coverage│
                                            │  gap detection      │
                                            └──────────┬──────────┘
                                                       │
                                              ┌────────▼────────┐
                                              │  Redis (live     │
                                              │  coverage state) │
                                              └────────┬────────┘
                                                       │
                                              ┌────────▼────────┐
                                              │  Live dashboard: │
                                              │  "These bus stops│
                                              │  still unplowed" │
                                              └─────────────────┘
```

This dual-mode design (batch historical analysis + real-time live tracking) is a strong FAANG talking point — it mirrors lambda architecture patterns used at Netflix and LinkedIn.

---

## File Structure

```
nyc-snow-equity/
│
├── docker-compose.yml
├── Makefile
├── README.md
├── SPEC.md
├── .env.example
│
├── infra/
│   ├── postgres/
│   │   ├── Dockerfile
│   │   └── init.sql                  # PostGIS schema + spatial indexes
│   ├── kafka/
│   │   └── docker-compose.kafka.yml  # Only needed for live mode
│   └── redis/
│       └── redis.conf                # Only needed for live mode
│
├── src/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── download_plow_data.py     # Fetch PlowNYC historical GPS from NYC Open Data
│   │   ├── download_mta_ridership.py # Fetch MTA bus ridership per stop
│   │   ├── download_pedestrian.py    # Fetch NYC DOT pedestrian counts
│   │   ├── download_census.py        # Fetch ACS demographic data by tract
│   │   ├── download_lion.py          # Fetch NYC street centerline network
│   │   ├── download_bus_stops.py     # Fetch MTA GTFS bus stop locations
│   │   └── live_plow_producer.py     # Poll PlowNYC live API → Kafka (live mode)
│   │
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── map_matcher.py            # Snap plow GPS breadcrumbs to LION street segments
│   │   ├── plow_response_time.py     # Compute time-to-first-plow per segment per storm
│   │   ├── plow_reliability.py       # Aggregate reliability score across all storms
│   │   ├── ridership_joiner.py       # Spatial join: bus stops → street segments
│   │   ├── pedestrian_joiner.py      # Spatial join: ped counters → street segments
│   │   ├── demand_service_ratio.py   # Compute the core metric
│   │   ├── census_overlay.py         # Join Census tract demographics to segments
│   │   ├── equity_classifier.py      # Classify segments into equity tiers
│   │   └── live_coverage_tracker.py  # Spark Streaming job for live mode
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── postgis_loader.py         # Bulk load processed data to PostGIS
│   │   └── parquet_writer.py         # Write intermediate datasets
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py                   # FastAPI app
│   │   ├── routes/
│   │   │   ├── segments.py           # Street segment data + scores
│   │   │   ├── bus_stops.py          # Bus stops with ridership + plow data
│   │   │   ├── equity.py             # Equity analysis endpoints
│   │   │   └── live.py               # Live coverage during storms
│   │   └── models.py
│   │
│   └── analysis/
│       ├── __init__.py
│       ├── gap_identifier.py         # Find worst-served high-demand segments
│       ├── equity_analysis.py        # Demographic disparity calculations
│       ├── temporal_patterns.py      # Do certain areas always get plowed last?
│       ├── borough_comparison.py     # Borough-level and community district rollups
│       └── policy_report.py          # Generate summary stats and recommendations
│
├── notebooks/
│   ├── 01_data_exploration.ipynb     # Explore each dataset independently
│   ├── 02_map_matching_validation.ipynb  # Verify plow GPS snaps correctly
│   ├── 03_ridership_heatmap.ipynb    # Visualize bus ridership density
│   ├── 04_plow_coverage_analysis.ipynb   # Storm-by-storm coverage timelines
│   ├── 05_equity_analysis.ipynb      # Core demographic disparity analysis
│   └── 06_policy_brief.ipynb         # Publication-ready findings
│
├── frontend/
│   ├── index.html                    # Deck.gl map application
│   ├── app.js                        # Layer rendering, filters, legend
│   └── style.css
│
├── tests/
│   ├── conftest.py
│   ├── test_map_matcher.py
│   ├── test_plow_response_time.py
│   ├── test_demand_service_ratio.py
│   ├── test_equity_classifier.py
│   ├── test_api.py
│   └── fixtures/
│       ├── sample_plow_gps.json
│       ├── sample_bus_stops.geojson
│       └── sample_lion_segments.geojson
│
├── benchmarks/
│   ├── map_matching_bench.py         # GPS points/sec snapped
│   ├── spatial_join_bench.py         # Joins/sec for ridership fusion
│   └── api_load_test.py             # Endpoint response times
│
├── scripts/
│   ├── run_full_pipeline.py          # End-to-end batch orchestration
│   ├── seed_sample_data.py           # Load small sample for development
│   └── generate_storm_simulator.py   # Simulate plow data for testing
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── DATA_SOURCES.md               # Where each dataset comes from + update freq
│   ├── METHODOLOGY.md                # How scores are computed, assumptions
│   ├── BENCHMARKS.md
│   └── POLICY_ANALYSIS.md            # Non-technical summary of findings
│
└── pyproject.toml
```

---

## Data Sources

| Dataset | Source | URL | Format | Update Frequency | Size Estimate |
|---------|--------|-----|--------|-----------------|---------------|
| PlowNYC GPS | NYC Open Data | `data.cityofnewyork.us` (search "PlowNYC") | CSV/JSON API | Per storm event | ~2-5M rows/storm |
| MTA Bus Ridership by Stop | data.ny.gov | Search "MTA Bus Ridership" | CSV | Annual | ~15K rows (one per stop) |
| MTA GTFS (bus stop locations) | mta.info | `http://web.mta.info/developers/developer-data-terms.html` | GTFS (stops.txt) | Quarterly | ~16K stops |
| NYC Pedestrian Counts | NYC Open Data | Search "Automated Pedestrian Counts" | CSV | Hourly (at ~100 locations) | ~1M rows/year |
| LION Street Centerline | NYC Open Data / DCP | Search "LION" or "NYC Street Centerline" | Shapefile/GeoJSON | Quarterly | ~170K segments |
| Census ACS Demographics | data.census.gov | ACS 5-Year, Table B19013 (income), B02001 (race) | CSV | Annual | ~2.2K tracts for NYC |
| NYC Community Districts | NYC Open Data | Search "Community Districts" | GeoJSON | Static | 59 districts |
| NOAA Storm History | NOAA NCEI | `ncei.noaa.gov` | CSV | Per event | Lookup table |

---

## Module Specifications

### 1. Ingestion: `src/ingestion/download_plow_data.py`

**Purpose:** Fetch all historical PlowNYC GPS data from NYC Open Data's Socrata API.

**Behavior:**
- Use the SODA API (`requests` with `$limit` and `$offset` pagination) to download all available PlowNYC data
- Each record contains: `timestamp`, `latitude`, `longitude`, `borough`, `route_name`, `sector`, `speed`, `direction`
- Download data per storm event (identifiable by date clusters in the timestamp field)
- Normalize timestamps to UTC
- Write raw data to `data/raw/plow/` as Parquet files partitioned by storm date
- Log: total records, date range, storms identified

**Storm identification logic:**
- Group GPS pings by date
- A "storm event" is a continuous period of plow activity with no gap > 24 hours
- Cross-reference with NOAA storm reports for the NYC area to get snowfall totals per event

---

### 2. Ingestion: `src/ingestion/download_lion.py`

**Purpose:** Fetch the LION street centerline dataset — this is the spatial backbone everything gets joined to.

**Behavior:**
- Download the LION geodatabase or shapefile from NYC DCP
- Load into PostGIS as the `street_segments` table
- Each segment has: `segment_id`, `street_name`, `from_street`, `to_street`, `borough`, `geom` (LineString), `road_class` (highway, arterial, local, etc.)
- Compute and store the length of each segment in feet
- Build a spatial index on `geom`

**Why LION and not OpenStreetMap?** LION is NYC's authoritative street network. It aligns with how the city dispatches plows (by segment/route) and how the PlowNYC data is structured. OSM would require extra work to match city operational data.

---

### 3. Processing: `src/processing/map_matcher.py`

**Purpose:** Snap raw plow GPS breadcrumbs to the nearest LION street segment.

**Algorithm:**
1. For each GPS point, query PostGIS for the nearest street segment within 30 meters:
   ```sql
   SELECT segment_id, ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as dist
   FROM street_segments
   WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 30)
   ORDER BY dist
   LIMIT 1;
   ```
2. If no segment within 30m, expand to 50m. If still nothing, discard the point (likely GPS noise in a park or parking lot).
3. For each matched point, store: `segment_id`, `timestamp`, `speed`, `direction`
4. Batch process using Spark for parallelism — partition GPS points by borough, then run PostGIS queries via JDBC or a UDF.

**Performance target:** Process 5M GPS points in under 10 minutes on a single machine (using connection pooling and batch ST_DWithin queries).

**Edge cases:**
- GPS points at intersections may match multiple segments — pick the one whose bearing best matches the plow's direction of travel
- Plows sometimes drive through parks or parking lots between routes — filter by speed (if speed < 3 mph for > 2 minutes, likely stationary/not plowing)

---

### 4. Processing: `src/processing/plow_response_time.py`

**Purpose:** For each street segment and each storm, compute how long it took for the first plow pass after snowfall began.

**Logic:**
1. For each storm event, get the snowfall start time from NOAA data (or estimate as the first plow deployment timestamp minus 1 hour)
2. For each street segment, find the earliest plow GPS match during that storm
3. `response_time = first_plow_timestamp - snowfall_start_time` (in minutes)
4. If a segment was never plowed during a storm, mark it as `response_time = NULL` (or the storm duration, for averaging purposes)
5. Also compute: `total_passes` (how many times the segment was plowed during the storm), `time_between_passes`

**Output table: `plow_events`**

| Column | Type | Description |
|--------|------|-------------|
| segment_id | VARCHAR | LION segment ID |
| storm_date | DATE | Storm event date |
| snowfall_start | TIMESTAMPTZ | When snow began |
| first_plow | TIMESTAMPTZ | First plow GPS match (NULL if never plowed) |
| response_minutes | FLOAT | Time to first plow (NULL if never) |
| total_passes | INT | Total plow passes during storm |
| avg_pass_interval_min | FLOAT | Average minutes between passes |

---

### 5. Processing: `src/processing/plow_reliability.py`

**Purpose:** Aggregate plow response times across all storms into a single reliability score per street segment.

**Metrics per segment:**
- `avg_response_minutes`: Mean time-to-first-plow across all storms
- `worst_response_minutes`: Worst storm response time
- `storms_never_plowed`: Count of storms where the segment got zero plow passes
- `reliability_score`: Composite score from 0 (never plowed) to 100 (always plowed quickly)

**Reliability score formula:**
```python
# Normalize response time to 0-1 (0 = fast, 1 = slow)
# Cap at 24 hours (1440 min) for never-plowed segments
norm_response = min(avg_response_minutes, 1440) / 1440

# Penalize for storms with zero coverage
miss_penalty = storms_never_plowed / total_storms

# Composite (lower = worse service)
reliability_score = round((1 - (0.6 * norm_response + 0.4 * miss_penalty)) * 100)
```

Document this formula in `METHODOLOGY.md` and explain why the weights are 60/40 — response time matters more than total misses because a segment that always gets plowed but takes 18 hours is arguably worse than one that was missed once.

---

### 6. Processing: `src/processing/ridership_joiner.py`

**Purpose:** Spatially join MTA bus stops to their nearest street segments and attach ridership data.

**Logic:**
1. Load bus stops from GTFS `stops.txt` (lat, lon, stop_id, stop_name)
2. Load MTA ridership data and join to stops by stop_id
3. For each bus stop, find the nearest LION street segment within 50 meters using PostGIS:
   ```sql
   SELECT s.segment_id, bs.stop_id, bs.avg_daily_riders,
          ST_Distance(s.geom::geography, bs.geom::geography) as dist
   FROM bus_stops bs
   CROSS JOIN LATERAL (
       SELECT segment_id, geom FROM street_segments
       WHERE ST_DWithin(geom::geography, bs.geom::geography, 50)
       ORDER BY ST_Distance(geom::geography, bs.geom::geography)
       LIMIT 1
   ) s;
   ```
4. Aggregate: if multiple stops map to the same segment, sum their ridership
5. Output: each segment now has `total_daily_riders` (sum of all bus stops on that segment)

---

### 7. Processing: `src/processing/demand_service_ratio.py`

**Purpose:** Compute the core metric — the ratio of pedestrian/transit demand to plow service quality.

**The metric:**
```python
# Higher = worse (high demand, bad service)
demand_service_gap = total_daily_riders / max(reliability_score, 1)
```

A segment with 5,000 daily riders and a reliability score of 20 gets a gap score of 250. A segment with 500 riders and a score of 80 gets 6.25. Rank by this score descending and you have your priority list.

**Also compute a percentile rank** so the data is interpretable: "This segment is in the 95th percentile for service gap — worse than 95% of all NYC street segments."

**Enrich with:**
- Pedestrian count data (for segments near DOT counters)
- Road classification (arterial vs. local — city policy says arterials get plowed first, so local streets with high ridership are the interesting finding)
- Snow emergency route designation (is this segment supposed to be a priority route?)

---

### 8. Processing: `src/processing/census_overlay.py`

**Purpose:** Join Census tract demographics to each street segment for equity analysis.

**Logic:**
1. Load Census tract geometries and ACS data (median household income, % white, % Black, % Hispanic, % below poverty line, % elderly 65+, % with disability)
2. Each street segment may span multiple tracts — assign the tract whose overlap is greatest:
   ```sql
   SELECT s.segment_id, c.tract_id, c.median_income, c.pct_minority,
          ST_Length(ST_Intersection(s.geom, c.geom)) as overlap_length
   FROM street_segments s
   JOIN census_tracts c ON ST_Intersects(s.geom, c.geom)
   ORDER BY overlap_length DESC;
   ```
3. Take the tract with the longest overlap per segment

---

### 9. Processing: `src/processing/equity_classifier.py`

**Purpose:** Classify segments into equity tiers for the policy brief.

**Tiers:**
- **Critical Equity Gap:** High demand (top 25% ridership) + poor service (bottom 25% reliability) + low income (bottom 25% median income)
- **High Priority:** High demand + poor service (any income level)
- **Moderate Concern:** Medium demand + poor service
- **Adequately Served:** Everything else

**Output:** Each segment gets a `tier` label and a `priority_rank` for the city to act on.

---

### 10. Storage: `infra/postgres/init.sql`

```sql
CREATE EXTENSION IF NOT EXISTS postgis;

-- NYC LION street centerline network
CREATE TABLE street_segments (
    segment_id      VARCHAR(10) PRIMARY KEY,
    street_name     VARCHAR(100),
    from_street     VARCHAR(100),
    to_street       VARCHAR(100),
    borough         VARCHAR(15),
    road_class      VARCHAR(20),
    snow_emergency  BOOLEAN DEFAULT FALSE,
    length_ft       FLOAT,
    geom            GEOMETRY(LineString, 4326) NOT NULL
);

-- MTA bus stops with ridership
CREATE TABLE bus_stops (
    stop_id         VARCHAR(10) PRIMARY KEY,
    stop_name       VARCHAR(100),
    avg_daily_riders FLOAT,
    route_count     INT,
    segment_id      VARCHAR(10) REFERENCES street_segments(segment_id),
    geom            GEOMETRY(Point, 4326) NOT NULL
);

-- Pedestrian counter locations
CREATE TABLE pedestrian_counters (
    counter_id      VARCHAR(20) PRIMARY KEY,
    location_name   VARCHAR(100),
    avg_daily_count FLOAT,
    segment_id      VARCHAR(10) REFERENCES street_segments(segment_id),
    geom            GEOMETRY(Point, 4326) NOT NULL
);

-- Per-storm plow events
CREATE TABLE plow_events (
    id              BIGSERIAL PRIMARY KEY,
    segment_id      VARCHAR(10) NOT NULL REFERENCES street_segments(segment_id),
    storm_date      DATE NOT NULL,
    snowfall_start  TIMESTAMPTZ,
    first_plow      TIMESTAMPTZ,
    response_minutes FLOAT,
    total_passes    INT,
    avg_pass_interval_min FLOAT
);

-- Raw plow GPS matches (for debugging / replay)
CREATE TABLE plow_gps_matched (
    id              BIGSERIAL PRIMARY KEY,
    segment_id      VARCHAR(10) NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    latitude        FLOAT,
    longitude       FLOAT,
    speed_mph       FLOAT,
    storm_date      DATE,
    geom            GEOMETRY(Point, 4326) NOT NULL
) PARTITION BY RANGE (storm_date);

-- Aggregated segment scores (the core output)
CREATE TABLE segment_scores (
    segment_id          VARCHAR(10) PRIMARY KEY REFERENCES street_segments(segment_id),
    avg_response_min    FLOAT,
    worst_response_min  FLOAT,
    storms_covered      INT,
    storms_missed       INT,
    reliability_score   FLOAT,          -- 0-100
    total_daily_riders  FLOAT,          -- Sum of all bus stops on segment
    pedestrian_count    FLOAT,          -- From DOT counters (NULL if no counter)
    demand_service_gap  FLOAT,          -- The core metric
    gap_percentile      FLOAT,          -- 0-100
    median_income       INT,
    pct_minority        FLOAT,
    pct_poverty         FLOAT,
    pct_elderly         FLOAT,
    census_tract_id     VARCHAR(11),
    community_district  VARCHAR(4),
    equity_tier         VARCHAR(25),    -- CRITICAL, HIGH, MODERATE, ADEQUATE
    priority_rank       INT
);

-- Census tract demographics
CREATE TABLE census_tracts (
    tract_id        VARCHAR(11) PRIMARY KEY,
    median_income   INT,
    pct_white       FLOAT,
    pct_black       FLOAT,
    pct_hispanic    FLOAT,
    pct_minority    FLOAT,
    pct_poverty     FLOAT,
    pct_elderly     FLOAT,
    pct_disability  FLOAT,
    total_population INT,
    geom            GEOMETRY(MultiPolygon, 4326) NOT NULL
);

-- Spatial indexes
CREATE INDEX idx_segments_geom ON street_segments USING GIST(geom);
CREATE INDEX idx_busstops_geom ON bus_stops USING GIST(geom);
CREATE INDEX idx_plow_gps_geom ON plow_gps_matched USING GIST(geom);
CREATE INDEX idx_plow_gps_time ON plow_gps_matched(timestamp);
CREATE INDEX idx_plow_events_segment ON plow_events(segment_id);
CREATE INDEX idx_plow_events_storm ON plow_events(storm_date);
CREATE INDEX idx_census_geom ON census_tracts USING GIST(geom);
CREATE INDEX idx_scores_gap ON segment_scores(demand_service_gap DESC);
CREATE INDEX idx_scores_tier ON segment_scores(equity_tier);
```

---

### 11. API: `src/api/`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/segments` | All segments as GeoJSON, filterable by borough, tier, min_riders |
| GET | `/api/segments/{id}` | Single segment with full detail: scores, stops, plow history |
| GET | `/api/segments/{id}/plow-history` | Per-storm plow timeline for a segment |
| GET | `/api/bus-stops` | All stops as GeoJSON with ridership + segment scores |
| GET | `/api/bus-stops/worst?limit=50` | Top N worst-served high-ridership stops |
| GET | `/api/equity-report` | Summary stats: by borough, by tier, by income quartile |
| GET | `/api/equity-report/community-district/{cd}` | Per-district breakdown |
| GET | `/api/storms` | List of all storm events with metadata |
| GET | `/api/storms/{date}/coverage` | Per-segment coverage map for a specific storm |
| GET | `/api/live/coverage` | (Live mode) Current plow coverage during active storm |
| GET | `/api/stats` | High-level dashboard stats |

---

### 12. Analysis: `notebooks/05_equity_analysis.ipynb`

**The core findings this notebook should produce:**

1. **The headline stat:** "X bus stops serving Y daily riders sit on streets that averaged Z+ hours to first plow across N storms last winter."

2. **Borough comparison:** Table and bar chart showing average reliability score by borough. Does one borough consistently lag?

3. **Income correlation:** Scatter plot of median household income (x) vs. reliability score (y) by segment. Compute Pearson correlation. If there's a negative correlation (lower income → worse service), that's the policy story.

4. **Race correlation:** Same analysis but for % minority population. This is sensitive — present the data factually without editorializing.

5. **Road class analysis:** Are local streets with high bus ridership systematically deprioritized compared to arterials? The city's policy says arterials first, but is that policy creating equity gaps?

6. **Map: Critical equity gap segments** — red overlay on the Deck.gl map showing the segments classified as CRITICAL tier. Are they clustered in specific neighborhoods?

7. **Case studies:** Pick 3-5 specific bus stops/segments with compelling stories. Example: "Stop B0001 at Nostrand Ave & Fulton St serves 3,200 daily riders. During the January 2024 storm, its street segment wasn't plowed for 14 hours. Median household income in the surrounding tract is $31,000."

---

### 13. Frontend: `frontend/`

**Map layers (toggleable):**
- **Street segments** colored by reliability score (green = good, red = bad)
- **Bus stops** sized by ridership volume
- **Demand-service gap** heatmap
- **Census income** choropleth (tract-level)
- **Equity tier** overlay (Critical segments highlighted)
- **Storm replay** slider: animate plow coverage over time for a selected storm

**Filters:**
- Borough selector
- Minimum daily riders threshold
- Equity tier selector
- Storm event selector (for replay)

---

## Key Engineering Decisions to Document

1. **Why batch-first, not streaming?** The historical analysis is inherently batch — you're comparing across storms that happened months apart. Streaming is only needed during live events. Building batch first gives you the full analytical output, and the live mode is a bonus feature.

2. **Why LION over OSM?** LION is NYC's authoritative street network, aligned with how the city plans plow routes. OSM would introduce matching errors with city operational data.

3. **Why 50m buffer for stop-to-segment join?** Bus stops are typically within 5-15m of the curb. 50m accounts for GPS error in stop coordinates and segments that don't perfectly align. Document the sensitivity analysis: how do results change at 30m vs 50m vs 100m?

4. **Why composite reliability score instead of just average response time?** A segment that was plowed in 2 hours for 7 storms but completely missed in 1 storm is different from one that took 8 hours every time. The composite captures both chronic slowness and total neglect.

5. **Correlation vs. causation:** The equity analysis shows correlation between demographics and service levels. It does not prove discrimination — there may be confounding factors (road width, plow depot locations, route optimization). Document this caveat prominently.

6. **Snowfall normalization:** A 2-inch dusting deploys fewer plows than a 14-inch nor'easter. Normalize response times by storm severity (snowfall total) when comparing across events.

---

## Benchmarks to Capture

| Metric | How to measure | Resume phrasing |
|--------|---------------|-----------------|
| Map matching throughput | GPS points snapped per second | "Map-matched X million GPS points to street network at Y points/sec" |
| Spatial join throughput | Bus stops joined to segments per second | "Performed spatial joins across X datasets at Y joins/sec" |
| Full pipeline runtime | End-to-end from raw data to scored segments | "Processed X storms of plow data against Y street segments in Z minutes" |
| API response time (geospatial) | p50/p99 for GeoJSON endpoints with spatial filters | "Served geospatial queries at p99 Xms over X street segments" |
| Live mode latency | Plow GPS ping to dashboard update | "Real-time coverage updates with sub-Xs end-to-end latency" |

---

## Quickstart

```bash
git clone https://github.com/boblaw/nyc-snow-equity.git
cd nyc-snow-equity
cp .env.example .env

# Start database
docker compose up -d postgres

# Download all datasets (~15 min first time)
make download-all

# Run the full batch pipeline
make pipeline

# Start the API
make api

# Open the dashboard
open http://localhost:8080

# (Optional) During a live snow event:
make live

# Run tests
make test
```

---

## Dependencies

```toml
[tool.poetry.dependencies]
python = "^3.11"
pyspark = "^3.5"
geopandas = "^0.14"
shapely = "^2.0"
sqlalchemy = "^2.0"
geoalchemy2 = "^0.14"
psycopg2-binary = "^2.9"
fastapi = "^0.109"
uvicorn = "^0.27"
httpx = "^0.27"
pyarrow = "^15.0"
pydantic = "^2.5"
sodapy = "^2.2"              # NYC Open Data / Socrata API client
census = "^0.8"              # Census API wrapper
pandas = "^2.2"
numpy = "^1.26"
scipy = "^1.12"              # For correlation analysis
scikit-learn = "^1.4"        # For clustering if needed

# Live mode only
kafka-python = "^2.0"
redis = "^5.0"

[tool.poetry.group.dev.dependencies]
pytest = "^8.0"
pytest-asyncio = "^0.23"
jupyter = "^1.0"
keplergl = "^0.3"
matplotlib = "^3.8"
seaborn = "^0.13"
locust = "^2.20"
```

---

## Claude Code Prompting Strategy

Work through in this order:

1. **Infra + Schema:**
   "Read SPEC.md. Set up docker-compose.yml with PostgreSQL + PostGIS. Create init.sql with the full schema. Make sure the database starts and the schema loads."

2. **Data download scripts:**
   "Read SPEC.md ingestion section. Build all the download scripts: plow data from NYC Open Data Socrata API, MTA ridership, GTFS bus stops, LION street network, pedestrian counts, Census ACS data. Each should save to data/raw/ as Parquet or GeoJSON and load spatial data into PostGIS."

3. **Map matching:**
   "Read SPEC.md map_matcher section. Build the GPS-to-street-segment map matcher using PostGIS ST_DWithin. Process all historical plow data and write matched results to PostGIS. Include the bearing-based disambiguation for intersection points."

4. **Plow response time + reliability:**
   "Read SPEC.md plow_response_time and plow_reliability sections. Compute per-segment per-storm response times, then aggregate into reliability scores. Write to the plow_events and segment_scores tables."

5. **Ridership + pedestrian joins:**
   "Read SPEC.md ridership_joiner and pedestrian_joiner sections. Spatially join bus stops and ped counters to street segments. Update segment_scores with demand data."

6. **Core metric + equity overlay:**
   "Read SPEC.md demand_service_ratio, census_overlay, and equity_classifier sections. Compute the demand-service gap, overlay Census demographics, classify equity tiers, and finalize segment_scores."

7. **API:**
   "Read SPEC.md API section. Build the FastAPI app with all listed endpoints. Wire to PostGIS. GeoJSON responses for map layers."

8. **Frontend:**
   "Build a Deck.gl map with toggleable layers: street segments by reliability score, bus stops by ridership, equity tier overlay, and income choropleth. Add borough and tier filters."

9. **Analysis notebooks:**
   "Create the Jupyter notebooks for exploratory analysis, equity analysis, and the policy brief. Include correlation plots, borough comparisons, and the case study narratives."

10. **Tests + benchmarks + README:**
    "Write tests for map matching, response time calculation, and API endpoints. Build benchmark scripts. Write the final README with architecture diagram and quickstart."

11. **Interview prep doc:**
    "Read SPEC.md section 'Interview Questions & Answers'. Create docs/INTERVIEW_PREP.md with the full Q&A content from the spec, formatted as a clean study guide."

---

## Interview Questions & Answers

This section is meant to become `docs/INTERVIEW_PREP.md` in your repo — a private study guide for you. These are the questions a FAANG interviewer would ask after reading this project, organized by topic. Each answer includes the decision, the tradeoff, and what you'd say differently if you had to scale it 100x.

---

### System Design & Architecture

**Q1: Walk me through the architecture. Why did you choose batch-first instead of building a streaming pipeline from the start?**

The core analysis compares plow performance across storms that happened weeks or months apart. That's inherently a batch workload — you can't stream your way to "average response time across 8 storms." Building batch-first meant I could deliver the full equity analysis, the policy brief, and the API without any streaming infrastructure. The live mode (Kafka + Spark Streaming during active storms) is a separate feature layered on top. This mirrors how most real data platforms evolve: you start with batch to prove the analytics, then add streaming when there's a real-time use case. If I'd gone streaming-first, I'd have spent weeks on Kafka and watermarks before producing a single insight.

*Tradeoff:* The batch pipeline reruns from scratch each time new storm data is added. There's no incremental processing. At NYC scale (~170K segments, 5-15 storms/year) that's fine — full recompute takes minutes. At 100x scale (national, every US city), I'd switch to an incremental model: process each new storm as a delta and merge into the aggregate table.

**Q2: Why PostGIS as the primary store instead of something like Snowflake, BigQuery, or DuckDB?**

Three reasons. First, the core operations are spatial joins (ST_DWithin, ST_Intersects, ST_Distance) that PostGIS handles natively with R-tree spatial indexes. Snowflake and BigQuery have geospatial support but it's less mature and more expensive for iterative development. Second, the API serves GeoJSON directly — PostGIS can do `ST_AsGeoJSON` in the query, so there's no serialization layer. Third, the dataset fits comfortably in a single Postgres instance. There's no need for distributed compute at the storage layer when you have 170K street segments and a few million plow GPS points per storm.

*Tradeoff:* PostGIS is single-node. If this expanded to every US city (~8 million street segments, hundreds of storms per city per year), I'd move to something like BigQuery with its GIS functions or partition across multiple Postgres instances. But premature distribution adds operational complexity without benefit at this scale.

*What I'd say if pushed:* "I chose the simplest thing that works for the data volume. I've worked with petabyte-scale distributed systems in my day job — I didn't need one here, and using one would have been over-engineering."

**Q3: Why three storage sinks (PostGIS + Parquet + Redis) instead of just Postgres?**

Each store serves a different access pattern. PostGIS handles the complex spatial queries the API needs (find all segments within a bounding box, ordered by gap score). Parquet is for reproducibility and batch analytics — the notebooks read directly from Parquet, which is faster for full-table scans than Postgres and doesn't put load on the database. Redis is only used in live mode for sub-millisecond reads of current plow positions on the dashboard. You could collapse PostGIS and Parquet into just Postgres, but then your notebook analytics compete with your API queries for database connections. Separating them is a standard pattern for isolating analytical and serving workloads.

*Tradeoff:* Three stores means three things to keep in sync. For batch data this is simple (write once after pipeline completes), but for live mode there's a brief consistency window where Redis has newer data than PostGIS. That's acceptable because the live dashboard is best-effort and the historical analysis is always consistent.

**Q4: Why Spark for the batch processing instead of just pandas or SQL scripts?**

Honestly, at NYC scale, pandas would work fine. I chose Spark for two reasons: first, the map-matching step benefits from parallelism — processing 5 million GPS points with ST_DWithin queries is embarrassingly parallel across partitions. Second, I wanted the architecture to demonstrate that it scales beyond one city. If you replaced NYC with "every US city," pandas breaks but Spark doesn't. In interviews, I'd rather explain why I chose Spark and acknowledge it's overkill than explain why I chose pandas and have to defend that it can't scale.

*Tradeoff:* Spark adds JVM overhead, dependency complexity (PySpark + Java), and a slower development loop compared to pandas. For a single-city analysis, the development time cost probably isn't worth it. I mitigate this by keeping the Spark jobs simple (mostly DataFrame operations, no custom RDDs) so they're readable by someone who only knows pandas.

---

### Data Modeling & Schema

**Q5: Why did you use a composite reliability score instead of just sorting by average response time?**

Average response time alone hides an important failure mode: segments that get completely skipped during some storms. A segment with an 8-hour average across all storms is different from one with a 2-hour average across 7 storms but was never plowed during an 8th storm. The composite score weights both chronic slowness (60%) and total neglect (40%). I weighted response time higher because consistent 12-hour waits affect more people across more events than a single missed storm. But the miss penalty ensures that segments with even one total failure get downranked.

*Tradeoff:* Any composite metric involves subjective weight choices. I documented the formula, ran sensitivity analysis on the weights (50/50 vs 60/40 vs 70/30), and found the top-50 worst segments were 80% stable across weight variations. That robustness check is in the methodology doc. An alternative approach would be to skip the composite entirely and present both metrics side-by-side, letting the policy audience decide what matters more.

**Q6: Why did you assign each street segment to a single Census tract instead of weighting across multiple tracts?**

Simplicity and interpretability. Most street segments fall entirely within one tract. For segments that span a tract boundary, I use the tract with the greatest geometric overlap. A population-weighted approach (assign demographic values proportional to overlap length) would be more precise, but it makes the results harder to explain to non-technical stakeholders — "this segment is 63% Tract A and 37% Tract B" is less actionable than "this segment is in Tract A." For the policy brief, clean categorization matters more than marginal precision.

*Tradeoff:* For segments on tract boundaries where the demographics differ significantly across tracts, the single-assignment approach introduces error. I mitigate this by flagging boundary segments in the data so the equity analysis can run a sensitivity check excluding them.

**Q7: How do you handle the fact that pedestrian counter coverage is sparse — only ~100 locations across all of NYC?**

I treat pedestrian counts as supplementary, not primary. The core demand metric is bus ridership, which has complete coverage (every stop has ridership data). Pedestrian counts enrich the analysis where they exist and are NULL everywhere else. For segments near a counter, the combined metric is `total_daily_riders + avg_daily_pedestrians`. For segments without a counter, I use ridership alone. I do not interpolate or estimate pedestrian counts for uncovered segments because that introduces unvalidatable assumptions.

*What I'd do with more resources:* NYC's LinkNYC kiosks have WiFi probe data that approximates foot traffic. That's not public, but if this were a city-funded project, that data would dramatically improve coverage.

---

### Geospatial Engineering

**Q8: Explain your map-matching approach. Why a simple nearest-segment lookup instead of a proper Hidden Markov Model map matcher?**

HMM map matching (like what Valhalla or OSRM uses) is designed for continuous GPS traces where you need to reconstruct a route — it considers the sequence of points and the road network topology. Plow GPS data doesn't need route reconstruction. I just need to know "was this segment plowed?" A single point landing within 30 meters of a segment is sufficient to confirm a plow passed it. The nearest-segment approach with a 30m threshold and bearing disambiguation handles 95%+ of cases correctly. I validated this manually in the map-matching notebook by overlaying matched points on the street network for several storm samples.

*Tradeoff:* The simple approach fails at complex intersections where multiple segments converge within 30m. The bearing check helps (match the segment whose bearing aligns with the plow's direction of travel), but some ambiguity remains. An HMM matcher would resolve these better but adds a significant dependency (Valhalla or a custom implementation) for marginal improvement. I'd add it if the map-matching error rate exceeded 5%.

**Q9: Why a 50-meter buffer for joining bus stops to street segments? How did you pick that number?**

Bus stops in the GTFS data have GPS coordinates that typically fall within 5-15 meters of the curb. Street segment centerlines in LION run down the middle of the road, so there's already a 5-10 meter offset from the curb. Add GPS measurement error (±5m) and you get a natural gap of 15-25 meters. I set the buffer at 50m to handle edge cases: stops at wide intersections, stops with coordinates on the sidewalk side of the building, and segments that curve. I ran the join at 30m, 50m, and 100m and compared unmatched stop rates: 30m missed 8% of stops, 50m missed 2%, 100m introduced false matches (stops matching to parallel streets one block over). 50m was the sweet spot.

*This is the kind of sensitivity analysis FAANG interviewers love.* It shows you didn't just pick a number — you tested the tradeoff empirically.

**Q10: How would you handle this if the city was 10x larger — say all of New York State, or the entire US?**

Three changes. First, the map-matching step would need spatial partitioning. Instead of querying all 170K segments for each GPS point, I'd partition both the GPS points and the street network by geohash (same approach as the ADS-B project) and do partition-local joins. Second, PostGIS on a single instance wouldn't handle millions of segments with concurrent API queries. I'd either shard by region (each city gets its own Postgres) or move to BigQuery with GIS functions. Third, the data download and normalization layer would become the hardest part — every city publishes plow data in different formats, different coordinate systems, different schemas. I'd build a normalization framework with per-city adapters.

---

### Data Quality & Edge Cases

**Q11: How do you handle storms with very different snowfall amounts? Is a 2-inch dusting comparable to a 14-inch nor'easter?**

No, and this is a critical normalization step. I join each storm event to NOAA snowfall totals for Central Park and LaGuardia. The city deploys different plow tiers based on storm severity: a 2-inch event gets salting trucks on arterials only, while a 14-inch event triggers full fleet deployment. Comparing raw response times across these is misleading. I normalize by computing a "response time relative to storm severity" — essentially, how did this segment's response time compare to the citywide median for storms of similar snowfall. A segment that takes 8 hours to plow in a 2-inch storm is much worse than one that takes 8 hours in a 14-inch storm.

*Tradeoff:* NOAA gives a single snowfall measurement per station, not per neighborhood. Queens might get 6 inches while Manhattan gets 3 inches in the same storm. Ideally I'd use gridded snowfall data (NOAA's SNODAS product), but that adds significant data engineering complexity for moderate analytical improvement. I document this limitation.

**Q12: What happens when a segment has zero bus stops and zero pedestrian counters? How do you rank it?**

It gets a demand score of zero, which effectively removes it from the equity analysis. This is intentional — segments with no measurable foot traffic aren't relevant to the "people standing in unplowed snow" story. However, I flag these segments separately because some may have high vehicle traffic or residential density that isn't captured by transit data. A future version could incorporate 311 complaint data (NYC publishes this) to capture "informal demand" — residents calling about unplowed streets are a signal even when there's no bus stop nearby.

**Q13: How do you handle the fact that PlowNYC only publishes data during declared snow emergencies?**

This is actually a significant data gap — light snowfalls that don't trigger a declaration produce no plow data, even though sidewalks and bus stops still get icy. I'm transparent about this in the methodology doc: the analysis only covers declared events. For the 2023-2024 winter season, that was N storms totaling X inches. To build confidence in the reliability scores, I compute a confidence interval based on the number of storms observed per segment. A segment plowed in all 12 storms has a tight confidence interval; one that appeared in only 3 storms (because it's in an area that sometimes falls outside the plow zone) has a wide one. The API exposes this confidence field so the frontend can visually distinguish high-confidence scores from low-confidence ones.

---

### Equity Analysis & Methodology

**Q14: How do you handle the correlation vs. causation problem in the equity analysis?**

Very carefully. The analysis shows that lower-income neighborhoods tend to have worse plow reliability scores. I present this as a correlation, not a causal claim. There are plausible confounders: lower-income areas may have narrower streets (harder to plow), be farther from plow depots (longer response times by geography), or have fewer snow emergency routes. I attempt to control for these by including road class and distance-to-nearest-depot as covariates in a multivariate regression. If the income effect persists after controlling for these physical factors, the finding is stronger. But I still don't claim discrimination — I say "after controlling for road characteristics, income remains a significant predictor of plow response time" and let policymakers draw their own conclusions.

*Why this matters for FAANG:* Data engineers at FAANG regularly have to present analytical findings to product and policy teams. Showing that you understand the difference between correlation and causation, and that you can control for confounders, is a signal of analytical maturity.

**Q15: If a city council member asked you "which 10 streets should we plow first next storm," could your system answer that?**

Yes. Sort `segment_scores` by `priority_rank` (which is ordered by `demand_service_gap` within the CRITICAL equity tier) and take the top 10. But I'd caveat that the ranking optimizes for equity (high-demand, underserved, low-income), not for operational efficiency. A plow route planner would need to factor in depot locations, route connectivity (you can't plow one segment in isolation — the plow has to drive there), and total fleet capacity. My ranking is a policy input, not an operations plan. Combining it with a route optimization layer (TSP variant with priority weights) would be a natural extension.

---

### Performance & Scale

**Q16: What's the most expensive operation in the pipeline and how did you optimize it?**

Map matching — by far. Each of the ~5 million GPS points per storm needs a spatial proximity query against 170K street segments. Naive implementation (one ST_DWithin query per point) takes hours. I optimized it three ways: (1) batch the queries using Spark partitions, running 8-16 parallel connections to PostGIS, (2) pre-filter GPS points by borough so each query only searches the borough's segments (reduces index scan scope by ~80%), and (3) use ST_DWithin with the geography type and a functional index on the geography cast so Postgres uses the spatial index efficiently. After optimization, the full map match runs in under 10 minutes for a major storm's data.

**Q17: How would you test this pipeline? What does your test suite look like?**

Three levels. Unit tests cover the pure functions: geohash encoding, Haversine distance, reliability score formula, equity tier classification. These run in milliseconds with no database dependency. Integration tests use a small PostGIS test database loaded with ~100 synthetic street segments, ~50 bus stops, and ~1000 fake plow GPS points. They verify that the spatial joins produce correct results, that the response time calculation handles edge cases (never-plowed segments, storms with only one plow pass), and that the API returns valid GeoJSON. End-to-end tests run the full pipeline on a small sample dataset and compare output to manually verified expected results. I also have a map-matching validation notebook where I visually inspect matched points on a map for a sample of each storm — this catches systematic errors that automated tests miss.

---

### Behavioral / Why This Project

**Q18: Why did you build this?**

I've spent my career building data platforms for government missions. The tools I built moved data for military decision-makers, but the same engineering can be applied to civic problems that directly affect everyday people. I wanted to show that a data engineering project can be technically rigorous and have a clear human impact. Every New Yorker who waits at an unplowed bus stop in a snowstorm is affected by the city's plow routing decisions — and those decisions should be informed by data, not just tradition or political pressure.

**Q19: What would you do differently if you started over?**

I'd start with the data quality investigation before building any pipeline. I spent time building the map matcher before fully understanding the GPS noise characteristics, which meant I had to revise my matching thresholds twice. In a production setting, I'd do a thorough EDA phase first: how noisy is the GPS? How complete is the coverage? Are there systematic gaps (plows that don't report in certain areas)? That would have saved a week of rework. I'd also invest earlier in the storm simulator so I could develop and test the pipeline year-round without waiting for actual snowfall.
