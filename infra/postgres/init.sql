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
    id              BIGSERIAL,
    segment_id      VARCHAR(10) NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    latitude        FLOAT,
    longitude       FLOAT,
    speed_mph       FLOAT,
    storm_date      DATE NOT NULL,
    geom            GEOMETRY(Point, 4326) NOT NULL,
    PRIMARY KEY (id, storm_date)
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
