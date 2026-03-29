-- sql/ddl/create_tables.sql
-- Run once to initialize the data warehouse schema

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw events table (landing zone)
CREATE TABLE IF NOT EXISTS raw.events (
    id          BIGSERIAL PRIMARY KEY,
    user_id     VARCHAR(50),
    event       VARCHAR(100),
    timestamp   TIMESTAMPTZ,
    page        VARCHAR(255),
    amount      NUMERIC(12, 2),
    source      VARCHAR(100),
    _ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Processed daily metrics (written by Spark)
CREATE TABLE IF NOT EXISTS marts.fct_daily_metrics (
    event_date        DATE,
    user_id           VARCHAR(50),
    total_events      INT,
    purchases         INT,
    total_revenue     NUMERIC(14, 2),
    unique_event_types INT,
    PRIMARY KEY (event_date, user_id)
);
