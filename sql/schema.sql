-- ----------------------------------------------------------------------
-- 0. STAGING TABLE (stg_weather_raw)
-- ELT Layer
--
-- Stores raw weather API responses before any transformation occurs.
-- This table serves as the landing zone for extracted data and allows:
--   Auditing of original API responses
--   Reprocessing without calling the API again
--   Separation of loading and transformation logic (ELT pattern)
--   Tracking of processed and unprocessed records
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_weather_raw (
    
    -- Unique identifier for each raw API extraction event
    staging_id SERIAL PRIMARY KEY,

    -- Complete unmodified API response stored as JSON
    raw_payload JSONB NOT NULL,

    -- Timestamp indicating when the API response was loaded
    -- into the staging layer
    extraction_timestamp TIMESTAMP WITH TIME ZONE
        DEFAULT CURRENT_TIMESTAMP,

    -- Indicates whether the staged record has been transformed
    -- and loaded into analytical dimension/fact tables
    processed BOOLEAN DEFAULT FALSE
);



-- ======================================================================
-- OPEN-METEO PROJECT: STAR SCHEMA DESIGN
-- Target Database: PostgreSQL / Relational SQL Database
-- File Structure: Creates Dimension and Fact Tables for Weather Analytics
-- ======================================================================

-- ----------------------------------------------------------------------
-- 1. DATE DIMENSION TABLE (dim_date)
-- Pre-calculates time grains to eliminate slow runtime SQL date functions.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week VARCHAR(20) NOT NULL
);

-- ----------------------------------------------------------------------
-- 2. LOCATION DIMENSION TABLE (dim_location)
-- Stores descriptive geographic attributes independent of weather metrics.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_location (
    location_key SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL UNIQUE,
    latitude NUMERIC(10,6) NOT NULL,
    longitude NUMERIC(10,6) NOT NULL
);


-- ----------------------------------------------------------------------
-- 3. CENTRAL WEATHER FACT TABLE (fact_weather)
-- Stores quantitative observational measurements and maps to lookup keys.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_weather (
    weather_id SERIAL PRIMARY KEY,

    date_key INTEGER NOT NULL,
    location_key INTEGER NOT NULL,

    temperature_2m_max NUMERIC(6,2),
    temperature_2m_min NUMERIC(6,2),
    precipitation_sum NUMERIC(8,2),
    wind_speed_10m_max NUMERIC(8,2),
    temp_range NUMERIC(6,2),

    load_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Enforces referential integrity; blocks metrics for non-existent dates
    CONSTRAINT fk_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    -- Enforces referential integrity; blocks metrics for unverified locations
    CONSTRAINT fk_location
        FOREIGN KEY (location_key)
        REFERENCES dim_location(location_key),

    -- Prevents duplicate weather observations for the same location and date
    CONSTRAINT uq_weather_day_location
        UNIQUE (date_key, location_key)
);