-- CREATE STAGING TABLE FOR FIRST ITERATION OF ADS METADATA
CREATE TABLE IF NOT EXISTS staging.ads_metadata (
    session_id INTEGER,
    description TEXT,
    category TEXT,
    time_in TIME,
    time_out TIME,
    phasic_ads FLOAT,
    ars_ads FLOAT
);