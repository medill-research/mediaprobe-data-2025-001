-- CREATE STAGING TABLE FOR SECOND ITERATION OF ADS METADATA
CREATE TABLE IF NOT EXISTS staging.ads_metadata_11272025 (
    session_id INTEGER,
    description TEXT,
    time_in TIME,
    time_out TIME,
    phasic_ads FLOAT,
    ars_ads FLOAT,
    ads_comparison TEXT
);