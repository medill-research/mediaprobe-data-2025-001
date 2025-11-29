-- CREATE STAGING TABLE FOR TIMELINE METADATA FROM 11/27/2025
CREATE TABLE IF NOT EXISTS staging.timeline_metadata_11272025 (
    session_id INTEGER,
    time_stamp TIME,
    phasic_program FLOAT,
    ars_program FLOAT
);