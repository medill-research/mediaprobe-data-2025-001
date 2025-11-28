-- CREATE STAGING TABLE FOR TIMELINE METADATA
CREATE TABLE IF NOT EXISTS staging.timeline_metadata (
    time_id INTEGER,
    session_id INTEGER,
    time_stamp TIMESTAMP,
    phasic_program FLOAT,
    ars_program FLOAT
);