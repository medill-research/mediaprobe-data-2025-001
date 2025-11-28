-- CREATE STAGING TABLE FOR SESSION METADATA
CREATE TABLE IF NOT EXISTS staging.session_metadata (
    session_id INTEGER PRIMARY KEY NOT NULL,
    country TEXT,
    channel TEXT,
    format TEXT,
    genre TEXT,
    subgenre TEXT,
    program TEXT,
    age_average NUMERIC(19, 3),
    age18_age34 NUMERIC(19, 3),
    age35_age64 NUMERIC(19, 3),
    age65 NUMERIC(19, 3),
    females_pct NUMERIC(19, 3),
    males_pct NUMERIC(19, 3),
    non_binary_pct NUMERIC(19, 3),
    start_time TIMESTAMP
);