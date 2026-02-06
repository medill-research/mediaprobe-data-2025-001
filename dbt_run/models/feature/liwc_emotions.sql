{{
    config(
        materialized="table",
        schema="feature"
    )
}}

{% set source_ads = source('galvanic_staging', 'liwc_metadata') %}
{% set source_emotions = source('galvanic_staging', 'emotion_predictions') %}

WITH LIWC_METADATA AS (
    SELECT * FROM {{ source_ads }}
),

EMOTION_PREDICTIONS AS (
    SELECT * FROM {{ source_emotions }}
),

AS_VARCHAR AS (
    SELECT COLUMNS(*)::VARCHAR FROM LIWC_METADATA
),

CAST_NA AS (
    SELECT NULLIF(COLUMNS(*), 'NA') FROM AS_VARCHAR
),

EMOTION_SCORES AS (

    SELECT
        split_part(LM.ID, '_', 1)::INTEGER AS session_id,
        (split_part(LM.ID, '_', 2) || ':' || split_part(LM.ID, '_', 3) || ':' || split_part(LM.ID, '_', 4))::TIME AS time_in,
        (split_part(LM.ID, '_', 5) || ':' || split_part(LM.ID, '_', 6) || ':' || replace(split_part(LM.ID, '_', 7), '.wav', ''))::TIME AS time_out,
        LM."Duration (ms)" AS ads_duration,
        EP.* EXCLUDE (EP.ID, EP.transcript)

    FROM LIWC_METADATA LM
    JOIN EMOTION_PREDICTIONS EP ON LM.ID = EP.ID

)

SELECT * FROM EMOTION_SCORES