{{
    config(
        materialized='external',
        location="../data/export/modeling_data.csv"
    )
}}

{% set source_ads = source('galvanic_staging', 'liwc_metadata') %}

WITH LIWC_METADATA AS (
    SELECT * FROM {{ source_ads }}
),

ADS_DATA AS (
    SELECT * FROM {{ ref('ads_us_full') }}
),

TRANSCRIPTS_DATA AS (

    SELECT
        split_part(ID, '_', 1)::INTEGER AS session_id,
        (split_part(ID, '_', 2) || ':' || split_part(ID, '_', 3) || ':' || split_part(ID, '_', 4))::TIME AS time_in,
        (split_part(ID, '_', 5) || ':' || split_part(ID, '_', 6) || ':' || replace(split_part(ID, '_', 7), '.wav', ''))::TIME AS time_out,
        TRIM(Transcript) AS transcript,
        format,
        genre,
        subgenre,
        program,
        description

    FROM LIWC_METADATA

)

SELECT
    AD.*,
    TD.transcript,
    TD.format,
    TD.genre,
    TD.subgenre,
    TD.program,
    TD.description

FROM ADS_DATA AD
LEFT JOIN TRANSCRIPTS_DATA TD ON AD.session_id = TD.session_id AND AD.session_in = TD.time_in AND AD.session_out = TD.time_out