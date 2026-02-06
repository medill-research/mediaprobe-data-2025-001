{{
    config(
        materialized="table",
        schema="curated"
    )
}}

WITH ADS_DATA AS (
    SELECT * FROM {{ ref('ads_us_full') }}
),

EMOTION_FEATURES AS (
    SELECT * FROM {{ ref('liwc_emotions') }}
)

SELECT
    AD.session_id,
    AD.session_in,
    AD.session_out,
    AD.phasic_ads,
    AD.ars_ads,
    AD.ads_comparison,
    EF.ads_duration,
    AD.segment_number,
    AD.ad_position,
    AD.program_country,
    AD.program_format,
    AD.age_average,
    AD.females_pct,
    EF.* EXCLUDE (EF.session_id, EF.time_in, EF.time_out, EF.ads_duration)

FROM ADS_DATA AD
JOIN EMOTION_FEATURES EF ON AD.session_id = EF.session_id AND AD.session_in = EF.time_in AND AD.session_out = EF.time_out