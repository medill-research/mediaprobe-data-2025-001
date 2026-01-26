{{
    config(
        materialized="table",
        schema="feature"
    )
}}

{% set source_ads = source('galvanic_staging', 'ads_metadata_11272025') %}

WITH ADS_BASE AS (

    SELECT
        session_id,
        TRIM(description) AS description,
        TRIM(ads_comparison) AS ads_comparison,
        time_in,
        time_out,
        ifnull(phasic_ads, 0) AS phasic_ads,
        ifnull(ars_ads, 0) AS ars_ads

    FROM {{ source_ads }}

    WHERE
        TRIM(description) != '' AND
        time_in <= time_out

),

PERCENTILE_DATA AS (

    SELECT
        *,
        quantile_cont(phasic_ads::DOUBLE, 0.01) WITHIN GROUP (ORDER BY phasic_ads) OVER () AS phasic_ads_lower,
        quantile_cont(phasic_ads::DOUBLE, 0.99) WITHIN GROUP (ORDER BY phasic_ads) OVER () AS phasic_ads_upper,
        quantile_cont(ars_ads::DOUBLE, 0.01) WITHIN GROUP (ORDER BY ars_ads) OVER () AS ars_ads_lower,
        quantile_cont(ars_ads::DOUBLE, 0.99) WITHIN GROUP (ORDER BY ars_ads) OVER () AS ars_ads_upper,
        LAG(time_in) OVER (PARTITION BY session_id ORDER BY time_in) AS lag_time_in,
        LAG(time_out) OVER (PARTITION BY session_id ORDER BY time_out) AS lag_time_out

    FROM ADS_BASE

),

WINSORIZED_DATA AS (

    SELECT
        session_id,
        time_in As session_in,
        time_out As session_out,
        description,
        ads_comparison,
        CASE
            WHEN lag_time_out IS NULL THEN time_in
            WHEN time_in = lag_time_out THEN time_in + INTERVAL '1 second'
            ELSE time_in
        END AS time_in,
        time_out,
        phasic_ads,
        CASE
            WHEN ars_ads > ars_ads_upper THEN ars_ads_upper
            WHEN ars_ads < ars_ads_lower THEN ars_ads_lower
            ELSE ars_ads
        END AS ars_ads

    FROM PERCENTILE_DATA

),

SECONDS_DIFF AS (

    SELECT
        *,
        EXTRACT(epoch FROM time_out)::INTEGER - EXTRACT(epoch FROM time_in)::INTEGER AS seconds_diff
    FROM WINSORIZED_DATA

),

UNNEST_DIFF AS (

    SELECT
        *,
        unnest(generate_series(0, seconds_diff)) AS second_offset
    FROM SECONDS_DIFF

)

SELECT
    session_id,
    session_in,
    session_out,
    time_in + INTERVAL (second_offset) SECOND AS time_sequence,
    description,
    ads_comparison,
    time_in,
    time_out,
    phasic_ads,
    ars_ads

FROM UNNEST_DIFF

ORDER BY
    session_id,
    time_sequence