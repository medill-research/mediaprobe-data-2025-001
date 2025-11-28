{{
    config(
        materialized="view",
        schema="feature"
    )
}}

-- DETERMINE THE ADS SEGMENT AND RUN SESSION FOR ROUND 1 PROGRAM
WITH BASE_TABLE AS (

    SELECT
        ROW_NUMBER() OVER () AS row_id,
        *,
        CASE
            WHEN ads_indicator != LAG(ads_indicator) OVER (PARTITION BY session_id ORDER BY time_sequence) THEN 1
            ELSE 0
        END AS change

    FROM {{ ref('program_metadata') }}

),

CHANGE_DETECTION AS (

    SELECT
        *,
        SUM(change) OVER (PARTITION BY session_id ORDER BY time_sequence) AS run_id

    FROM BASE_TABLE

),

SEGMENT_DETECTION AS (

    SELECT
        * EXCLUDE (change, run_id),
        DENSE_RANK() OVER (PARTITION BY session_id, ads_indicator ORDER BY run_id) AS segment_id

    FROM CHANGE_DETECTION

),

-- DETERMINE THE POISTION OF ADS IN EACH AD POD
AD_CHANGE AS (

    SELECT
        *,
        CASE
            WHEN ads_description != LAG(ads_description) OVER (PARTITION BY session_id, segment_id ORDER BY time_sequence) THEN 1
            ELSE 0
        END AS ad_change

    FROM SEGMENT_DETECTION

    WHERE
        ads_indicator = 1

),

CHANGE_DETECTION_AD AS (

    SELECT
        *,
        SUM(ad_change) OVER (PARTITION BY session_id, segment_id ORDER BY time_sequence) AS ads_run_id

    FROM AD_CHANGE

),

POSITION_DETECTION AS (

    SELECT
        * EXCLUDE (ad_change, ads_run_id),
        DENSE_RANK() OVER (PARTITION BY session_id, segment_id ORDER BY ads_run_id) AS position_id

    FROM CHANGE_DETECTION_AD

)

SELECT
    ROW_NUMBER() OVER (ORDER BY SD.session_id, SD.time_sequence) AS row_id,
    SD.* EXCLUDE (SD.row_id, SD.segment_id),
    SD.segment_id AS segment_number,
    PD.position_id AS ad_position

FROM SEGMENT_DETECTION SD
LEFT JOIN POSITION_DETECTION PD ON SD.row_id = PD.row_id

ORDER BY
    SD.session_id,
    SD.time_sequence