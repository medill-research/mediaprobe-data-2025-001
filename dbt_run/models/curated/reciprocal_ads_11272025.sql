{{
    config(
        materialized="table",
        schema="curated"
    )
}}

-- GET THE MEAN FROM FIRST 30 SECONDS OF PROGRAM
WITH PROGRAM_FEATURES AS (

    SELECT
        session_id,
        segment_number,
        time_sequence,
        phasic_program,
        ars_program,
        ROW_NUMBER() OVER (PARTITION BY session_id, segment_number ORDER BY time_sequence) AS row_ranking

    FROM {{ ref('vw_program_ranked_11272025') }}

    WHERE
        ads_indicator = 0

),

PROGRAM_MEAN AS (

    SELECT
        session_id,
        segment_number,
        AVG(phasic_program) AS prev_phasic_program,
        AVG(ars_program) AS prev_ars_program

    FROM PROGRAM_FEATURES

    WHERE
        row_ranking <= 30

    GROUP BY
        session_id,
        segment_number

),

-- GET THE LAST AD FROM EACH POD
ADS_FEATURES AS (

    SELECT DISTINCT
        session_id,
        segment_number + 1 AS segment_number,
        ads_comparison,
        ad_position,
        phasic_ads,
        ars_ads

    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY session_id, segment_number ORDER BY time_sequence DESC
            ) AS row_rank,
            ROW_NUMBER() OVER (
                PARTITION BY session_id, segment_number ORDER BY ad_position
            ) AS ad_position
        FROM {{ ref('vw_program_ranked_11272025') }}
        WHERE ads_indicator = 1
    )

    WHERE
        row_rank = 1

)

SELECT
    PM.session_id,
    PM.segment_number AS program_seg,
    PM.segment_number - 1 AS pod_number,
    AF.ads_comparison,
    AF.ad_position,
    AF.phasic_ads AS prev_phasic_ads,
    AF.ars_ads AS prev_ars_ads,
    PM.prev_phasic_program AS phasic_program,
    PM.prev_ars_program AS ars_program,

FROM PROGRAM_MEAN PM
LEFT JOIN ADS_FEATURES AF ON PM.session_id = AF.session_id AND PM.segment_number = AF.segment_number

WHERE
    AF.ad_position IS NOT NULL

ORDER BY
    PM.session_id,
    program_seg,
    pod_number