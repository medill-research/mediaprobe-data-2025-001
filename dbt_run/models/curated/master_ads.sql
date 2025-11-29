{{
    config(
        materialized="table",
        schema="curated"
    )
}}

{% set source_session = source('galvanic_staging', 'session_metadata') %}

-- CREATE A VIEW FOR HOLDING ADS INFORMATION ONLY FOR FEATURE ENGINEERING
WITH ADS_BASE AS (

    SELECT DISTINCT
        session_id,
        time_sequence,
        segment_number,
        ROW_NUMBER() OVER (PARTITION BY session_id, segment_number ORDER BY ad_position) AS ad_position,
        ads_description,
        phasic_program,
        ars_program,
        phasic_ads,
        ars_ads

    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, segment_number, ads_time_in, ads_time_out ORDER BY time_sequence) AS row_rank
        FROM {{ ref('vw_program_ranked') }}
        WHERE ads_indicator = 1
    )

    WHERE
        row_rank = 1

),

ADS_FEATURES AS (

    SELECT
        session_id,
        segment_number,
        ad_position,
        LAG(phasic_ads, 1, 0) OVER (PARTITION BY session_id, segment_number ORDER BY ad_position) AS prev_phasic_ads,
        LAG(ars_ads, 1, 0) OVER (PARTITION BY session_id, segment_number ORDER BY ad_position) AS prev_ars_ads,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY time_sequence) AS rolling_ads_count,

        -- CHECK IF THE SAME AD HAS BEEN REPEATED IN THE SAME SEGMENT
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY session_id, segment_number, ads_description
                ORDER BY ad_position
            ) > 1 THEN 1
            ELSE 0
        END AS repeated_ad

    FROM ADS_BASE

),

-- CREATE FEATURES THAT ARE COMING THE PROGRAMS
PROGRAM_FEATURES AS (

    SELECT
        session_id,
        segment_number,
        time_sequence,
        phasic_program,
        ars_program,
        ROW_NUMBER() OVER (PARTITION BY session_id, segment_number ORDER BY time_sequence DESC) AS row_ranking

    FROM {{ ref('vw_program_ranked') }}

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

)

SELECT
    AB.session_id,
    AB.segment_number AS pod_number,
    AB.ad_position,
    AB.ads_description,
    SS.country AS program_country,
    SS.channel AS channel_name,
    SS.format AS program_format,
    SS.genre AS program_genre,
    SS.subgenre AS program_subgenre,
    SS.program AS program_name,
    SS.age_average,
    SS.age18_age34,
    SS.age35_age64,
    SS.age65,
    SS.females_pct,
    SS.males_pct,
    SS.non_binary_pct,
    PM.prev_phasic_program,
    PM.prev_ars_program,
    AF.prev_phasic_ads,
    AF.prev_ars_ads,
    AF.rolling_ads_count,
    AF.repeated_ad,
    AB.phasic_ads,
    AB.ars_ads

FROM ADS_BASE AB
LEFT JOIN ADS_FEATURES AF ON AB.session_id = AF.session_id AND AB.segment_number = AF.segment_number AND AB.ad_position = AF.ad_position
LEFT JOIN PROGRAM_MEAN PM ON AB.session_id = PM.session_id AND AB.segment_number = PM.segment_number
LEFT JOIN {{ source_session }} SS ON AB.session_id = SS.session_id

ORDER BY
    AB.session_id,
    AB.segment_number,
    AB.ad_position