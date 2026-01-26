{{
    config(
        materialized="view",
        schema="feature"
    )
}}

{% set session_data = source('galvanic_staging', 'session_metadata') %}

WITH PROGRAM_DATA AS (
    SELECT * FROM {{ ref('vw_program_ranked_11272025') }}
),

SESSION_DATA AS (
    SELECT * FROM {{ session_data }}
),

LIWC_DATA AS (
    SELECT * FROM {{ ref('liwc_features') }}
),

PROGRAM_RANK AS (

    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, segment_number, ads_time_in, ads_time_out ORDER BY time_sequence) AS row_rank
    FROM PROGRAM_DATA
    WHERE ads_indicator = 1

),

ADS_BASE AS (

    SELECT
        PR.session_id,
        PR.session_in,
        PR.session_out,
        PR.ads_time_in,
        PR.ads_time_out,
        PR.time_sequence,
        PR.segment_number,
        ROW_NUMBER() OVER (PARTITION BY PR.session_id, PR.segment_number ORDER BY PR.ad_position) AS ad_position,
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
        PR.ads_description,
        PR.ads_comparison,
        PR.phasic_ads,
        PR.ars_ads

    FROM PROGRAM_RANK PR
    LEFT JOIN SESSION_DATA SS ON PR.session_id = SS.session_id

    WHERE
        PR.row_rank = 1 AND
        PR.ads_comparison = 'Ads' and
        SS.country = 'US'

)

SELECT
    * EXCLUDE (
        LD.session_id, LD.time_in, LD.time_out, LD.ads_comparison, LD.country, AB.phasic_ads, AB.ars_ads
    ),
    AB.phasic_ads,
    AB.ars_ads

FROM ADS_BASE AB
LEFT JOIN LIWC_DATA LD ON AB.session_id = LD.session_id AND AB.session_in = LD.time_in AND AB.session_out = LD.time_out