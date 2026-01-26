{{
    config(
        materialized="table",
        schema="curated"
    )
}}

WITH MODELING_DATA AS (
    SELECT * FROM {{ ref('vw_liwc_modeling') }}
),

BASE_TABLE AS (

    SELECT
        session_id,
        session_in,
        session_out,
        phasic_ads,
        ars_ads,
        ads_comparison,
        segment_number,
        ad_position,
        program_country,
        program_format,
        age_average,
        females_pct,
        * EXCLUDE (
            session_id, session_in, session_out, phasic_ads, ars_ads, ads_time_in, ads_time_out, time_sequence,
            segment_number, ad_position, program_country, channel_name, program_format, program_genre, program_subgenre,
            program_name, age_average, age18_age34, age35_age64, age65, females_pct, males_pct, non_binary_pct,
            ads_description, ads_comparison
        )

    FROM MODELING_DATA

),

NULL_COUNT AS (

    SELECT
        session_id,
        session_in,
        session_out,
        COUNT(*) FILTER (WHERE col_value IS NULL) AS null_count

    FROM (
        FROM BASE_TABLE
        UNPIVOT INCLUDE NULLS (
            col_value
            FOR col_name
            IN (COLUMNS(* EXCLUDE (
                session_id, session_in, session_out,
                phasic_ads, ars_ads, ads_comparison, segment_number, ad_position, program_country,
                program_format, age_average, females_pct
            )))
        )
    )

    GROUP BY
        session_id,
        session_in,
        session_out

),

FILTERED_NULLS AS (

    SELECT
        BT.*

    FROM BASE_TABLE BT
    JOIN NULL_COUNT NC ON BT.session_id = NC.session_id AND BT.session_in = NC.session_in AND BT.session_out = NC.session_out

    WHERE
        NC.null_count <= 10

)

SELECT
    session_id,
    session_in,
    session_out,
    phasic_ads,
    ars_ads,
    ads_comparison,
    segment_number,
    ad_position,
    program_country,
    program_format,
    age_average,
    females_pct,
    * EXCLUDE (
        session_id, session_in, session_out,
        phasic_ads, ars_ads, ads_comparison, segment_number, ad_position,
        program_country, program_format, age_average, females_pct
    )

FROM FILTERED_NULLS