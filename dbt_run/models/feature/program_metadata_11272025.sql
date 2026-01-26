{{
    config(
        materialized="table",
        schema="feature"
    )
}}

{% set source_ads = source('galvanic_staging', 'timeline_metadata_11272025') %}

WITH BASE_TABLE AS (

    SELECT
        session_id,
        time_stamp AS time_sequence,
        phasic_program,
        ars_program,
        quantile_cont(phasic_program::DOUBLE, 0.01) WITHIN GROUP (ORDER BY phasic_program) OVER () AS phasic_program_lower,
        quantile_cont(phasic_program::DOUBLE, 0.99) WITHIN GROUP (ORDER BY phasic_program) OVER () AS phasic_program_upper,
        quantile_cont(ars_program::DOUBLE, 0.01) WITHIN GROUP (ORDER BY ars_program) OVER () AS ars_program_lower,
        quantile_cont(ars_program::DOUBLE, 0.99) WITHIN GROUP (ORDER BY ars_program) OVER () AS ars_program_upper

    FROM {{ source_ads }}

),

WINSORIZED_TABLE AS (

    SELECT
        session_id,
        time_sequence,
        phasic_program,
        CASE
            WHEN ars_program > ars_program_upper THEN ars_program_upper
            WHEN ars_program < ars_program_lower THEN ars_program_lower
            ELSE ars_program
        END AS ars_program

    FROM BASE_TABLE

)

SELECT
    WT.session_id,
    AE.session_in,
    AE.session_out,
    WT.time_sequence,
    WT.phasic_program,
    WT.ars_program,
    AE.description AS ads_description,
    AE.ads_comparison,
    AE.time_in AS ads_time_in,
    AE.time_out AS ads_time_out,
    AE.phasic_ads,
    AE.ars_ads,
    CASE
        WHEN AE.time_in IS NULL THEN 0
        ELSE 1
    END AS ads_indicator

FROM WINSORIZED_TABLE WT
LEFT JOIN {{ ref('ads_expanded_11272025') }} AE ON WT.session_id = AE.session_id AND WT.time_sequence = AE.time_sequence