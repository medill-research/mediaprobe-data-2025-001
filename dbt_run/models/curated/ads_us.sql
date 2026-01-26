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

        -- LIWC DIMENSION: SUMMARY LANGUAGE VARIABLES
        Analytic_EN,
        Clout_EN,
        Authentic_EN,
        Tone_EN,
        WPS_EN,
        BigWords_EN,
        Dic_EN,

        -- LIWC DIMENSION: UNKNOWN
        exclam,
        otherp,
        apostro,
        anger_pct,
        happiness_pct,
        neutrality_pct,
        sadness_pct,
        avg_activation,
        avg_dominance,
        avg_valence,
        avg_fundamental_freq,
        avg_loudness,
        avg_speaking_rate,
        avg_speaking_rate_variation,
        avg_intonation_score,

        -- LIWC DIMENSION: PUNCTUATION
        period,
        q_mark,
        comma,

        -- LIWC DIMENSION: OTHER GRAMMAR
        number,
        verb,

        -- LIWC DIMENSION: WORD COUNT
        wc,

        -- LIWC DIMENSION: LINGUISTIC DIMENSIONS
        ppron,
        ipron,
        conj,
        auxverb,
        adverb,
        negate,
        article,
        det_EN,
        prep_EN,
        adj_EN,
        quantity_EN,

        -- LIWC DIMENSION: PSYCHOLOGICAL PROCESSES
        family,
        friend,
        insight,
        cause,
        tentat,
        health,
        achieve,
        discrep,

        -- LIWC DIMENSION: DRIVES PROCESSES
        affiliation_EN,
        power_EN,

        -- LIWC DIMENSION: COGNITION PROCESSES
        allnone_EN,
        certitude_EN,
        differ_EN,
        memory_EN,

        -- LIWC DIMENSION: AFFECT PROCESSES
        tone_pos_EN,
        tone_neg_EN,
        emo_pos_EN,
        emo_neg_EN,
        swear,

        -- LIWC DIMENSION: SOCIAL PROCESSES
        prosocial_EN,
        polite_EN,
        conflict_EN,
        moral_EN,
        comm_EN,
        female_EN,
        male_EN,

        -- LIWC DIMENSION: CULTURE PROCESSES
        politic_EN,
        ethnicity_EN,
        tech_EN,

        -- LIWC DIMENSION: LIFESTYLE PROCESSES
        leisure,
        home,
        "work",
        money,
        relig,

        -- LIWC DIMENSION: PHISICAL PROCESSES
        illness_EN,
        wellness_EN,
        mental_EN,
        substances_EN,
        food_EN,
        sexual,
        death,
        need_EN,
        want_EN,
        acquire_EN,
        lack_EN,
        fulfill_EN,
        fatigue_EN,
        reward_EN,
        risk_EN,
        curiosity_EN,
        allure_EN,

        -- LIWC DIMENSION: PERCEPTION PROCESSES
        attention_EN,
        motion,
        "space",
        visual_EN,
        auditory_EN,
        feeling_EN,

        -- LIWC DIMENSION: TIME PROCESSES
        time,
        focuspast_EN,
        focuspresent_EN,
        focusfuture_EN,

        -- LIWC DIMENSION: CONVERSATION PROCESSES
        netspeak_EN,
        nonflu_EN,
        assent,
        filler

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
    COALESCE(
        COLUMNS(* EXCLUDE (
            session_id, session_in, session_out,
            phasic_ads, ars_ads, ads_comparison, segment_number, ad_position,
            program_country, program_format, age_average, females_pct
        )),
        0
    )

FROM FILTERED_NULLS