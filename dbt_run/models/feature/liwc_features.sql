{{
    config(
        materialized="table",
        schema="feature"
    )
}}

{% set source_ads = source('galvanic_staging', 'liwc_metadata') %}

WITH LIWC_METADATA AS (
    SELECT * FROM {{ source_ads }}
),

AS_VARCHAR AS (
    SELECT COLUMNS(*)::VARCHAR FROM LIWC_METADATA
),

CAST_NA AS (
    SELECT NULLIF(COLUMNS(*), 'NA') FROM AS_VARCHAR
),

FEATURE_DATA AS (

    SELECT
        split_part(ID, '_', 1)::INTEGER AS session_id,
        (split_part(ID, '_', 2) || ':' || split_part(ID, '_', 3) || ':' || split_part(ID, '_', 4))::TIME AS time_in,
        (split_part(ID, '_', 5) || ':' || split_part(ID, '_', 6) || ':' || replace(split_part(ID, '_', 7), '.wav', ''))::TIME AS time_out,
        ads_comparison,
        country,
        * EXCLUDE (
            ID, Transcript, "Language", description, "in", "out", phasic, ars, session_id, ads_comparison, country,
            channel, format, genre, subgenre, program, age_average, "18-34", "35-64", "65+", "%females", "%males", "%non-binary",
            start_time, "Study Name", Gender, Age, "Group", "Type", "Label", "Start (ms)", "Duration (ms)", "Parent Stimulus", "Comment",
            "Voice Activity (ms)", "Voice Activity (%)", "Maximum Emotion", "Average Fundamental Frequency Minimum", "Average Fundamental Frequency Maximum",
            "Average Fundamental Frequency Standard Deviation", "Average Loudness Minimum", "Average Loudness Maximum", "Average Loudness Standard Deviation",
            "Average Male Confidence", "Average Female Confidence", "Average Child Confidence", "Average Estimated Age", "Segment_EN"
        )

    FROM CAST_NA

),

EXCLUDED_PT AS (

    SELECT COLUMNS(c -> NOT regexp_matches(c, '_PT$'))
    FROM FEATURE_DATA

),

RANAMED_COLUMNS AS (

    SELECT
        * RENAME (
            WC AS wc, Exclam AS exclam, OtherP AS otherp, Apostro AS apostro, Period AS period, AllPunc AS all_punc, QMark AS q_mark,
            Comma AS comma, "Anger (%)" AS anger_pct, "Happiness (%)" AS happiness_pct, "Neutrality (%)" AS neutrality_pct,
            "Sadness (%)" AS sadness_pct, "Average Activation" AS avg_activation, "Average Dominance" AS avg_dominance, "Average Valence" AS avg_valence,
            "Average Fundamental Frequency Average" AS avg_fundamental_freq, "Average Loudness Average" AS avg_loudness, "Average Speaking Rate" AS avg_speaking_rate,
            "Average Speaking Rate Variation" AS avg_speaking_rate_variation, "Average Intonation Score" AS avg_intonation_score
        )

    FROM EXCLUDED_PT

)

SELECT
    session_id,
    time_in,
    time_out,
    ads_comparison,
    country,
    COLUMNS(* EXCLUDE (session_id, time_in, time_out, ads_comparison, country))::DOUBLE

FROM RANAMED_COLUMNS
