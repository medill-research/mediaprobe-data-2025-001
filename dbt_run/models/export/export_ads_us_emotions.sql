{{
    config(
        materialized='external',
        location="../data/export/ads_us_emotions.csv"
    )
}}

SELECT * FROM {{ ref("ads_us_emotions") }}