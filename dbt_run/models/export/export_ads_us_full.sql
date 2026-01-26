{{
    config(
        materialized='external',
        location="../data/export/ads_us_full.csv"
    )
}}

SELECT * FROM {{ ref("ads_us_full") }}