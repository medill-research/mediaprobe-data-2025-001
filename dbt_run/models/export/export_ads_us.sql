{{
    config(
        materialized='external',
        location="../data/export/ads_us.csv"
    )
}}

SELECT * FROM {{ ref("ads_us") }}