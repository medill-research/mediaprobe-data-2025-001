{{
    config(
        materialized='external',
        location="../data/export/reciprocal_ads.csv"
    )
}}

SELECT * FROM {{ ref("reciprocal_ads") }}