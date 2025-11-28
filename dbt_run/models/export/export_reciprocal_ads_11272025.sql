{{
    config(
        materialized='external',
        location="../data/export/reciprocal_ads_11272025.csv"
    )
}}

SELECT * FROM {{ ref("reciprocal_ads_11272025") }}