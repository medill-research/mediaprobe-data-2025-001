{{
    config(
        materialized='external',
        location="../data/export/master_ads.csv"
    )
}}

SELECT * FROM {{ ref("master_ads") }}