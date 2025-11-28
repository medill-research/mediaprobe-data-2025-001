{{
    config(
        materialized='external',
        location="../data/export/master_ads_11272025.csv"
    )
}}

SELECT * FROM {{ ref("master_ads_11272025") }}