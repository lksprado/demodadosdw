{{ config(
    tags=["ecidadania", "participacao"]
) }}

WITH
bignumbers AS (
    SELECT * FROM {{ ref('stg_ecidadania_bignumbers') }}
)

SELECT * FROM bignumbers
