{{ config(
    tags=["fct","ecidadania"]
) }}

WITH
bignumbers AS (
    SELECT * FROM {{ ref('stg_ecidadania_bignumbers') }}
)

SELECT * FROM bignumbers
