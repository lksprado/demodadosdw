{{ config(
    tags=["mrt","ecidadania"]
) }}


WITH
tab AS (
    SELECT * FROM {{ ref('int_fct_ecidadania_bignumbers') }}
)

SELECT *
FROM tab
