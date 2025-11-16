{{ config(
    tags=["fct","senado"]
) }}

WITH
bignumbers AS (
    SELECT * FROM {{ ref('stg_ecidadania__bignumbers')}}
)
select * from bignumbers