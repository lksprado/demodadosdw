{{ config(
    tags=["mrt","ecidadania","senado"]
) }}


WITH
tab AS (
    SELECT * FROM {{ ref('int_fct_ecidadania_bignumbers')}}
)
select 
*
from tab