{{ config(
    tags=["fct","senado"]
) }}


WITH
proposicoes AS (
    SELECT * FROM {{ ref('stg_ecidadania__paginas')}}
),
mais_votados AS (
    SELECT * FROM {{ ref('stg_ecidadania__mais_votados')}}
)
select 
t1.*,
case when t2.sk_proposicao is not null then true else false end::boolean as flag_mais_votado_no_dia
from proposicoes t1
left join mais_votados t2 
on t1.sk_proposicao = t2.sk_proposicao and t1.data_extracao = t2.data_extracao