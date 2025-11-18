{{ config(
    tags=["dim","senado"]
) }}

with 
tipos_proposicoes as (
    select * from {{ ref('stg_senado__tipos_projetos')}}
)
select * from tipos_proposicoes