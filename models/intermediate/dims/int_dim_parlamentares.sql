{{ config(
    tags=["dim","parlamentar"]
) }}

WITH
senadores as (
    select * from {{ ref('int_dim_senadores')}}
),
deputados as (
    select * from {{ ref('int_dim_deputados')}}
)
select * from senadores
union all 
select * from deputados
