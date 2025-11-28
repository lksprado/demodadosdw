{{ config(
    tags=["fct","radar","parlamentar"]
) }}


WITH
de_para as (
    select 
    *
    from {{ ref('int_map_parlamentares')}}
),
fato as (
    select 
    t2.sk_parlamentar,
    t2.casa,
    date_trunc('quarter', t1.data_trimestre - interval '1 month')::date as data_trimestre,
    -- date_trunc('quarter', t1.data_trimestre)::date as data_trimestre,
    t1.perc_governismo_trimestre,
    t1.data_carga
    from {{ ref("stg_radarcongresso__governismo_deputados") }} t1
    inner join 
    de_para t2
    on t1.id_parlamentar_radar = t2.id_parlamentar_radar
)
select * from fato

