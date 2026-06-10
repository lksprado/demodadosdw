{{ config(
    enabled=false,
    tags=["fct","radar","parlamentar"]
) }}

with todas_casas as (
    select
        id_parlamentar_radar,
        total_votos_favor_governo,
        total_votos_contra_governo,
        perc_governismo,
        data_carga
    from {{ ref("stg_radarcongresso_governismo_deputados") }}

    union all

    select
        id_parlamentar_radar,
        total_votos_favor_governo,
        total_votos_contra_governo,
        perc_governismo,
        data_carga
    from {{ ref("stg_radarcongresso_governismo_senadores") }}
),

de_para as (
    select *
    from {{ ref('int_map_parlamentares') }}
),

fato as (
    select distinct
        t2.sk_parlamentar,
        t2.casa,
        t1.total_votos_favor_governo,
        t1.total_votos_contra_governo,
        t1.perc_governismo,
        t1.data_carga
    from todas_casas t1
    inner join de_para t2
        on t1.id_parlamentar_radar = t2.id_parlamentar_radar
)

select * from fato
