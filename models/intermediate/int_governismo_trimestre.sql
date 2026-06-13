{{ config(
    enabled=false,
    tags=["radar","parlamentar"]
) }}

with todas_casas as (
    select
        id_parlamentar_radar,
        data_trimestre,
        perc_governismo_trimestre,
        data_carga
    from {{ ref("stg_radarcongresso_governismo_deputados") }}

    union all

    select
        id_parlamentar_radar,
        data_trimestre,
        perc_governismo_trimestre,
        data_carga
    from {{ ref("stg_radarcongresso_governismo_senadores") }}
),

de_para as (
    select *
    from {{ ref('int_map_parlamentares') }}
),

fato as (
    select
        t2.sk_parlamentar,
        t2.casa,
        date_trunc('quarter', t1.data_trimestre - interval '1 month')::date as data_trimestre,
        t1.perc_governismo_trimestre,
        t1.data_carga
    from todas_casas t1
    inner join de_para t2
        on t1.id_parlamentar_radar = t2.id_parlamentar_radar
)

select * from fato
