WITH
de_para as (
    select 
    *
    from {{ ref('int_map_parlamentares')}}
),
fato as (
    select 
    t2.sk_parlamentar,
    t1.total_votos_favor_governo,
    t1.total_votos_contra_governo,
    t1.perc_governismo,
    t1.data_carga
    from {{ ref("stg_radarcongresso__governismo_senadores") }} t1
    inner join 
    de_para t2
    on t1.id_parlamentar_radar = t2.id_parlamentar_radar
    group by 
    t2.sk_parlamentar,
    t1.total_votos_favor_governo,
    t1.total_votos_contra_governo,
    t1.perc_governismo,
    t1.data_carga
)
select * from fato