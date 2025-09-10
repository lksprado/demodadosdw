WITH
dim as (
    select 
    sk_parlamentar,
    fk_id_parlamentar_radar
    from {{ ref('int_dim_senadores')}}
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
    dim t2
    on t1.id_parlamentar_radar = t2.fk_id_parlamentar_radar
    group by 
    t2.sk_parlamentar,
    t1.total_votos_favor_governo,
    t1.total_votos_contra_governo,
    t1.perc_governismo,
    t1.data_carga
)
select * from fato