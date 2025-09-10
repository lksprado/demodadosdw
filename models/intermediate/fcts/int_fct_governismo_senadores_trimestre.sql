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
    case 
        when t1.data_trimestre = date '2023-03-31' then date '2023-04-01'
        else t1.data_trimestre
    end as data_trimestre,
    t1.perc_governismo_trimestre,
    t1.data_carga
    from {{ ref("stg_radarcongresso__governismo_senadores") }} t1
    inner join 
    dim t2
    on t1.id_parlamentar_radar = t2.fk_id_parlamentar_radar
)
select * from fato

