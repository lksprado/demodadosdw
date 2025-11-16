{{ config(
    tags=["fct","ranking","parlamentar"]
) }}


WITH
de_para as (
    select 
    *
    from {{ ref('int_map_parlamentares')}}
    where casa = 'camara'
),
fato as (
    select 
    t2.sk_parlamentar,
    t1.score_total,
    t1.score_antiprivilegio,
    t1.score_antidesperdicio,
    t1.score_processos,
    t1.score_outros,
    t1.ranking_por_cargo,
    t1.ranking_por_estado,
    t1.cnt_parlamentares_estado,
    t1.cnt_cargo_por_estado,
    t1.total_parlamentares,
    t1.total_parlamentar_por_casa,
    t1.perc_economia,
    t1.vlr_mandato_disponivel_total,
    t1.vlr_mandato_gasto_total,
    t1.vlr_mandato_economizado_total,
    t1.vlr_cota_parlamentar_disponivel_subtotal,
    t1.vlr_cota_parlamentar_gasto_subtotal,
    t1.vlr_cota_parlamentar_economizado_subtotal,
    t1.vlr_verba_gabinete_disponivel_subtotal,
    t1.vlr_verba_gabinete_gasto_subtotal,
    t1.vlr_verba_gabinete_economizado_subtotal
    from {{ ref("stg_ranking__parlamentares") }} t1
    inner join 
    de_para t2
    on t1.id_parlamentar_ranking = t2.id_parlamentar_ranking
)
select * from fato