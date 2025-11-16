{{ config(
    tags=["dim","parlamentar"]
) }}


WITH
de_para as (
    select
    sk_parlamentar,
    id_nk,
    casa
    from {{ ref('int_map_parlamentares') }}
),
deputados AS (
    SELECT * FROM {{ ref('stg_parlamento__deputados')}}
),
enrichment as (
    select * from {{ ref('stg_ranking__parlamentares')}}
),
final as (
    select
    t1.sk_parlamentar,
    t2.nome,
    t2.partido,
    t1.casa,
    t2.nome_eleitoral,
    t2.email,
    CASE WHEN t2.redesocial_x_twitter IS NULL AND t3.redesocial_x_twitter IS NOT NULL THEN t3.redesocial_x_twitter ELSE  t2.redesocial_x_twitter END AS redesocial_x_twitter,
    CASE WHEN t2.redesocial_instagram IS NULL AND t3.redesocial_instagram IS NOT NULL THEN t3.redesocial_instagram ELSE  t2.redesocial_instagram END AS redesocial_instagram,
    CASE WHEN t2.redesocial_facebook IS NULL AND t3.redesocial_facebook IS NOT NULL THEN t3.redesocial_facebook ELSE  t2.redesocial_facebook END AS redesocial_facebook,
    CASE WHEN t2.redesocial_youtube IS NULL AND t3.redesocial_youtube IS NOT NULL THEN t3.redesocial_youtube ELSE  t2.redesocial_youtube END AS redesocial_youtube,
    t2.telefone,
    t2.sexo,
    t2.data_nascimento,
    t2.geracao,
    t2.uf_nascimento,
    t2.municipio_nascimento,
    t2.uf_representacao,
    t2.escolaridade,
    t2.id_legislatura,
    t2.data_posse,
    t2.num_gabinete_predio,
    t2.num_gabinete_andar,
    t2.num_gabinete_sala,
    t2.situacao_atual,
    t2.condicao_eleitoral,
    t2.link_api_oficial,
    t2.link_foto,
    t2.data_carga
    from de_para t1
    left join deputados t2
    on t1.id_nk = t2.id
    left join enrichment t3 
    on t1.id_nk = t3.id_parlamentar_ranking
    where t1.casa = 'camara'
)
SELECT * FROM final
