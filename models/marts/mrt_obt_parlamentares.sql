WITH
parlamentares AS (
    SELECT * FROM {{ ref('int_dim_parlamentares')}}
),

fct_gov_total AS (
    SELECT * FROM {{ ref('int_fct_governismo_senadores_total')}}
    UNION ALL 
    SELECT * FROM {{ ref('int_fct_governismo_deputados_total')}}
),
fct_ranking_total AS (
    SELECT * FROM {{ ref('int_fct_ranking_senadores_total')}}
    UNION ALL 
    SELECT * FROM {{ ref('int_fct_ranking_deputados_total')}}
),
tab AS (
    SELECT
        t1.sk_parlamentar,
        t1.nome,
        t1.partido,
        t1.casa,
        t1.nome_eleitoral,
        t1.email,
        t1.redesocial_x_twitter,
        t1.redesocial_instagram,
        t1.redesocial_facebook,
        t1.redesocial_youtube,
        t1.telefone,
        t1.sexo,
        t1.data_nascimento,
        t1.geracao,
        t1.uf_nascimento,
        t1.municipio_nascimento,
        t1.uf_representacao,
        t1.escolaridade,
        t1.id_legislatura,
        t1.data_posse,
        t1.num_gabinete_predio,
        t1.num_gabinete_andar,
        t1.num_gabinete_sala,
        t1.situacao_atual,
        t1.condicao_eleitoral,
        t1.link_api_oficial,
        t1.link_foto,
        t2.total_votos_favor_governo,
        t2.total_votos_contra_governo,
        t2.perc_governismo,
        t3.score_total,
        t3.score_antiprivilegio,
        t3.score_antidesperdicio,
        t3.score_processos,
        t3.score_outros,
        t3.ranking_por_cargo,
        t3.ranking_por_estado,
        t3.cnt_parlamentares_estado,
        t3.cnt_cargo_por_estado,
        t3.total_parlamentares,
        t3.total_parlamentar_por_casa,
        t3.perc_economia,
        t3.vlr_mandato_disponivel_total,
        t3.vlr_mandato_gasto_total,
        t3.vlr_mandato_economizado_total,
        t3.vlr_cota_parlamentar_disponivel_subtotal,
        t3.vlr_cota_parlamentar_gasto_subtotal,
        t3.vlr_cota_parlamentar_economizado_subtotal,
        t3.vlr_verba_gabinete_disponivel_subtotal,
        t3.vlr_verba_gabinete_gasto_subtotal,
        t3.vlr_verba_gabinete_economizado_subtotal
    FROM parlamentares AS t1
    LEFT JOIN fct_gov_total AS t2
    ON t1.sk_parlamentar = t2.sk_parlamentar
    LEFT JOIN fct_ranking_total AS t3
    ON t1.sk_parlamentar = t3.sk_parlamentar
)
SELECT * FROM tab
