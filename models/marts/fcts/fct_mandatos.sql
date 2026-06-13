{{ config(
    tags=["fct","parlamentar"]
) }}

WITH
de_para AS (
    SELECT sk_parlamentar, id_nk, casa
    FROM {{ ref('int_map_parlamentares') }}
),

mandatos_camara AS (
    SELECT
        id,
        id_legislatura,
        partido,
        uf_representacao,
        data_posse,
        situacao_atual,
        condicao_eleitoral,
        num_gabinete_predio,
        num_gabinete_andar,
        num_gabinete_sala,
        data_carga
    FROM {{ ref('stg_camara_deputados') }}
),

mandatos_senado_atual AS (
    SELECT
        id,
        id_legislatura,
        partido,
        uf_representacao,
        data_posse,
        situacao_atual,
        condicao_eleitoral,
        num_gabinete_predio,
        num_gabinete_andar,
        num_gabinete_sala,
        data_carga,
        1 AS prioridade
    FROM {{ ref('stg_senado_senadores') }}
),

mandatos_senado_historico AS (
    SELECT
        id,
        id_legislatura,
        partido,
        uf_representacao,
        data_posse,
        situacao_atual,
        condicao_eleitoral,
        num_gabinete_predio,
        num_gabinete_andar,
        num_gabinete_sala,
        data_carga,
        2 AS prioridade
    FROM {{ ref('stg_senado_legislaturas') }}
),

-- prioridade 1 = senadores ativos; prioridade 2 = histórico
mandatos_senado AS (
    SELECT DISTINCT ON (id, id_legislatura)
        id,
        id_legislatura,
        partido,
        uf_representacao,
        data_posse,
        situacao_atual,
        condicao_eleitoral,
        num_gabinete_predio,
        num_gabinete_andar,
        num_gabinete_sala,
        data_carga
    FROM (
        SELECT * FROM mandatos_senado_atual
        UNION ALL
        SELECT * FROM mandatos_senado_historico
    ) AS combined
    ORDER BY id, id_legislatura, prioridade
),

final_camara AS (
    SELECT
        t1.sk_parlamentar,
        t2.id_legislatura,
        t2.partido,
        t2.uf_representacao,
        t2.data_posse,
        t2.situacao_atual,
        t2.condicao_eleitoral,
        t2.num_gabinete_predio,
        t2.num_gabinete_andar,
        t2.num_gabinete_sala,
        t2.data_carga
    FROM de_para AS t1
    INNER JOIN mandatos_camara AS t2 ON t1.id_nk = t2.id
    WHERE t1.casa = 'camara'
),

final_senado AS (
    SELECT
        t1.sk_parlamentar,
        t2.id_legislatura,
        t2.partido,
        t2.uf_representacao,
        t2.data_posse,
        t2.situacao_atual,
        t2.condicao_eleitoral,
        t2.num_gabinete_predio,
        t2.num_gabinete_andar,
        t2.num_gabinete_sala,
        t2.data_carga
    FROM de_para AS t1
    INNER JOIN mandatos_senado AS t2 ON t1.id_nk = t2.id
    WHERE t1.casa = 'senado'
)

SELECT * FROM final_camara
UNION ALL
SELECT * FROM final_senado
