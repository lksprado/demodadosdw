{{ config(
    tags=["camara", "senado", "parlamentar", "votacoes"]
) }}

WITH presidentes AS (
    SELECT
        presidente,
        mandato,
        inicio::DATE AS inicio,
        fim::DATE AS fim
    FROM {{ ref('raw_executivo_presidente') }}
),
legislaturas as (
    SELECT 
        legislatura::INT as legislatura,
        inicio::DATE as inicio,
        fim::DATE as fim
    FROM {{ ref('raw_legislaturas') }}
),
orientacao_governo AS (
    SELECT
        sk_votacao,
        orientacao_voto
    FROM {{ ref('dim_orientacao_votacoes') }}
    WHERE sigla_partido_bloco = 'GOVERNO'
),
proposicoes as (
    SELECT
        sk_proposicao,
        proposicao_id_nk,
        tipo_proposicao,
        data_proposicao
    FROM
        {{ ref('dim_proposicoes') }}
),
votacoes_orientadas_governo AS (
    SELECT

        t1.sk_votacao,
        t1.votacao_id_nk,
        t1.sk_proposicao,
        t1.casa,
        t1.data_votacao,
        t1.aprovado,
        t2.orientacao_voto
    FROM {{ ref('dim_votacoes') }} AS t1
    INNER JOIN orientacao_governo AS t2
        ON t1.sk_votacao = t2.sk_votacao
),

votos_parlamentares_joined AS (
    SELECT
        t5.sk_proposicao,
        t5.tipo_proposicao,
        t5.data_proposicao,
        t1.casa,
        t1.sk_voto,
        t1.sk_parlamentar,
        t1.sk_votacao,
        t4.deputado_id_nk,
        t4.senador_id_nk,
        t4.nome,
        t4.uf,
        t2.votacao_id_nk,
        t2.data_votacao,
        t3.legislatura,
        t2.aprovado,
        t1.voto,
        t2.orientacao_voto AS voto_governo,
        CASE
            WHEN t1.voto = t2.orientacao_voto THEN 1
            ELSE 0
        END AS voto_alinhado
    FROM {{ ref('fct_votos') }} AS t1
    INNER JOIN votacoes_orientadas_governo AS t2
        ON t1.sk_votacao = t2.sk_votacao
    LEFT JOIN legislaturas AS t3
        ON t2.data_votacao BETWEEN t3.inicio AND t3.fim
    LEFT JOIN {{ ref('dim_parlamentares') }} AS t4
        ON t1.sk_parlamentar = t4.sk_parlamentar
    LEFT JOIN proposicoes AS t5
        ON t2.sk_proposicao = t5.sk_proposicao
),

final AS (
    SELECT
        
        sk_voto,
        sk_parlamentar,
        sk_votacao,

        votacao_id_nk,
        deputado_id_nk,
        senador_id_nk,
        
        casa,
        nome,
        uf,
        tipo_proposicao,
        legislatura,
        aprovado,
        voto,
        voto_governo,
        voto_alinhado,
        
        data_proposicao,
        data_votacao
    FROM votos_parlamentares_joined
)

SELECT * FROM final
