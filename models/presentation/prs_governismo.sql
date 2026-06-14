{{ config(
    tags=["prs","parlamentar"]
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

votacoes_orientadas_governo AS (
    SELECT
        t1.sk_votacao,
        t1.casa,
        t1.data_votacao,
        t1.objeto,
        t1.aprovado,
        t2.orientacao_voto
    FROM {{ ref('dim_votacoes') }} AS t1
    INNER JOIN orientacao_governo AS t2
        ON t1.sk_votacao = t2.sk_votacao
),

votos_parlamentares_joined AS (
    SELECT
        t1.casa,
        t1.sk_voto,
        t1.sk_parlamentar,
        t1.sk_votacao,
        t2.data_votacao,
        t3.legislatura,
        t2.objeto,
        t2.aprovado,
        t1.voto,
        t2.orientacao_voto AS voto_governo
    FROM {{ ref('fct_votos') }} AS t1
    INNER JOIN votacoes_orientadas_governo AS t2
        ON t1.sk_votacao = t2.sk_votacao
    LEFT JOIN legislaturas AS t3
        ON t2.data_votacao BETWEEN t3.inicio AND t3.fim
),

final AS (
    SELECT
        casa,
        sk_voto,
        sk_parlamentar,
        sk_votacao,
        data_votacao,
        legislatura,
        objeto,
        aprovado,
        voto,
        voto_governo,
        CASE
            WHEN voto = voto_governo THEN 1
            ELSE 0
        END AS voto_alinhado
    FROM votos_parlamentares_joined
)

SELECT * FROM final
