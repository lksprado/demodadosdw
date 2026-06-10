{{ config(
    tags=["mrt","camara","votacoes"]
) }}

WITH
orientacao_governo AS (
    SELECT * FROM {{ ref('int_fct_votacoes_orientacao') }}
    WHERE
        sigla_partido_bloco = 'GOVERNO'
        AND orientacao_voto <> 'LIBERADO'
        AND orientacao_voto <> 'ABSTENCAO'
),

votos_parlamentares AS (
    SELECT * FROM {{ ref('int_fct_votos') }}
    WHERE voto IS NOT NULL
),

votacao AS (
    SELECT *
    FROM {{ ref('int_fct_votacoes') }}
),

joined AS (
    SELECT
        t1.sk_votacao,
        t1.id_votacao,
        t1.casa,
        t1.orientacao_voto AS voto_governo,
        t2.sk_parlamentar,
        t2.sk_voto,
        t2.id_deputado,
        t2.voto AS voto_deputado,
        t3.data_votacao,
        t3.sigla_orgao,
        t3.proposicao_objeto,
        t3.aprovado
    FROM orientacao_governo AS t1
    INNER JOIN votos_parlamentares AS t2
        ON t1.sk_votacao = t2.sk_votacao
    LEFT JOIN votacao AS t3
        ON t1.sk_votacao = t3.sk_votacao
),

final AS (
    SELECT
        *,
        CASE
            WHEN voto_governo = 'SIM' AND voto_deputado = 'SIM' THEN TRUE
            WHEN voto_governo = 'NAO' AND voto_deputado = 'NAO' THEN TRUE
            WHEN voto_governo = 'OBSTRUCAO' AND voto_deputado = 'OBSTRUCAO' THEN TRUE
            WHEN voto_deputado = 'ABSTENCAO' THEN NULL
            ELSE FALSE
        END AS alinhado_ao_governo
    FROM joined
)

SELECT * FROM final
