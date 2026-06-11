{{ config(
    tags=["fct","camara","votacoes"]
) }}

WITH orientacao_governo AS (
    SELECT sk_votacao, orientacao_voto AS voto_governo
    FROM {{ ref('int_fct_votacoes_orientacao') }}
    WHERE sigla_partido_bloco = 'GOVERNO'
      AND orientacao_voto NOT IN ('LIBERADO', 'ABSTENCAO')
),

votos_parlamentares AS (
    SELECT sk_parlamentar, sk_voto, sk_votacao, voto
    FROM {{ ref('int_fct_votos') }}
    WHERE voto IS NOT NULL
),

joined AS (
    SELECT
        t2.sk_voto,
        t2.sk_parlamentar,
        t1.sk_votacao,
        t1.voto_governo,
        t2.voto AS voto_deputado,
        CASE
            WHEN t1.voto_governo = 'SIM'       AND t2.voto = 'SIM'       THEN TRUE
            WHEN t1.voto_governo = 'NAO'       AND t2.voto = 'NAO'       THEN TRUE
            WHEN t1.voto_governo = 'OBSTRUCAO' AND t2.voto = 'OBSTRUCAO' THEN TRUE
            WHEN t2.voto = 'ABSTENCAO'                                    THEN NULL
            ELSE FALSE
        END AS alinhado_ao_governo
    FROM orientacao_governo AS t1
    INNER JOIN votos_parlamentares AS t2
        ON t1.sk_votacao = t2.sk_votacao
)

SELECT * FROM joined
