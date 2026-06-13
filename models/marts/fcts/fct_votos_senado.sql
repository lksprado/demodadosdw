{{ config(
    tags=["fct","senado","votacoes"]
) }}

WITH orientacao_governo AS (
    SELECT
        codigo_votacao,
        voto AS voto_governo
    FROM {{ ref('stg_senado_votacoes_orientacao') }}
    WHERE partido = 'GOVERNO'
      AND voto NOT IN ('LIBERADO', 'ABSTENCAO')
),

votos_senadores AS (
    SELECT
        codigo_sessao_votacao,
        codigo_parlamentar,
        data_sessao,
        identificacao,
        sigla,
        resultado_votacao,
        CASE
            WHEN sigla_voto = 'SIM'                   THEN 'SIM'
            WHEN sigla_voto = 'NAO'                   THEN 'NAO'
            WHEN sigla_voto = 'ABSTENCAO'             THEN 'ABSTENCAO'
            WHEN sigla_voto IN ('OBSTRUCAO', 'P-OD')  THEN 'OBSTRUCAO'
        END AS voto
    FROM {{ ref('stg_senado_votos_senadores') }}
),

de_para AS (
    SELECT sk_parlamentar, id_nk::INT AS id_nk
    FROM {{ ref('int_map_parlamentares') }}
    WHERE casa = 'senado'
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['v.codigo_parlamentar', 'o.codigo_votacao', "'senado'"]) }} AS sk_voto,
        d.sk_parlamentar,
        o.codigo_votacao,
        v.data_sessao,
        v.identificacao,
        v.sigla,
        v.resultado_votacao,
        o.voto_governo,
        v.voto AS voto_senador,
        CASE
            WHEN o.voto_governo = 'SIM' AND v.voto = 'SIM' THEN TRUE
            WHEN o.voto_governo = 'NAO' AND v.voto = 'NAO' THEN TRUE
            WHEN v.voto = 'ABSTENCAO'                       THEN NULL
            ELSE FALSE
        END AS alinhado_ao_governo
    FROM orientacao_governo AS o
    INNER JOIN votos_senadores AS v
        ON o.codigo_votacao = v.codigo_sessao_votacao
    INNER JOIN de_para AS d
        ON v.codigo_parlamentar = d.id_nk
    WHERE v.voto IS NOT NULL
)

SELECT * FROM joined
