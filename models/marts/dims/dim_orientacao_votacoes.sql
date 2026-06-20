WITH
orientacao_camara AS (
    SELECT
        sk_votacao,
        tipo_lideranca,
        sigla_partido_bloco,
        orientacao_voto
    FROM {{ ref('int_orientacoes_camara_corrigidas') }}
),

orientacao_senado AS (
    SELECT
        sk_votacao,
        NULL AS tipo_lideranca,
        partido AS sigla_partido_bloco,
        orientacao_voto
    FROM {{ ref('int_orientacoes_senado_filtradas') }}
),

final AS (
    SELECT * FROM orientacao_camara
    UNION ALL
    SELECT * FROM orientacao_senado
)

SELECT * FROM final
