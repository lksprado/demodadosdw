{{ config(
    tags=["camara", "senado", "votacoes"]
) }}

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
        '{{ var('null_string') }}' AS tipo_lideranca,
        partido AS sigla_partido_bloco,
        orientacao_voto
    FROM {{ ref('int_orientacoes_senado_filtradas') }}
),

unioned AS (
    SELECT * FROM orientacao_camara
    UNION ALL
    SELECT * FROM orientacao_senado
)

SELECT * FROM unioned
UNION ALL
{{ dummy_row([
    ['sk_votacao', 'sk'],
    ['tipo_lideranca', 'text'],
    ['sigla_partido_bloco', 'text'],
    ['orientacao_voto', 'text'],
]) }}
