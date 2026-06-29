{{ config(
    tags=["camara", "senado", "votacoes"]
) }}

WITH
votacoes_camara AS (
    SELECT
        DISTINCT ON (sk_votacao)
        sk_votacao,
        votacao_id_nk,
        casa,
        data_votacao,
        sk_proposicao,
        aprovado,
        sk_data
    FROM {{ ref('int_votacoes_camara_deduplicadas') }}
),

votacoes_senado AS (
    SELECT
        DISTINCT ON (sk_votacao)
        sk_votacao,
        votacao_id_nk,
        casa,
        data_votacao,
        sk_proposicao,
        aprovado,
        sk_data
    FROM {{ ref('int_votacoes_senado_filtradas') }}
),

unioned AS (
    SELECT * FROM votacoes_camara
    UNION ALL
    SELECT * FROM votacoes_senado
)

SELECT * FROM unioned
UNION ALL
{{ dummy_row([
    ['sk_votacao', 'sk'],
    ['votacao_id_nk', 'null::text'],
    ['casa', 'text'],
    ['data_votacao', 'null::date'],
    ['sk_proposicao', 'sk'],
    ['aprovado', 'null::int'],
    ['sk_data', '1'],
]) }}
