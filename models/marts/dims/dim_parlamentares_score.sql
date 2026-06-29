{{ config(
    tags=["camara", "senado", "score"]
) }}

WITH
deputados AS (
    SELECT
        sk_parlamentar,
        pontuacao_geral,
        ranking_geral,
        ranking_casa,
        ranking_partido,
        ranking_estado,
        ranking_casa_estado
    FROM {{ ref('int_deputados_pontuacao') }}
),

senadores AS (
    SELECT
        sk_parlamentar,
        pontuacao_geral,
        ranking_geral,
        ranking_casa,
        ranking_partido,
        ranking_estado,
        ranking_casa_estado
    FROM {{ ref('int_senadores_pontuacao') }}
),

unioned AS (
    SELECT * FROM deputados
    UNION ALL
    SELECT * FROM senadores
)

SELECT * FROM unioned
UNION ALL
{{ dummy_row([
    ['sk_parlamentar', 'sk'],
    ['pontuacao_geral', 'null::numeric'],
    ['ranking_geral', 'null::int'],
    ['ranking_casa', 'null::int'],
    ['ranking_partido', 'null::int'],
    ['ranking_estado', 'null::int'],
    ['ranking_casa_estado', 'null::int'],
]) }}
