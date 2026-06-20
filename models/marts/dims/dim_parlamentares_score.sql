{{ config(
    tags=["camara","senado"]
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

final AS (
    SELECT * FROM deputados
    UNION ALL
    SELECT * FROM senadores
)

SELECT * FROM final
