{{ config(
    tags=["camara","senado"]
) }}

WITH
deputados AS (
    SELECT
        sk_parlamentar,
        ano,
        pontuacao,
        nota_base_votacoes,
        nota_base_gastos,
        nota_base_presenca,
        nota_base_privilegios,
        bonus_processos,
        bonus_producao_legislativa,
        bonus_articulacao_legislativa
    FROM {{ ref('int_deputados_score_explodido') }}
),

senadores AS (
    SELECT
        sk_parlamentar,
        ano,
        pontuacao,
        nota_base_votacoes,
        nota_base_gastos,
        nota_base_presenca,
        nota_base_privilegios,
        bonus_processos,
        bonus_producao_legislativa,
        bonus_articulacao_legislativa
    FROM {{ ref('int_senadores_score_explodido') }}
),

final AS (
    SELECT * FROM deputados
    UNION ALL
    SELECT * FROM senadores
)

SELECT * FROM final
