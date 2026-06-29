{{ config(
    tags=["camara", "senado", "score"]
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
    FROM {{ ref('int_deputados_pontuacao_explodida') }}
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
    FROM {{ ref('int_senadores_pontuacao_explodida') }}
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
    ['ano', 'null::int'],
    ['pontuacao', 'null::numeric'],
    ['nota_base_votacoes', 'null::numeric'],
    ['nota_base_gastos', 'null::numeric'],
    ['nota_base_presenca', 'null::numeric'],
    ['nota_base_privilegios', 'null::numeric'],
    ['bonus_processos', 'null::numeric'],
    ['bonus_producao_legislativa', 'null::numeric'],
    ['bonus_articulacao_legislativa', 'null::numeric'],
]) }}
