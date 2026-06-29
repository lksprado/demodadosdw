{{ config(
    tags=["camara", "senado", "parlamentar"]
) }}

WITH
deputados AS (
    SELECT *
    FROM {{ ref('int_deputados_padronizados') }}
),

senadores AS (
    SELECT *
    FROM {{ ref('int_senadores_padronizados') }}
),

parlamentares AS (
    SELECT * FROM deputados
    UNION ALL
    SELECT * FROM senadores
),

final AS (
    SELECT
        sk_parlamentar,
        CASE WHEN casa = 'CAMARA' THEN parlamentar_id_nk END AS deputado_id_nk,
        CASE WHEN casa = 'SENADO' THEN parlamentar_id_nk END AS senador_id_nk,
        casa,
        nome,
        nome_completo,
        sexo,
        uf
    FROM parlamentares
)

SELECT * FROM final
UNION ALL
{{ dummy_row([
    ['sk_parlamentar', 'sk'],
    ['deputado_id_nk', 'null::int'],
    ['senador_id_nk', 'null::int'],
    ['casa', 'text'],
    ['nome', 'text'],
    ['nome_completo', 'text'],
    ['sexo', 'text'],
    ['uf', 'text'],
]) }}
