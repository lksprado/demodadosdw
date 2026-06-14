{{ config(
    tags=["camara","senado"]
) }}

WITH
deputados AS (
    SELECT *
    FROM {{ ref('int_deputados') }}
),

senadores AS (
    SELECT *
    FROM {{ ref('int_senadores') }}
),

parlamentares AS (
    SELECT * FROM deputados
    UNION ALL
    SELECT * FROM senadores
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'id']) }} AS sk_parlamentar,
        casa,
        id,
        nome,
        nome_completo,
        sexo,
        uf
    FROM parlamentares
)

SELECT * FROM final
