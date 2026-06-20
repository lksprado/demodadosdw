{{ config(
    tags=["camara","senado"]
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
        {{ dbt_utils.generate_surrogate_key(['casa', 'id']) }} AS sk_parlamentar,
        casa,
        id,
        nome,
        nome_completo,
        sexo,
        uf
    FROM parlamentares
),

-- linha dummy para FKs sem correspondência (COALESCE com null_key nas facts cai aqui)
dummy AS (
    SELECT
        '{{ var('null_key') }}'    AS sk_parlamentar,
        '{{ var('null_string') }}' AS casa,
        0                          AS id,
        '{{ var('null_string') }}' AS nome,
        '{{ var('null_string') }}' AS nome_completo,
        '{{ var('null_string') }}' AS sexo,
        '{{ var('null_string') }}' AS uf
)

SELECT * FROM final
UNION ALL
SELECT * FROM dummy
