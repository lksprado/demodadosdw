{{ config(
    tags=["camara","votacoes"]
) }}


WITH
senado_votacoes AS (
    SELECT
        *,
        'SENADO' AS casa
    FROM {{ ref('stg_senado_votacoes') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['codigo_votacao', 'casa']) }} AS sk_votacao,
        casa,
        codigo_votacao AS id,
        data_sessao,
        identificacao,
        CASE
            WHEN resultado_votacao = 'APROVADO' THEN 1
            ELSE 0
        END AS aprovado
    FROM senado_votacoes
    WHERE codigo_votacao IS NOT NULL
    ORDER BY data_sessao DESC
    
)

SELECT * FROM final
