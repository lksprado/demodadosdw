{{ config(
    tags=["senado","votacoes"]
) }}


WITH
senado_votos AS (
    SELECT
        *,
        'SENADO' AS casa
    FROM {{ ref('stg_senado_votacoes_orientacao') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['codigo_votacao', 'casa']) }} AS sk_votacao,
        casa,
        codigo_votacao,
        numero_materia,
        sigla_tipo_materia,
        partido,
        orientacao_voto
    FROM senado_votos
    WHERE orientacao_voto IS NOT NULL
        AND orientacao_voto <> 'LIBERADO'
)

SELECT * FROM final
