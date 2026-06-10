{{ config(
    tags=["fct","camara","votacoes"]
) }}


WITH
camara_votos AS (
    SELECT
        *,
        'camara' AS casa
    FROM {{ ref('stg_camara_votacoes_orientacao') }}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_votacao', 'casa']) }} AS sk_votacao,
        id_votacao,
        casa,
        sigla_partido_bloco,
        orientacao_voto,
        codigo_tipo_liderancao,
        codigo_partido_bloco
    FROM camara_votos
    WHERE orientacao_voto IS NOT NULL
)

SELECT * FROM renamed
