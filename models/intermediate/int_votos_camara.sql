{{ config(
    tags=["camara","votacoes"]
) }}


WITH
camara_votos AS (
    SELECT
        *,
        'camara' AS casa
    FROM {{ ref('stg_camara_votos_deputados') }}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_deputado','id_votacao', 'casa']) }} AS sk_voto,
        {{ dbt_utils.generate_surrogate_key(['id_deputado', 'casa']) }} AS sk_parlamentar,
        {{ dbt_utils.generate_surrogate_key(['id_votacao', 'casa']) }} AS sk_votacao,
        casa,
        id_deputado,
        id_votacao,
        id_legislatura,
        CASE
            WHEN voto = 'BRANCO' THEN NULL
            WHEN voto = 'FAVORAVEL COM RESTRICOES' THEN 'SIM'
            ELSE voto
        END AS voto
    FROM camara_votos
)

SELECT * FROM renamed
