{{ config(
    tags=["camara","votacoes"]
) }}


WITH
camara_votos AS (
    SELECT
        *,
        'CAMARA' AS casa
    FROM {{ ref('stg_camara_votos_deputados') }}
),

votos_filtrados AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_deputado','id_votacao', 'casa']) }} AS sk_voto,
        {{ dbt_utils.generate_surrogate_key(['casa', 'id_deputado']) }} AS sk_parlamentar,
        {{ dbt_utils.generate_surrogate_key(['id_votacao', 'casa']) }} AS sk_votacao,
        casa,
        id_deputado,
        id_votacao,
        CASE
            WHEN voto = 'FAVORAVEL COM RESTRICOES' THEN 'SIM'
            ELSE voto
        END AS voto
    FROM camara_votos
    WHERE voto NOT IN ('ARTIGO 17', 'BRANCO', 'ABSTENCAO')
)

SELECT * FROM votos_filtrados
