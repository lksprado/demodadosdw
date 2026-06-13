{{ config(
    tags=["camara","votacoes"]
) }}


WITH
camara_votacoes AS (
    SELECT
        *,
        'camara' AS casa
    FROM {{ ref('stg_camara_votacoes') }}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id', 'casa']) }} AS sk_votacao,
        casa,
        id,
        data_votacao,
        sigla_orgao,
        proposicao_objeto,
        aprovado
    FROM camara_votacoes
    ORDER BY data_votacao DESC
)

SELECT * FROM renamed
