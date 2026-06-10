{{ config(
    tags=["dim","senado"]
) }}

WITH
tipos_proposicoes AS (
    SELECT * FROM {{ ref('stg_senado_tipos_projetos') }}
)

SELECT * FROM tipos_proposicoes
