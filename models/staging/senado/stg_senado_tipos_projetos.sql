{{ config(
    tags=["stg","senado","proposicoes"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_tipos_projetos') }}
)

SELECT
    sigla AS sigla_proposicao,
    descricao AS descricao_proposicao,
    "dataInicio" AS data_inicio,
    "dataFim" AS data_fim
FROM source
