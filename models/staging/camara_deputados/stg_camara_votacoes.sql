{{ config(
    tags=["stg","camara","votacoes"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_votacoes') }}
),

renamed AS (
    SELECT
        id,
        data::DATE AS data_votacao,
        datahoraregistro::TIMESTAMP AS datahora_votacao,
        siglaorgao AS sigla_orgao,
        proposicaoobjeto AS proposicao_objeto,
        {{ clean_string("descricao","upper") }} AS descricao,
        (aprovacao::INT)::BOOLEAN AS aprovado
    FROM source
)

SELECT * FROM renamed
