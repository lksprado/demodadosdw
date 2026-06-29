{{ config(
    tags=["camara", "votacoes"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_votacoes') }}
),

renamed AS (
    SELECT
        id                                           AS votacao_id_nk,
        data::DATE                                   AS data_votacao,
        datahoraregistro::TIMESTAMP                  AS datahora_votacao,
        siglaorgao                                   AS sigla_orgao,
        proposicaoobjeto                             AS proposicao_objeto,
        {{ clean_string("descricao","upper") }}      AS descricao,
        SPLIT_PART(uriproposicaoobjeto, '/', 7)::INT AS proposicao_id_fk,
        aprovacao::INT                               AS aprovado
    FROM source
)

SELECT * FROM renamed
