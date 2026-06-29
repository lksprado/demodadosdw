{{ config(
    tags=["camara", "legislacao"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_proposicao') }}
),

renamed AS (
    SELECT
        CASE
            WHEN id ~ '^-?\d+$' THEN id::INT
            ELSE SPLIT_PART(statusproposicao_descricaosituacao, '/', 7)::INT
        END                                         AS proposicao_id_nk,
        {{ clean_integer("codtipo") }}              AS codigo_tipo,
        {{ clean_string("descricaotipo","upper") }} AS descricao_tipo,
        dataapresentacao::TIMESTAMP::DATE           AS data_proposicao,
        {{ clean_string("keywords","upper") }}      AS palavras_chave
    FROM source
)

SELECT * FROM renamed
