{{ config(
    tags=["camara", "legislacao"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_proposicao_tema') }}
),

renamed AS (
    SELECT
        SPLIT_PART(url_temas, '/', 7)      AS proposicao_id_nk,
        {{ clean_string("tema","upper") }} AS tema,
        codtema                            AS codigo_tema,
        relevancia::INT                    AS relevancia
    FROM source
)

SELECT * FROM renamed
