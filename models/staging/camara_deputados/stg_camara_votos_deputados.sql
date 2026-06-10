{{ config(
    tags=["stg","camara","votacoes"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_votos_deputados') }}
),

renamed AS (
    SELECT
        deputado__id::INT AS id_deputado,
        {{ clean_string("tipovoto","upper") }} AS voto,
        deputado__idlegislatura AS id_legislatura,
        SPLIT_PART(url_votos, '/', 7) AS id_votacao
    FROM source
)

SELECT * FROM renamed WHERE voto <> 'ARTIGO 17'
