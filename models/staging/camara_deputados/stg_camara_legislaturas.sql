{{ config(
    tags=["stg","camara","parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_legislaturas') }}
),

renamed AS (
    SELECT
        idlegislatura AS id,
        id AS id_deputado
    FROM source
    WHERE nome IS NOT NULL
)

SELECT * FROM renamed
