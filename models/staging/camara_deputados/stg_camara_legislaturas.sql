{{ config(
    tags=["camara", "parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_legislaturas') }}
),

renamed AS (
    SELECT
        idlegislatura AS legislatura_id_nk,
        id            AS deputado_id_fk,
        nome,
        siglauf       AS uf
    FROM source
    WHERE nome IS NOT NULL
)

SELECT * FROM renamed
