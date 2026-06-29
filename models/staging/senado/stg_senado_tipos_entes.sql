{{ config(
    tags=["senado", "legislacao"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_tipos_entes') }}
),

renamed AS (
    SELECT
        nome                             AS nome_ente,
        "siglaTipo"                      AS tipo_ente,
        SPLIT_PART(UPPER(sigla), ' ', 1) AS codigo_ente,
        CASE
            WHEN casa = '-' THEN NULL
            ELSE casa
        END                              AS codigo_casa
    FROM source
    WHERE sigla IS NOT NULL AND sigla <> '-'
    GROUP BY
        codigo_ente,
        nome_ente,
        tipo_ente,
        casa
    ORDER BY codigo_ente
),

dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY codigo_ente ORDER BY codigo_ente, tipo_ente) AS rn
    FROM renamed
)

SELECT
    codigo_ente,
    nome_ente,
    tipo_ente,
    codigo_casa
FROM dedup
WHERE rn = 1
