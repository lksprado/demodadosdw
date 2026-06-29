{{ config(
    materialized='incremental',
    unique_key='data_extracao',
    tags=["ecidadania", "participacao"]
) }}

{% if is_incremental() %}

    SELECT * FROM {{ this }}
    WHERE FALSE

{% else %}

WITH source AS (
    SELECT * FROM {{ source('ecidadania','raw_ecidadania_bignumbers') }}
),

renamed AS (
    SELECT
        total_proposicoes_votadas::INT,
        total_pessoas_votaram::INT,
        total_votos_registrados::INT,
        (total_votos_registrados::NUMERIC / NULLIF(total_pessoas_votaram::NUMERIC, 0))::NUMERIC(18, 5) AS votos_por_pessoa,
        TO_DATE(dt_extracao, 'YYYY-MM-DD') AS data_extracao
    FROM source
)

SELECT * FROM renamed

{% endif %}
