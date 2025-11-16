{{ config(
    tags=["stg","ecidadania","senado"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_tipos_entes') }}
),
renamed AS (
    SELECT
    split_part(upper(sigla), ' ', 1) as codigo_ente,
    nome as nome_ente,
    "siglaTipo" as tipo_ente,
    CASE 
        WHEN casa = '-' then null
    ELSE casa
    END as codigo_casa
    FROM source
    where sigla is not null and sigla <> '-'
    group BY
    codigo_ente,
    nome_ente,
    tipo_ente,
    casa
    ORDER BY codigo_ente  
),
dedup as (
    select 
    ROW_NUMBER() OVER (PARTITION BY codigo_ente ORDER BY codigo_ente,tipo_ente) as rn,
    *
    from renamed
)
select 
codigo_ente,
nome_ente,
tipo_ente,
codigo_casa 
from dedup 
where rn = 1
