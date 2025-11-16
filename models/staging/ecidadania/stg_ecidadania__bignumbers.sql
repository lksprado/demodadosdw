{{ config(
    tags=["stg","ecidadania","senado"]
) }}


WITH source AS (
    SELECT * FROM {{ source('ecidadania','raw_ecidadania_bignumbers') }}
),
renamed AS (
    SELECT
    to_date(dt_extracao,'YYYY-MM-DD') as data_extracao,
    total_proposicoes_votadas::int,
    total_pessoas_votaram::int,
    total_votos_registrados::int,
    ROUND(total_votos_registrados::int / total_pessoas_votaram::int, 3) AS votos_por_pessoa
    FROM source
)
select * from renamed
