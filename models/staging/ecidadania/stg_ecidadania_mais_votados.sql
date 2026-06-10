{{ config(
    tags=["stg","ecidadania"]
) }}


WITH source AS (
    SELECT * FROM {{ source('ecidadania','raw_ecidadania_mais_votados') }}
),

renamed AS (
    SELECT
        TO_DATE(dt_extracao, 'YYYY-MM-DD') AS data_extracao,
        SPLIT_PART(UPPER(titulo), '/', 2)::INT AS ano_proposicao,
        {{ dbt_utils.generate_surrogate_key(['titulo']) }} AS sk_proposicao,
        titulo AS id_proposicao,
        {{ clean_string("tipo_proposicao", "upper") }} AS tipo_proposicao,
        {{ clean_string("descritivo", "upper") }} AS ementa,
        votos_sim::INT,
        votos_nao::INT,
        (votos_sim + votos_nao)::INT AS total_votos,
        CASE
            WHEN votos_sim > votos_nao THEN 'A FAVOR'
            WHEN votos_nao > votos_sim THEN 'CONTRA'
            WHEN votos_nao = votos_sim THEN 'EMPATE'
        END AS vontade_popular,
        link
    FROM source
)

SELECT * FROM renamed
