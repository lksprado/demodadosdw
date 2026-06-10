{{ config(
    tags=["stg","ecidadania"]
) }}


WITH source AS (
    SELECT * FROM {{ source('ecidadania','raw_ecidadania_paginas') }}
),

renamed AS (
    SELECT
        TO_DATE(dt_extracao, 'YYYY-MM-DD') AS data_extracao,
        {{ dbt_utils.generate_surrogate_key(['titulo']) }} AS sk_proposicao,
        titulo AS id_proposicao,
        tipo_proposicao,
        descritivo AS ementa,
        votos_sim,
        votos_nao,
        (votos_sim + votos_nao) AS total_votos,
        link
    FROM source
    WHERE total_votos IS NOT NULL AND total_votos > 0
),

votos_agrupados AS (
    SELECT
        id_proposicao,
        SUM(votos_sim) AS vt_sim,
        SUM(votos_nao) AS vt_nao,
        SUM(total_votos) AS total_vt
    FROM renamed
    GROUP BY id_proposicao
),

linha_representativa AS (
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id_proposicao ORDER BY total_votos DESC) AS rn
        FROM renamed
    ) AS sub
    WHERE rn = 1
)

SELECT
    t1.data_extracao,
    t1.sk_proposicao,
    t1.id_proposicao,
    t1.ementa,
    t1.link,
    CAST(SPLIT_PART(UPPER(t1.id_proposicao), '/', 2) AS INT) AS ano_proposicao,
    SPLIT_PART(UPPER(t1.id_proposicao), ' ', 1) AS sigla_proposicao,
    CAST(t2.vt_sim AS INT) AS votos_sim,
    CAST(t2.vt_nao AS INT) AS votos_nao,
    CAST(t2.total_vt AS INT) AS total_votos,
    CASE
        WHEN t2.vt_sim > t2.vt_nao THEN 'A FAVOR'
        WHEN t2.vt_nao > t2.vt_sim THEN 'CONTRA'
        WHEN t2.vt_nao = t2.vt_sim THEN 'EMPATE'
    END AS vontade_popular
FROM linha_representativa AS t1
INNER JOIN votos_agrupados AS t2
    ON t1.id_proposicao = t2.id_proposicao
