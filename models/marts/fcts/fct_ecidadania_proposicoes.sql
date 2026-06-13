{{ config(
    tags=["fct","ecidadania"]
) }}


WITH
proposicoes AS (
    SELECT * FROM {{ ref('stg_ecidadania_paginas') }}
),

mais_votados AS (
    SELECT * FROM {{ ref('stg_ecidadania_mais_votados') }}
)

SELECT
    t1.*,
    COALESCE(t2.sk_proposicao IS NOT NULL, FALSE)::BOOLEAN AS flag_mais_votado_no_dia
FROM proposicoes AS t1
LEFT JOIN mais_votados AS t2
    ON t1.sk_proposicao = t2.sk_proposicao AND t1.data_extracao = t2.data_extracao
