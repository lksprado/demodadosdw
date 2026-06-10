{{ config(
    enabled=false,
    tags=["mrt","radar","parlamentar"]
) }}

WITH
dim_parl AS (
    SELECT * FROM {{ ref('int_dim_parlamentares')}}
),

fct_gov AS (
    SELECT * FROM {{ ref('int_fct_governismo_parlamentares_total')}}
),
tab AS (
    SELECT
        t1.sk_parlamentar,
        t1.casa,
        t1.nome_eleitoral,
        t1.partido,
        t1.uf_representacao,
        t2.perc_governismo
    FROM dim_parl AS t1
    INNER JOIN fct_gov AS t2
        ON t1.sk_parlamentar = t2.sk_parlamentar
    WHERE t2.perc_governismo IS NOT NULL
)
SELECT * FROM tab
