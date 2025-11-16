{{ config(
    tags=["mrt","radar","parlamentar"]
) }}


WITH
dim_dep AS (
    SELECT * FROM {{ ref('int_dim_senadores')}}
),

fct_gov_total AS (
    SELECT * FROM {{ ref('int_fct_governismo_senadores_total')}}
),

tab AS (
    SELECT
        t1.sk_parlamentar,
        t1.nome_eleitoral,
        t1.partido,
        t1.uf_representacao,
        t2.perc_governismo
    FROM dim_dep AS t1
    INNER JOIN fct_gov_total AS t2
        ON t1.sk_parlamentar = t2.sk_parlamentar
    where t2.perc_governismo is not null
)

SELECT * FROM tab
