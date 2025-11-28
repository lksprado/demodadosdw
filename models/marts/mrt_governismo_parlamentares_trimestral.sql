{{ config(
    tags=["mrt","radar","parlamentar"]
) }}

WITH
dim_parl AS (
    SELECT * FROM {{ ref('int_dim_parlamentares')}}
),

fct_gov_deps AS (
    SELECT * FROM {{ ref('int_fct_governismo_deputados_trimestre')}}
),
fct_gov_sens AS (
    SELECT * FROM {{ ref('int_fct_governismo_senadores_trimestre')}}
),
fct_union as (
    select * from fct_gov_deps
    union
    select * from fct_gov_sens
),
tab AS (
    SELECT
        t1.sk_parlamentar,
        t1.casa,
        t1.nome_eleitoral,
        t1.partido,
        t1.uf_representacao,
        t2.data_trimestre,
        t2.perc_governismo_trimestre
    FROM dim_parl AS t1
    INNER JOIN fct_union AS t2
        ON t1.sk_parlamentar = t2.sk_parlamentar
    where t2.perc_governismo_trimestre is not null
)
SELECT * FROM tab
