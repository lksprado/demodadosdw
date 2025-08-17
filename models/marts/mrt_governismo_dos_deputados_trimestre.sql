WITH
dim_dep AS (
    SELECT * FROM {{ ref('int_dim_deputados')}}
),

fct_gov_total AS (
    SELECT * FROM {{ ref('int_fct_governismo_deputados_trimestre')}}
),

tab AS (
    SELECT
        t1.sk_deputado,
        t1.nome_eleitoral_atual,
        t1.partido_atual,
        t2.data_trimestre,
        t2.perc_governismo_trimestre
    FROM dim_dep AS t1
    INNER JOIN fct_gov_total AS t2
        ON t1.sk_deputado = t2.sk_deputado
)

SELECT * FROM tab
