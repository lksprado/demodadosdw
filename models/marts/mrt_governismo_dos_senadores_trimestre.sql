WITH
dim_dep AS (
    SELECT * FROM {{ ref('int_dim_senadores')}}
),

fct_gov_total AS (
    SELECT * FROM {{ ref('int_fct_governismo_senadores_trimestre')}}
),

tab AS (
    SELECT
        t1.sk_parlamentar,
        t1.nome_eleitoral_atual,
        t1.partido_atual,
        t2.data_trimestre,
        t2.perc_governismo_trimestre
    FROM dim_dep AS t1
    INNER JOIN fct_gov_total AS t2
        ON t1.sk_parlamentar = t2.sk_parlamentar
    where t2.perc_governismo_trimestre is not null
)

SELECT * FROM tab
