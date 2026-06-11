{{ config(
    enabled=false,
    tags=["fct","parlamentar"]
) }}

WITH governismo_camara AS (
    SELECT
        sk_parlamentar,
        'camara' AS casa,
        COUNT(*) FILTER (WHERE alinhado_ao_governo IS NOT NULL) AS total_votos,
        COUNT(*) FILTER (WHERE alinhado_ao_governo = TRUE)      AS total_votos_favor_governo,
        COUNT(*) FILTER (WHERE alinhado_ao_governo = FALSE)     AS total_votos_contra_governo
    FROM {{ ref('int_fct_votos_alinhados_camara') }}
    GROUP BY 1, 2
),

governismo_senado AS (
    SELECT
        sk_parlamentar,
        'senado' AS casa,
        COUNT(*) FILTER (WHERE alinhado_ao_governo IS NOT NULL) AS total_votos,
        COUNT(*) FILTER (WHERE alinhado_ao_governo = TRUE)      AS total_votos_favor_governo,
        COUNT(*) FILTER (WHERE alinhado_ao_governo = FALSE)     AS total_votos_contra_governo
    FROM {{ ref('int_fct_votos_senado') }}
    GROUP BY 1, 2
),

todas_casas AS (
    SELECT * FROM governismo_camara
    UNION ALL
    SELECT * FROM governismo_senado
)

SELECT
    sk_parlamentar,
    casa,
    total_votos,
    total_votos_favor_governo,
    total_votos_contra_governo,
    ROUND(
        total_votos_favor_governo::NUMERIC / NULLIF(total_votos, 0) * 100,
        2
    ) AS perc_governismo
FROM todas_casas
