{{ config(
    tags=["prs","parlamentar"]
) }}

/*
  Grão: um parlamentar × legislatura.
  Agrega os votos de prs_governismo para produzir o percentual bruto de governismo
  por parlamentar em cada legislatura, separado por casa legislativa.

  Limitação: perc_governismo tem alta variância para parlamentares com poucos votos —
  um deputado com 3 votos e 100% de alinhamento parece mais governista do que um com
  300 votos e 70%, o que pode distorcer rankings. Para análises comparativas use
  prs_governismo_por_deputado_legislatura_ajustado, que aplica média bayesiana.
*/

WITH governismo AS (
    SELECT
        casa,
        sk_parlamentar,
        legislatura,
        COUNT(sk_voto) AS qt_votos,
        COUNT(DISTINCT sk_votacao) AS qt_votacoes,
        COUNT(sk_voto) FILTER (WHERE voto_alinhado = 1) AS qt_votos_alinhados,
        COUNT(sk_voto) FILTER (WHERE voto_alinhado = 0) AS qt_votos_nao_alinhados,
        ROUND(100.0 * COUNT(sk_voto) FILTER (WHERE voto_alinhado = 1)::NUMERIC / NULLIF(COUNT(sk_voto), 0),2) AS perc_governismo
    FROM {{ ref('prs_governismo') }}
    GROUP BY
        casa,
        sk_parlamentar,
        legislatura
)
SELECT *
FROM governismo