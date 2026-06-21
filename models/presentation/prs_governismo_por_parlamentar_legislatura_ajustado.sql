{{ config(
    tags=["prs","parlamentar"]
) }}

/*
  Grão: um parlamentar × legislatura.
  Estende prs_governismo_por_deputado_legislatura com score_governismo_ponderado,
  um ajuste bayesiano que corrige a instabilidade do percentual bruto em parlamentares
  com baixa participação.

  Fórmula (média bayesiana):
    score = (qt_votos × perc_governismo + C × μ) / (qt_votos + C)

  onde:
    C = média de votos dos parlamentares da mesma casa e legislatura (peso de confiança)
    μ = média de perc_governismo da mesma casa e legislatura (prior)

  Comportamento esperado:
    - Poucos votos  → score puxado em direção à média da casa/legislatura (menor confiança)
    - Muitos votos  → score converge para perc_governismo real (maior confiança)

  Isso garante que parlamentares com alta participação e baixo alinhamento sejam
  ranqueados abaixo de parlamentares igualmente alinhados com mais votos — tornando
  o ranking mais resistente a outliers de baixa amostragem.
*/

WITH governismo_por_parlamentar_legislatura AS (
    SELECT *  FROM {{ ref('prs_governismo_por_parlamentar_legislatura') }}
),
scored AS (
    SELECT
        *,
        AVG(qt_votos)        OVER (PARTITION BY legislatura, casa) AS _prior_votos,
        AVG(perc_governismo) OVER (PARTITION BY legislatura, casa) AS _prior_governismo
    FROM governismo_por_parlamentar_legislatura
)
SELECT
    casa,
    sk_parlamentar,
    deputado_id,
    senador_id,
    legislatura,
    qt_votos,
    qt_votacoes,
    qt_votos_alinhados,
    qt_votos_nao_alinhados,
    perc_governismo,
    ROUND(
        (qt_votos * perc_governismo + _prior_votos * _prior_governismo)
        / NULLIF(qt_votos + _prior_votos, 0)
    , 2) AS score_governismo_ponderado
FROM scored
