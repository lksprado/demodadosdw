WITH
votacoes_camara AS (
    SELECT
        sk_votacao,
        casa,
        data_votacao,
        proposicao_objeto AS objeto,
        aprovado
    FROM {{ ref('int_votacoes_camara') }}
),

votacoes_senado AS (
    SELECT
        sk_votacao,
        casa,
        data_sessao AS data_votacao,
        identificacao AS objeto,
        aprovado
    FROM {{ ref('int_votacoes_senado') }}
),

final AS (
    SELECT * FROM votacoes_camara
    UNION ALL
    SELECT * FROM votacoes_senado
)

SELECT * FROM final
