WITH
votacoes_camara AS (
    SELECT
        DISTINCT ON (sk_votacao)
        sk_votacao,
        casa,
        data_votacao,
        proposicao_objeto AS objeto,
        aprovado
    FROM {{ ref('int_votacoes_camara_deduplicadas') }}
),

votacoes_senado AS (
    SELECT
        DISTINCT ON (sk_votacao)
        sk_votacao,
        casa,
        data_sessao AS data_votacao,
        identificacao AS objeto,
        aprovado
    FROM {{ ref('int_votacoes_senado_filtradas') }}
),

unioned AS (
    SELECT * FROM votacoes_camara
    UNION ALL
    SELECT * FROM votacoes_senado
),

-- linha dummy para FKs sem correspondência (COALESCE com null_key nas facts cai aqui)
dummy AS (
    SELECT
        '{{ var('null_key') }}'    AS sk_votacao,
        '{{ var('null_string') }}' AS casa,
        NULL::date                 AS data_votacao,
        '{{ var('null_string') }}' AS objeto,
        NULL::int                  AS aprovado
),

final AS (
    SELECT * FROM unioned
    UNION ALL
    SELECT * FROM dummy
)

SELECT * FROM final
