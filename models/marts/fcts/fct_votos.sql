WITH
votos_deputados AS (
    SELECT
        sk_voto,
        sk_parlamentar,
        COALESCE(sk_votacao, '{{ var('null_key') }}') AS sk_votacao,
        casa,
        voto
    FROM {{ ref('int_votos_camara_filtrados') }}
),

votos_senadores AS (
    SELECT
        sk_voto,
        sk_parlamentar,
        COALESCE(sk_votacao, '{{ var('null_key') }}') AS sk_votacao,
        casa,
        voto
    FROM {{ ref('int_votos_senado_filtrados') }}
),

votos_parlamentares AS (
    SELECT * FROM votos_deputados
    UNION ALL
    SELECT * FROM votos_senadores
),

-- roteia sk_parlamentar sem correspondencia na dim (orfao historico ou NULL) para a linha dummy
final AS (
    SELECT
        v.sk_voto,
        COALESCE(p.sk_parlamentar, '{{ var('null_key') }}') AS sk_parlamentar,
        v.sk_votacao,
        v.casa,
        v.voto
    FROM votos_parlamentares v
    LEFT JOIN {{ ref('dim_parlamentares') }} p ON v.sk_parlamentar = p.sk_parlamentar
)

SELECT * FROM final
