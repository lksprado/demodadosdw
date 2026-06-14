WITH
votos_deputados AS (
    SELECT
        sk_voto,
        sk_parlamentar,
        sk_votacao,
        casa,
        voto
    FROM {{ ref('int_votos_camara') }}
),

votos_senadores AS (
    SELECT
        sk_voto,
        sk_parlamentar,
        sk_votacao,
        casa,
        voto
    FROM {{ ref('int_votos_senado') }}
),

votos_parlamentares AS (
    SELECT * FROM votos_deputados
    UNION ALL
    SELECT * FROM votos_senadores
)

SELECT * FROM votos_parlamentares
