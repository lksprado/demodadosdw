{{ config(
    tags=["camara", "senado", "votacoes"]
) }}

WITH
votos_deputados AS (
    SELECT
        sk_voto,
        sk_parlamentar,
        sk_votacao,
        votacao_id_fk AS votacao_id_nk,
        casa,
        partido,
        partido_nome,
        voto
    FROM {{ ref('int_votos_camara_filtrados') }}
),

votos_senadores AS (
    SELECT
        sk_voto,
        sk_parlamentar,
        sk_votacao,
        votacao_id_nk::TEXT AS votacao_id_nk,
        casa,
        partido,
        partido_nome,
        voto
    FROM {{ ref('int_votos_senado_filtrados') }}
),

votos_parlamentares AS (
    SELECT * FROM votos_deputados
    UNION ALL
    SELECT * FROM votos_senadores
),

final AS (
    SELECT
        sk_voto,
        sk_parlamentar,
        sk_votacao,
        votacao_id_nk,
        casa,
        partido,
        partido_nome,
        voto
    FROM votos_parlamentares
)

SELECT * FROM final
