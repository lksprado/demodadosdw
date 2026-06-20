{{ config(
    tags=["senado","votacoes"]
) }}


WITH
senado_votos AS (
    SELECT
        *,
        'SENADO' AS casa
    FROM {{ ref('stg_senado_votos_senadores') }}
),

votos_tratados AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['codigo_parlamentar','codigo_votacao', 'casa']) }} AS sk_voto,
        {{ dbt_utils.generate_surrogate_key(['casa', 'codigo_parlamentar']) }} AS sk_parlamentar,
        {{ dbt_utils.generate_surrogate_key(['codigo_votacao', 'casa']) }} AS sk_votacao,
        casa,
        codigo_parlamentar AS id_senador,
        codigo_votacao AS id_votacao,
        data_sessao,
        identificacao,
        CASE
            WHEN sigla_voto IN ('SIM', 'SIM - PRESIDENTE ART.48 INCISO XXIII') THEN 'SIM'
            WHEN sigla_voto = 'NAO' THEN 'NAO'
            WHEN sigla_voto = 'ABSTENCAO' THEN 'ABSTENCAO'
            WHEN sigla_voto IN ('OBSTRUCAO', 'P-OD') THEN 'OBSTRUCAO'
        END AS voto
    FROM senado_votos
),

votos_filtrados AS (
    SELECT * FROM votos_tratados
    WHERE voto IS NOT NULL
        AND voto <> 'ABSTENCAO'
        AND id_votacao IS NOT NULL
)

SELECT * FROM votos_filtrados
