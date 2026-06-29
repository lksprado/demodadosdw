{{ config(
    tags=["senado", "votacoes"]
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
        {{ dbt_utils.generate_surrogate_key(['casa', 'senador_id_nk','codigo_votacao']) }} AS sk_voto,
        {{ dbt_utils.generate_surrogate_key(['casa', 'senador_id_nk']) }}                  AS sk_parlamentar,
        {{ dbt_utils.generate_surrogate_key(['casa', 'codigo_votacao']) }}                 AS sk_votacao,
        casa,
        senador_id_nk,
        codigo_votacao                                                                     AS votacao_id_nk,
        data_sessao,
        identificacao,
        CASE
            WHEN sigla_voto IN ('SIM', 'SIM - PRESIDENTE ART.48 INCISO XXIII') THEN 'SIM'
            WHEN sigla_voto = 'NAO' THEN 'NAO'
            WHEN sigla_voto = 'ABSTENCAO' THEN 'ABSTENCAO'
            WHEN sigla_voto IN ('OBSTRUCAO', 'P-OD') THEN 'OBSTRUCAO'
        END                                                                                     AS voto,
        CAST(TO_CHAR(data_sessao, 'YYYYMMDD') AS INTEGER)                                       AS sk_data
    FROM senado_votos
),

votos_filtrados AS (
    SELECT * FROM votos_tratados
    WHERE
        voto IS NOT NULL
        AND voto <> 'ABSTENCAO'
        AND votacao_id_nk IS NOT NULL
)

SELECT * FROM votos_filtrados
