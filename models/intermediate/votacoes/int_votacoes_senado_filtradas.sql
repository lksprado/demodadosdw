{{ config(
    tags=["senado", "votacoes"]
) }}


WITH
senado_votacoes AS (
    SELECT
        *,
        'SENADO' AS casa
    FROM {{ ref('stg_senado_votacoes') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'codigo_votacao']) }} AS sk_votacao,
        casa,
        codigo_votacao::TEXT                                               AS votacao_id_nk,
        {{ dbt_utils.generate_surrogate_key(['casa', 'processo_id_nk']) }} AS sk_proposicao,
        data_sessao                                                        AS data_votacao,
        identificacao,
        CASE
            WHEN resultado_votacao = 'APROVADO' THEN 1
            ELSE 0
        END                                                                AS aprovado,
        CAST(TO_CHAR(data_sessao, 'YYYYMMDD') AS INTEGER)                  AS sk_data
    FROM senado_votacoes
    WHERE codigo_votacao IS NOT NULL
    ORDER BY data_sessao DESC

)

SELECT * FROM final
