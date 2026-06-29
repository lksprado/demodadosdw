{{ config(
    tags=["camara", "votacoes"]
) }}


WITH
camara_votacoes AS (
    SELECT
        *,
        'CAMARA' AS casa
    FROM {{ ref('stg_camara_votacoes') }}
),

dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY votacao_id_nk) AS rn
    FROM camara_votacoes
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'votacao_id_nk']) }}    AS sk_votacao,
        casa,
        votacao_id_nk,
        data_votacao,
        sigla_orgao,
        {{ dbt_utils.generate_surrogate_key(['casa', 'proposicao_id_fk']) }} AS sk_proposicao,
        aprovado,
        CAST(TO_CHAR(data_votacao, 'YYYYMMDD') AS INTEGER)                AS sk_data
    FROM dedup
    WHERE rn = 1
    ORDER BY data_votacao DESC
)

SELECT * FROM final
