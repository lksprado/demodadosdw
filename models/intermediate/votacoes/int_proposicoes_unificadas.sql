{{ config(
    tags=["camara", "legislacao"]
) }}


WITH
camara_proposicoes AS (
    SELECT
        'CAMARA'                               AS casa,
        proposicao_id_nk                       AS id,
        {{ clean_string ("t2.nome","upper") }} AS tipo_proposicao,
        data_proposicao
    FROM {{ ref('stg_camara_proposicoes') }} AS t1
    LEFT JOIN {{ ref('raw_camara_tipos_proposicao') }} AS t2
        ON t1.codigo_tipo = t2.cod
),

senado_proposicoes AS (
    SELECT
        'SENADO'                                    AS casa,
        processo_id_nk                              AS id,
        {{ clean_string ("t2.descricao","upper") }} AS tipo_proposicao,
        data_apresentacao                           AS data_proposicao
    FROM {{ ref('stg_senado_processos') }} AS t1
    LEFT JOIN {{ ref('raw_senado_tipos_projetos') }} AS t2
        ON t1.codigo_tipo = t2.sigla
),

proposicoes AS (
    SELECT * FROM camara_proposicoes
    UNION ALL
    SELECT * FROM senado_proposicoes
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'id']) }} AS sk_proposicao,
        casa,
        id as proposicao_id_nk,
        coalesce(tipo_proposicao, '{{ var("null_string") }}') as tipo_proposicao,
        data_proposicao,
        CAST(TO_CHAR(data_proposicao, 'YYYYMMDD') AS INTEGER)  AS sk_data
    FROM proposicoes
),

-- A origem traz múltiplas linhas para o mesmo (casa, id): linhas parciais (sem data),
-- linhas corrompidas no CSV (tipo 'desconhecido') e a linha completa. Mantemos uma
-- linha por sk_proposicao, escolhendo a mais completa: data mais recente vence; em
-- empate, o tipo válido vence sobre 'desconhecido'.
deduplicada AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sk_proposicao
            ORDER BY
                data_proposicao DESC NULLS LAST,
                (tipo_proposicao <> '{{ var("null_string") }}') DESC
        ) AS rn
    FROM final
)

SELECT
    sk_proposicao,
    casa,
    proposicao_id_nk,
    tipo_proposicao,
    data_proposicao,
    sk_data
FROM deduplicada
WHERE rn = 1
