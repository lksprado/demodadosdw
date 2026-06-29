{{ config(
    tags=["senado", "parlamentar"]
) }}

WITH
senadores AS (
    SELECT DISTINCT ON (senador_id_nk)
        1                                                AS prioridade,
        senador_id_nk                                    AS parlamentar_id_nk,
        nome,
        nome_completo,
        sexo,
        uf
    FROM {{ ref('stg_senado_senadores') }}
    ORDER BY senador_id_nk
),

senadores_historico AS (
    SELECT DISTINCT ON (senador_id_nk)
        0                                                AS prioridade,
        senador_id_nk                                    AS parlamentar_id_nk,
        nome,
        nome_completo,
        sexo,
        uf
    FROM {{ ref('stg_senado_legislaturas') }}
    ORDER BY senador_id_nk
),

-- MANTEM DADOS MAIS RECENTES DO ORIUNDOS DO ENDPOINT QUE RETORNA SENADORES ATUAIS COM MAIS DETALHES
senadores_completo AS (
    SELECT
        *,
        'SENADO'                                                     AS casa,
        ROW_NUMBER() OVER (PARTITION BY parlamentar_id_nk ORDER BY prioridade DESC) AS rn
    FROM (
        SELECT * FROM senadores
        UNION ALL
        SELECT * FROM senadores_historico
        ORDER BY parlamentar_id_nk
    )
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'parlamentar_id_nk']) }} AS sk_parlamentar,
        casa,
        parlamentar_id_nk,
        nome,
        nome_completo,
        sexo,
        uf
    FROM senadores_completo
    WHERE rn = 1
)

SELECT * FROM final
