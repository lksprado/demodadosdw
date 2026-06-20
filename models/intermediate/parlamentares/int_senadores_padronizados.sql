{{ config(
    tags=["senado","parlamentar"]
) }}

WITH
senadores AS (
    SELECT DISTINCT ON (id)
        1 AS prioridade,
        identificacaoparlamentar_codigoparlamentar AS id,
        identificacaoparlamentar_nomeparlamentar AS nome,
        identificacaoparlamentar_nomecompletoparlamentar AS nome_completo,
        identificacaoparlamentar_sexoparlamentar AS sexo,
        identificacaoparlamentar_ufparlamentar AS uf
    FROM {{ ref('stg_senado_senadores') }}
    ORDER BY id
),

senadores_historico AS (
    SELECT DISTINCT ON (id)
        0 AS prioridade,
        identificacaoparlamentar_codigoparlamentar AS id,
        identificacaoparlamentar_nomeparlamentar AS nome,
        identificacaoparlamentar_nomecompletoparlamentar AS nome_completo,
        identificacaoparlamentar_sexoparlamentar AS sexo,
        identificacaoparlamentar_ufparlamentar AS uf
    FROM {{ ref('stg_senado_legislaturas') }}
    ORDER BY id
),

-- MANTEM DADOS MAIS RECENTES DO ORIUNDOS DO ENDPOINT QUE RETORNA SENADORES ATUAIS COM MAIS DETALHES
senadores_completo AS (
    SELECT
        *,
        'SENADO' AS casa,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY prioridade DESC) AS rn
    FROM (
        SELECT * FROM senadores
        UNION ALL
        SELECT * FROM senadores_historico
        ORDER BY id
    )
),

final AS (
    SELECT
        casa,
        id,
        nome,
        nome_completo,
        sexo,
        uf
    FROM senadores_completo
    WHERE rn = 1
)

SELECT * FROM final
