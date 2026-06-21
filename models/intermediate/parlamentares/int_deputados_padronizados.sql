{{ config(
    tags=["camara","parlamentar"]
) }}

WITH
deputados AS (
    SELECT DISTINCT ON (id)
        1 AS prioridade,
        id,
        ultimostatus_nomeeleitoral AS nome,
        nomecivil AS nome_completo,
        CASE
            WHEN sexo = 'M' THEN 'MASCULINO'
            WHEN sexo = 'F' THEN 'FEMININO'
        END AS sexo,
        ufnascimento AS uf
    FROM {{ ref('stg_camara_deputados') }}
    ORDER BY id
),

deputados_historico AS (
    SELECT DISTINCT ON (id_deputado)
        0 AS prioridade,
        id_deputado AS id,
        nome,
        NULL AS nome_completo,
        NULL AS sexo,
        uf
    FROM {{ ref('stg_camara_legislaturas') }}
    ORDER BY id_deputado
),

-- MANTEM DADOS MAIS COMPLETOS DO ENDPOINT DE DETALHES; LEGISLATURAS PREENCHE DEPUTADOS HISTORICOS AUSENTES
deputados_completo AS (
    SELECT
        *,
        'CAMARA' AS casa,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY prioridade DESC) AS rn
    FROM (
        SELECT * FROM deputados
        UNION ALL
        SELECT * FROM deputados_historico
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
    FROM deputados_completo
    WHERE rn = 1
)

SELECT * FROM final
