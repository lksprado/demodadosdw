{{ config(
    tags=["camara", "parlamentar"]
) }}

WITH
deputados AS (
    SELECT DISTINCT ON (deputado_id_nk)
        1              AS prioridade,
        deputado_id_nk AS parlamentar_id_nk,
        nome_eleitoral AS nome,
        nome_civil     AS nome_completo,
        CASE
            WHEN sexo = 'M' THEN 'MASCULINO'
            WHEN sexo = 'F' THEN 'FEMININO'
        END            AS sexo,
        uf_nascimento  AS uf
    FROM {{ ref('stg_camara_deputados') }}
    ORDER BY deputado_id_nk
),

deputados_historico AS (
    SELECT DISTINCT ON (deputado_id_fk)
        0              AS prioridade,
        deputado_id_fk AS parlamentar_id_nk,
        nome,
        NULL           AS nome_completo,
        NULL           AS sexo,
        uf
    FROM {{ ref('stg_camara_legislaturas') }}
    ORDER BY deputado_id_fk
),

-- MANTEM DADOS MAIS COMPLETOS DO ENDPOINT DE DETALHES; LEGISLATURAS PREENCHE DEPUTADOS HISTORICOS AUSENTES
deputados_completo AS (
    SELECT
        *,
        'CAMARA'                                                     AS casa,
        ROW_NUMBER() OVER (PARTITION BY parlamentar_id_nk ORDER BY prioridade DESC) AS rn
    FROM (
        SELECT * FROM deputados
        UNION ALL
        SELECT * FROM deputados_historico
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
    FROM deputados_completo
    WHERE rn = 1
)

SELECT * FROM final
