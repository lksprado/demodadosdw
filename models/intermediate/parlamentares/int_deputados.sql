{{ config(
    tags=["camara","parlamentar"]
) }}

WITH
deputados AS (
    SELECT
        'CAMARA' AS casa,
        id,
        ultimostatus_nomeeleitoral AS nome,
        nomecivil AS nome_completo,
        ufnascimento AS uf,
        CASE
            WHEN sexo = 'M' THEN 'MASCULINO'
            WHEN sexo = 'F' THEN 'FEMININO'
        END AS sexo
    FROM {{ ref('stg_camara_deputados') }}
    ORDER BY id
),

final AS (
    SELECT
        casa,
        id,
        nome,
        nome_completo,
        sexo,
        uf
    FROM deputados
)

SELECT * FROM final
