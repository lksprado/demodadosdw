{{ config(
    tags=["dim","parlamentar"]
) }}

WITH senadores AS (
    SELECT * FROM {{ ref('int_dim_senadores') }}
),

deputados AS (
    SELECT * FROM {{ ref('int_dim_deputados') }}
)

SELECT
    sk_parlamentar,
    nome,
    nome_eleitoral,
    email,
    telefone,
    sexo,
    data_nascimento,
    geracao,
    uf_nascimento,
    municipio_nascimento,
    escolaridade,
    redesocial_x_twitter,
    redesocial_instagram,
    redesocial_facebook,
    redesocial_youtube,
    link_api_oficial,
    link_foto,
    data_carga
FROM senadores

UNION ALL

SELECT
    sk_parlamentar,
    nome,
    nome_eleitoral,
    email,
    telefone,
    sexo,
    data_nascimento,
    geracao,
    uf_nascimento,
    municipio_nascimento,
    escolaridade,
    redesocial_x_twitter,
    redesocial_instagram,
    redesocial_facebook,
    redesocial_youtube,
    link_api_oficial,
    link_foto,
    data_carga
FROM deputados
