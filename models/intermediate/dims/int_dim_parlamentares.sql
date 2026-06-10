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
    partido,
    casa,
    nome_eleitoral,
    email,
    redesocial_x_twitter,
    redesocial_instagram,
    redesocial_facebook,
    redesocial_youtube,
    telefone,
    sexo,
    data_nascimento,
    geracao,
    uf_nascimento,
    municipio_nascimento,
    uf_representacao,
    escolaridade,
    id_legislatura,
    data_posse,
    num_gabinete_predio,
    num_gabinete_andar,
    num_gabinete_sala,
    situacao_atual,
    condicao_eleitoral,
    link_api_oficial,
    link_foto,
    data_carga
FROM senadores

UNION ALL

SELECT
    sk_parlamentar,
    nome,
    partido,
    casa,
    nome_eleitoral,
    email,
    redesocial_x_twitter,
    redesocial_instagram,
    redesocial_facebook,
    redesocial_youtube,
    telefone,
    sexo,
    data_nascimento,
    geracao,
    uf_nascimento,
    municipio_nascimento,
    uf_representacao,
    escolaridade,
    id_legislatura,
    data_posse,
    num_gabinete_predio,
    num_gabinete_andar,
    num_gabinete_sala,
    situacao_atual,
    condicao_eleitoral,
    link_api_oficial,
    link_foto,
    data_carga
FROM deputados
