{{ config(
    tags=["dim","senado","parlamentar"]
) }}

WITH
de_para AS (
    SELECT sk_parlamentar, id_nk, id_parlamentar_ranking, casa
    FROM {{ ref('int_map_parlamentares') }}
),

senadores AS (
    SELECT DISTINCT ON (id)
        id,
        nome,
        nome_eleitoral,
        email,
        telefone,
        sexo,
        uf_nascimento,
        municipio_nascimento,
        escolaridade,
        link_foto,
        data_carga
    FROM {{ ref('stg_senado_senadores') }}
    ORDER BY id
),

-- dedup: pega 1 linha por senador para dados de pessoa (igual em todos os mandatos)
senadores_historico AS (
    SELECT DISTINCT ON (id)
        id,
        nome,
        nome_eleitoral,
        email,
        telefone,
        sexo,
        uf_nascimento,
        municipio_nascimento,
        escolaridade,
        link_foto,
        data_carga
    FROM {{ ref('stg_senado_legislaturas') }}
    ORDER BY id
),

enrichment AS (
    SELECT * FROM {{ ref('stg_ranking_parlamentares') }}
),

final AS (
    SELECT
        t1.sk_parlamentar,
        COALESCE(t2.nome, t4.nome)                               AS nome,
        COALESCE(t2.nome_eleitoral, t4.nome_eleitoral)           AS nome_eleitoral,
        COALESCE(t2.email, t4.email)                             AS email,
        COALESCE(t2.telefone, t4.telefone)                       AS telefone,
        COALESCE(t2.sexo, t4.sexo)                               AS sexo,
        t3.data_nascimento::DATE                                  AS data_nascimento,
        COALESCE(t2.uf_nascimento, t4.uf_nascimento)             AS uf_nascimento,
        COALESCE(t2.municipio_nascimento, t4.municipio_nascimento) AS municipio_nascimento,
        COALESCE(t2.escolaridade, t4.escolaridade)               AS escolaridade,
        t3.redesocial_x_twitter,
        t3.redesocial_instagram,
        t3.redesocial_facebook,
        t3.redesocial_youtube,
        t3.link_api_oficial,
        COALESCE(t2.link_foto, t4.link_foto)                     AS link_foto,
        COALESCE(t2.data_carga, t4.data_carga)                   AS data_carga,
        CASE
            WHEN t3.data_nascimento IS NULL THEN NULL
            WHEN (t3.data_nascimento)::DATE BETWEEN DATE '1928-01-01' AND DATE '1945-12-31' THEN 'Silenciosa'
            WHEN (t3.data_nascimento)::DATE BETWEEN DATE '1946-01-01' AND DATE '1964-12-31' THEN 'Baby Boomer'
            WHEN (t3.data_nascimento)::DATE BETWEEN DATE '1965-01-01' AND DATE '1980-12-31' THEN 'X'
            WHEN (t3.data_nascimento)::DATE BETWEEN DATE '1981-01-01' AND DATE '1996-12-31' THEN 'Millennial'
            WHEN (t3.data_nascimento)::DATE BETWEEN DATE '1997-01-01' AND DATE '2012-12-31' THEN 'Z'
            WHEN (t3.data_nascimento)::DATE >= DATE '2013-01-01' THEN 'Alpha'
            ELSE 'OUTRA'
        END AS geracao
    FROM de_para AS t1
    LEFT JOIN senadores AS t2
        ON t1.id_nk = t2.id
    LEFT JOIN senadores_historico AS t4
        ON t1.id_nk = t4.id
    LEFT JOIN enrichment AS t3
        ON t1.id_parlamentar_ranking = t3.id_parlamentar_ranking
    WHERE t1.casa = 'senado'
)

SELECT * FROM final
