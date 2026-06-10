{{ config(
    tags=["dim","senado","parlamentar"]
) }}

WITH
de_para AS (
    SELECT *
    FROM {{ ref('int_map_parlamentares') }}
),

senadores AS (
    SELECT * FROM {{ ref('stg_senado_senadores') }}
),

enrichment AS (
    SELECT * FROM {{ ref('stg_ranking_parlamentares') }}
),

final AS (
    SELECT
        t1.sk_parlamentar,
        t2.nome,
        t2.partido,
        t1.casa,
        t2.nome_eleitoral,
        t2.email,
        t3.redesocial_x_twitter,
        t3.redesocial_instagram,
        t3.redesocial_facebook,
        t3.redesocial_youtube,
        t2.telefone,
        t2.sexo,
        t3.data_nascimento::DATE,
        t2.uf_nascimento,
        t2.municipio_nascimento,
        t2.uf_representacao,
        t2.escolaridade,
        t2.id_legislatura,
        t2.data_posse,
        t2.num_gabinete_predio,
        t2.num_gabinete_andar,
        t2.num_gabinete_sala,
        t2.situacao_atual,
        t2.condicao_eleitoral,
        t3.link_api_oficial,
        t2.link_foto,
        t2.data_carga,
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
    LEFT JOIN enrichment AS t3
        ON t1.id_parlamentar_ranking = t3.id_parlamentar_ranking
    WHERE t1.casa = 'senado'
)

SELECT * FROM final
