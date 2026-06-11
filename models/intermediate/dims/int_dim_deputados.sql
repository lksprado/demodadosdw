{{ config(
    tags=["dim","camara","parlamentar"]
) }}

WITH
de_para AS (
    SELECT sk_parlamentar, id_nk, id_parlamentar_ranking, casa
    FROM {{ ref('int_map_parlamentares') }}
),

deputados AS (
    SELECT * FROM {{ ref('stg_camara_deputados') }}
),

enrichment AS (
    SELECT * FROM {{ ref('stg_ranking_parlamentares') }}
),

final AS (
    SELECT
        t1.sk_parlamentar,
        t2.nome,
        t2.nome_eleitoral,
        t2.email,
        t2.telefone,
        t2.sexo,
        t2.data_nascimento,
        t2.geracao,
        t2.uf_nascimento,
        t2.municipio_nascimento,
        t2.escolaridade,
        t2.link_api_oficial,
        t2.link_foto,
        CASE WHEN t2.redesocial_x_twitter IS NULL THEN t3.redesocial_x_twitter ELSE t2.redesocial_x_twitter END AS redesocial_x_twitter,
        CASE WHEN t2.redesocial_instagram IS NULL  THEN t3.redesocial_instagram  ELSE t2.redesocial_instagram  END AS redesocial_instagram,
        CASE WHEN t2.redesocial_facebook IS NULL   THEN t3.redesocial_facebook   ELSE t2.redesocial_facebook   END AS redesocial_facebook,
        CASE WHEN t2.redesocial_youtube IS NULL    THEN t3.redesocial_youtube    ELSE t2.redesocial_youtube    END AS redesocial_youtube,
        t2.data_carga
    FROM de_para AS t1
    LEFT JOIN deputados AS t2
        ON t1.id_nk = t2.id
    LEFT JOIN enrichment AS t3
        ON t1.id_parlamentar_ranking = t3.id_parlamentar_ranking
    WHERE t1.casa = 'camara'
)

SELECT * FROM final
