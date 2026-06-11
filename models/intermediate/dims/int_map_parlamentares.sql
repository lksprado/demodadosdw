{{ config(
    unique_key = 'sk_parlamentar',
    tags = ['dim', 'parlamentar']
) }}
WITH
camara AS (
    SELECT
        id,
        'camara' AS casa
    FROM {{ ref('stg_camara_deputados') }}
),

senado AS (
    SELECT id, 'senado' AS casa FROM {{ ref('stg_senado_senadores') }}
    UNION
    SELECT id, 'senado' AS casa FROM {{ ref('stg_senado_legislaturas') }}
),

parlamento AS (
    SELECT * FROM camara
    UNION ALL
    SELECT * FROM senado
),

ranking AS (
    SELECT DISTINCT ON (id_parlamentar_congresso)
        id_parlamentar_ranking,
        id_parlamentar_congresso
    FROM {{ ref('stg_ranking_parlamentares') }}
    ORDER BY id_parlamentar_congresso
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['id', 'casa']) }} AS sk_parlamentar,
    casa,
    id AS id_nk,
    NULL::INT AS id_parlamentar_radar,
    t2.id_parlamentar_ranking
FROM parlamento AS t1
LEFT JOIN ranking AS t2
    ON t1.id = t2.id_parlamentar_congresso
