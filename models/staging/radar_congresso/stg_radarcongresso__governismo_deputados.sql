WITH source AS (
    SELECT *
    FROM {{ source('radar','raw_radar_governismo_deputados')}}
),

renamed AS (
    SELECT
        id::int AS id_parlamentar_radar,
        afavor::int AS total_votos_favor_governo,
        n::int AS total_votos_contra_governo,
        total::int AS perc_governismo,
        to_date(trimestre, 'YYYY-MM-DD') AS data_trimestre,
        perc_governismo::int AS perc_governismo_trimestre,
        data_carga
    FROM source
)

SELECT * FROM renamed
