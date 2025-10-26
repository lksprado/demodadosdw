WITH source AS (
    SELECT * 
    FROM {{ source('radar','raw_radar_parlamentares')}}
),
renamed AS (
    SELECT
        idparlamentarvoz::int AS id_parlamentar_radar,
        idparlamentar::int AS id_parlamentar_congresso,
        nomeeleitoral as nome_eleitoral,
        uf,
        emexercicio as is_ativo,
        case 
            when casa like 'camara' then 'deputado'
            when casa like 'senado' then 'senador'
        end as tipo_mandato,
        parlamentarpartido as partido_dict,
        nomeprocessado as nome_completo,
        arquivo_origem,
        data_carga
    FROM source
)
SELECT * FROM renamed
