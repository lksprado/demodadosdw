{{ config(
    tags=["stg","ecidadania","senado"]
) }}


WITH source AS (
    SELECT * FROM {{ source('ecidadania','raw_ecidadania_mais_votados') }}
),
renamed AS (
    SELECT
    to_date(dt_extracao,'YYYY-MM-DD') as data_extracao,
    split_part(upper(titulo), '/', 2)::int as ano_proposicao,
    {{ dbt_utils.generate_surrogate_key(['titulo']) }} as sk_proposicao,
    titulo as id_proposicao,
    tipo_proposicao,
    descritivo as ementa,
    votos_sim::int,
    votos_nao::int,
    (votos_sim + votos_nao)::int as total_votos,
    case 
        when votos_sim > votos_nao then 'A favor'
        when votos_nao > votos_sim then 'Contra'
        when votos_nao = votos_sim then 'Empate'
    end as vontade_popular,
    link
    FROM source
)
select * from renamed
