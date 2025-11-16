{{ config(
    tags=["stg","ecidadania","senado"]
) }}


WITH source AS (
    SELECT * FROM {{ source('ecidadania','raw_ecidadania_paginas') }}
),
renamed AS (
    SELECT
    to_date(dt_extracao,'YYYY-MM-DD') as data_extracao,
    {{ dbt_utils.generate_surrogate_key(['titulo']) }} as sk_proposicao,
    titulo as id_proposicao,
    tipo_proposicao,
    descritivo as ementa,
    votos_sim,
    votos_nao,
    (votos_sim + votos_nao) as total_votos,
    link
    FROM source
    where total_votos is not null and total_votos > 0
),
votos_agrupados AS (
    select
    id_proposicao,
    sum(votos_sim) as vt_sim,
    sum(votos_nao) as vt_nao,
    sum(total_votos) as total_vt
    from renamed
    group by id_proposicao
),
linha_representativa as (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY id_proposicao ORDER BY total_votos DESC) AS rn
        FROM renamed
    ) sub
    where rn=1
)
select 
t1.data_extracao,
split_part(upper(t1.id_proposicao), '/', 2)::int as ano_proposicao,
t1.sk_proposicao,
t1.id_proposicao,
t1.ementa,
t2.vt_sim::int as votos_sim,
t2.vt_nao::int as votos_nao,
t2.total_vt::int as total_votos,
case 
    when t2.vt_sim > t2.vt_nao then 'A favor'
    when t2.vt_nao > t2.vt_sim then 'Contra'
    when t2.vt_nao = t2.vt_sim then 'Empate'
end as vontade_popular,
t1.link
FROM linha_representativa t1
JOIN votos_agrupados t2 
on t1.id_proposicao = t2.id_proposicao
