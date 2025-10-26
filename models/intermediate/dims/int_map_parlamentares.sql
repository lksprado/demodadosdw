{{ config(
    unique_key = 'sk_parlamentar',
    tags = ['intermediate', 'mapping']
) }}
with 
camara as (
    select 
    id,
    'camara' as casa 
    from {{ ref('stg_parlamento__deputados') }}
),
senado as (
    select 
    id,
    'senado' as casa
    from {{ ref('stg_parlamento__senadores') }}
),
parlamento as (
    select * from camara
    union all 
    select * from senado
),
radar as (
    select 
    id_parlamentar_radar,
    id_parlamentar_congresso
    from {{ ref('stg_radarcongresso__parlamentares') }}
),
ranking as (
    select 
    id_parlamentar_ranking,
    id_parlamentar_congresso
    from {{ ref('stg_ranking__parlamentares') }}
)
select 
{{ dbt_utils.generate_surrogate_key(['id', 'casa']) }} as sk_parlamentar,
casa,
id as id_nk,
t2.id_parlamentar_radar,
t3.id_parlamentar_ranking
from parlamento t1 
left join radar t2 
on t1.id = t2.id_parlamentar_congresso
left join ranking t3
on t1.id = t3.id_parlamentar_congresso
