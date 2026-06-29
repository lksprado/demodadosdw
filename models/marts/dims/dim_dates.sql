{{ config(
    tags=["datas"]
) }}

with
date_dimension as (
    select * from {{ ref("int_dates") }}
),
final as (
    select 
    cast(to_char(date_day, 'YYYYMMDD') as integer) as data_sk,
    *
    from date_dimension
)
select * from final