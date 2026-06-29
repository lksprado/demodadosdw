{{ config(
    tags=["datas"]
) }}

{{ dbt_date.get_date_dimension("1900-01-01", "2050-12-31") }}
