{% macro clean_integer(field) %}
    cast(
        nullif(
            regexp_replace(cast({{ field }} as text), '[^0-9]', '','g'),
            ''
        ) as bigint
    )
{% endmacro %}