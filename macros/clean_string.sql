{% macro clean_string(field, case_style='lower') -%}
    {%- set cleaned = "TRIM(TRANSLATE(" ~ field ~ ", 'àáâãäèéêëìíîïòóôõöùúûüçñýÿÀÁÂÃÄÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÇÑÝ', 'aaaaaeeeeiiiiooooouuuucnyyAAAAAEEEEIIIIOOOOOUUUUCNY'))" -%}
    {%- if case_style == 'upper' -%}UPPER({{ cleaned }}){%- elif case_style == 'initcap' -%}INITCAP({{ cleaned }}){%- elif case_style == 'lower' -%}LOWER({{ cleaned }}){%- else -%}{{ cleaned }}{%- endif -%}
{%- endmacro %}