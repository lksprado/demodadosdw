{% macro dummy_row(columns) %}
{#-
    Gera a linha dummy (unknown member) de uma dimensão, garantindo que toda
    LEFT JOIN sem correspondência (FK = '{{ var("null_key") }}' nas facts) caia
    em uma linha conhecida em vez de NULL.

    Uso:
        SELECT * FROM final
        UNION ALL
        {{ dummy_row([
            ['sk_entidade', 'sk'],
            ['atributo_texto', 'text'],
            ['atributo_data', 'null::date'],
            ['metrica', 'null::numeric'],
        ]) }}

    `columns`: lista de pares [nome_coluna, tipo], na MESMA ordem do SELECT final.
    O tipo controla o valor gerado:
      - 'sk'   -> '{{ var("null_key") }}'    (chave substituta desconhecida)
      - 'text' -> '{{ var("null_string") }}' (atributo texto desconhecido)
      - qualquer outra string é usada como expressão SQL literal, ex.:
        'null::int', 'null::date', '1' (sk_data), 'null::numeric' (métricas)
-#}
    SELECT
    {% for col in columns -%}
        {%- if col[1] == 'sk' -%}
            '{{ var("null_key") }}'
        {%- elif col[1] == 'text' -%}
            '{{ var("null_string") }}'
        {%- else -%}
            {{ col[1] }}
        {%- endif %} AS {{ col[0] }}{{ "," if not loop.last }}
    {% endfor -%}
{% endmacro %}
