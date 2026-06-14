{{ config(
    tags=["stg","senado","parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_legislaturas') }}
),

renamed AS (
    SELECT
        identificacaoparlamentar_codigoparlamentar,
        {{ clean_string("identificacaoparlamentar_nomeparlamentar","upper") }} AS identificacaoparlamentar_nomeparlamentar,
        {{ clean_string("identificacaoparlamentar_nomecompletoparlamentar","upper") }} AS identificacaoparlamentar_nomecompletoparlamentar,
        UPPER(identificacaoparlamentar_sexoparlamentar) AS identificacaoparlamentar_sexoparlamentar,
        UPPER(identificacaoparlamentar_formatratamento) AS identificacaoparlamentar_formatratamento,
        mandatos_mandato,
        identificacaoparlamentar_emailparlamentar,
        identificacaoparlamentar_siglapartidoparlamentar,
        identificacaoparlamentar_codigopubliconalegatual::INT AS identificacaoparlamentar_codigopubliconalegatual,
        identificacaoparlamentar_ufparlamentar
    {# DESCONSIDERADOS 
    identificacaoparlamentar_urlfotoparlamentar
    identificacaoparlamentar_urlpaginaparlamentar
    identificacaoparlamentar_urlpaginaparticular
     #}
    FROM source
)

SELECT * FROM renamed
