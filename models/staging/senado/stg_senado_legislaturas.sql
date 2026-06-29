{{ config(
    tags=["senado", "parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_legislaturas') }}
),

renamed AS (
    SELECT
        identificacaoparlamentar_codigoparlamentar                                    AS senador_id_nk,
        {{ clean_string("identificacaoparlamentar_nomeparlamentar","upper") }}         AS nome,
        {{ clean_string("identificacaoparlamentar_nomecompletoparlamentar","upper") }} AS nome_completo,
        UPPER(identificacaoparlamentar_sexoparlamentar)                                AS sexo,
        UPPER(identificacaoparlamentar_formatratamento)                                AS forma_tratamento,
        mandatos_mandato,
        identificacaoparlamentar_emailparlamentar                                     AS email,
        identificacaoparlamentar_siglapartidoparlamentar                              AS sigla_partido,
        identificacaoparlamentar_codigopubliconalegatual::INT                          AS codigo_publico_na_leg_atual,
        identificacaoparlamentar_ufparlamentar                                        AS uf
    {# DESCONSIDERADOS 
    identificacaoparlamentar_urlfotoparlamentar
    identificacaoparlamentar_urlpaginaparlamentar
    identificacaoparlamentar_urlpaginaparticular
     #}
    FROM source
)

SELECT * FROM renamed
