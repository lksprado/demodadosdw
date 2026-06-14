{{ config(
    tags=["stg","senado","parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_senadores') }}
),

renamed AS (
    SELECT
        identificacaoparlamentar_codigoparlamentar,
        identificacaoparlamentar_codigopubliconalegatual,
        identificacaoparlamentar_nomeparlamentar,
        identificacaoparlamentar_nomecompletoparlamentar,
        identificacaoparlamentar_sexoparlamentar,
        identificacaoparlamentar_formatratamento,
        identificacaoparlamentar_emailparlamentar,
        identificacaoparlamentar_telefones_telefone,
        identificacaoparlamentar_siglapartidoparlamentar,
        identificacaoparlamentar_ufparlamentar,
        identificacaoparlamentar_bloco_codigobloco,
        identificacaoparlamentar_bloco_nomebloco,
        identificacaoparlamentar_bloco_nomeapelido,
        identificacaoparlamentar_membromesa,
        identificacaoparlamentar_membrolideranca,
        mandato_codigomandato,
        mandato_ufparlamentar,
        mandato_primeiralegislaturadomandato_numerolegislatura,
        mandato_segundalegislaturadomandato_numerolegislatura,
        mandato_descricaoparticipacao,
        mandato_suplentes_suplente,
        mandato_exercicios_exercicio,
        TO_DATE(identificacaoparlamentar_bloco_datacriacao::TEXT, 'YYYYMMDD') AS identificacaoparlamentar_bloco_datacriacao,
        TO_DATE(mandato_primeiralegislaturadomandato_datainicio::TEXT, 'YYYYMMDD') AS mandato_primeiralegislaturadomandato_datainicio,
        TO_DATE(mandato_primeiralegislaturadomandato_datafim::TEXT, 'YYYYMMDD') AS mandato_primeiralegislaturadomandato_datafim,
        TO_DATE(mandato_segundalegislaturadomandato_datainicio::TEXT, 'YYYYMMDD') AS mandato_segundalegislaturadomandato_datainicio,
        TO_DATE(mandato_segundalegislaturadomandato_datafim::TEXT, 'YYYYMMDD') AS mandato_segundalegislaturadomandato_datafim,
        NULLIF(mandato_titular_descricaoparticipacao, 'NAN') AS mandato_titular_descricaoparticipacao,
        NULLIF(mandato_titular_codigoparlamentar, 'NAN') AS mandato_titular_codigoparlamentar,
        NULLIF(mandato_titular_nomeparlamentar, 'NAN') AS mandato_titular_nomeparlamentar
    {# DESCONSIDERADOS 
    identificacaoparlamentar_urlfotoparlamentar
    identificacaoparlamentar_urlpaginaparlamentar
    identificacaoparlamentar_urlpaginaparticular
     #}
    FROM source
)

SELECT * FROM renamed
