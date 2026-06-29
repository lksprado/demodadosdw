{{ config(
    tags=["senado", "parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_senadores') }}
),

renamed AS (
    SELECT
        identificacaoparlamentar_codigoparlamentar AS senador_id_nk,
        identificacaoparlamentar_codigopubliconalegatual AS codigo_publico_na_leg_atual,
        identificacaoparlamentar_nomeparlamentar AS nome,
        identificacaoparlamentar_nomecompletoparlamentar AS nome_completo,
        identificacaoparlamentar_sexoparlamentar AS sexo,
        identificacaoparlamentar_formatratamento AS forma_tratamento,
        identificacaoparlamentar_emailparlamentar AS email,
        identificacaoparlamentar_telefones_telefone AS telefones_telefone,
        identificacaoparlamentar_siglapartidoparlamentar AS sigla_partido,
        identificacaoparlamentar_ufparlamentar AS uf,
        identificacaoparlamentar_bloco_codigobloco AS bloco_codigo_bloco,
        identificacaoparlamentar_bloco_nomebloco AS bloco_nome_bloco,
        identificacaoparlamentar_bloco_nomeapelido AS bloco_nome_apelido,
        identificacaoparlamentar_membromesa AS membro_mesa,
        identificacaoparlamentar_membrolideranca AS membro_lideranca,
        mandato_codigomandato,
        mandato_ufparlamentar,
        mandato_primeiralegislaturadomandato_numerolegislatura,
        mandato_segundalegislaturadomandato_numerolegislatura,
        mandato_descricaoparticipacao,
        mandato_suplentes_suplente,
        mandato_exercicios_exercicio,
        TO_DATE(identificacaoparlamentar_bloco_datacriacao::TEXT, 'YYYYMMDD')      AS identificacaoparlamentar_bloco_datacriacao,
        TO_DATE(mandato_primeiralegislaturadomandato_datainicio::TEXT, 'YYYYMMDD') AS mandato_primeiralegislaturadomandato_datainicio,
        TO_DATE(mandato_primeiralegislaturadomandato_datafim::TEXT, 'YYYYMMDD')    AS mandato_primeiralegislaturadomandato_datafim,
        TO_DATE(mandato_segundalegislaturadomandato_datainicio::TEXT, 'YYYYMMDD')  AS mandato_segundalegislaturadomandato_datainicio,
        TO_DATE(mandato_segundalegislaturadomandato_datafim::TEXT, 'YYYYMMDD')     AS mandato_segundalegislaturadomandato_datafim,
        NULLIF(mandato_titular_descricaoparticipacao, 'NAN')                       AS mandato_titular_descricaoparticipacao,
        NULLIF(mandato_titular_codigoparlamentar, 'NAN')                           AS mandato_titular_codigoparlamentar,
        NULLIF(mandato_titular_nomeparlamentar, 'NAN')                             AS mandato_titular_nomeparlamentar
    {# DESCONSIDERADOS 
    identificacaoparlamentar_urlfotoparlamentar
    identificacaoparlamentar_urlpaginaparlamentar
    identificacaoparlamentar_urlpaginaparticular
     #}
    FROM source
)

SELECT * FROM renamed
