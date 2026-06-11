{{ config(
    tags=["stg","senado","parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_senadores') }}
),

renamed AS (
    SELECT
        identificacaoparlamentar_codigoparlamentar AS id,
        identificacaoparlamentar_nomecompletoparlamentar AS nome,
        identificacaoparlamentar_nomeparlamentar AS nome_eleitoral,
        identificacaoparlamentar_siglapartidoparlamentar AS partido,
        identificacaoparlamentar_emailparlamentar AS email,
        NULL AS redesocial_x_twitter,
        NULL AS redesocial_instagram,
        NULL AS redesocial_facebook,
        NULL AS redesocial_youtube,
        NULL AS data_nascimento,
        NULL AS geracao,
        NULL AS uf_nascimento,
        NULL AS municipio_nascimento,
        identificacaoparlamentar_ufparlamentar AS uf_representacao,
        NULL AS escolaridade,
        NULL::INT AS id_legislatura,
        NULL AS num_gabinete_sala,
        NULL AS num_gabinete_predio,
        NULL AS num_gabinete_andar,
        NULL AS situacao_atual,
        identificacaoparlamentar_urlfotoparlamentar AS link_foto,
        data_carga,
        NULL AS telefone,
        CASE
            WHEN UPPER(identificacaoparlamentar_sexoparlamentar) LIKE 'MASCULINO' THEN 'M'
            WHEN UPPER(identificacaoparlamentar_sexoparlamentar) LIKE 'FEMININO' THEN 'F'
        END AS sexo,
        NULL::DATE data_posse,
        NULL AS condicao_eleitoral
    -- identificacaoparlamentar_urlpaginaparlamentar as url_website,    
    --identificacaoparlamentar_bloco_codigobloco as bloco_atual,
    --identificacaoparlamentar_bloco_nomebloco as bloco_atual,
    --identificacaoparlamentar_bloco_nomeapelido
    --identificacaoparlamentar_bloco_datacriacao
    --identificacaoparlamentar_membromesa
    --identificacaoparlamentar_membrolideranca as is_lideranca,
    --mandato_codigomandato
    --mandato_primeiralegislaturadomandato_datafim
    --mandato_segundalegislaturadomandato_numerolegislatura
    --mandato_segundalegislaturadomandato_datainicio
    --to_date(mandato_segundalegislaturadomandato_datafim,'YYYYMMDD') as data_fim_mandato,
    --mandato_suplentes_suplente
    --mandato_exercicios_exercicio
    --mandato_titular_descricaoparticipacao
    --mandato_titular_codigoparlamentar
    -- CASE 
    --     WHEN mandato_titular_nomeparlamentar LIKE 'NAN' THEN null
    --     ELSE mandato_titular_nomeparlamentar
    -- END AS nome_titular_mandato,
    --identificacaoparlamentar_urlpaginaparticular
    FROM source
)

SELECT * FROM renamed
