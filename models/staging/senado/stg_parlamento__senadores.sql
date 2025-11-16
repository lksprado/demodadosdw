{{ config(
    tags=["stg","senado","parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('senado','raw_parlamento_senadores') }}
),
renamed AS (
    SELECT
    identificacaoparlamentar_codigoparlamentar as id,
    identificacaoparlamentar_nomecompletoparlamentar as nome,
    identificacaoparlamentar_nomeparlamentar as nome_eleitoral,
    identificacaoparlamentar_siglapartidoparlamentar as partido,
    identificacaoparlamentar_emailparlamentar as email,
    null as redesocial_x_twitter,
    null as redesocial_instagram,
    null as redesocial_facebook,
    null as redesocial_youtube,
    (REPLACE(identificacaoparlamentar_telefones_telefone,'''','"')::jsonb -> 0 ->> 'NumeroTelefone') as telefone,
    CASE 
        WHEN identificacaoparlamentar_sexoparlamentar LIKE 'MASCULINO' then 'M'
        WHEN identificacaoparlamentar_sexoparlamentar LIKE 'FEMININO' then 'F'
    END AS sexo,
    null as data_nascimento,
    null as geracao,
    null as uf_nascimento,
    null as municipio_nascimento,
    identificacaoparlamentar_ufparlamentar as uf_representacao,
    null as escolaridade,    
    mandato_primeiralegislaturadomandato_numerolegislatura as id_legislatura,
    to_date(cast(mandato_primeiralegislaturadomandato_datainicio AS text),'YYYYMMDD') as data_posse,
    null as num_gabinete_sala,
    null as num_gabinete_predio,
    null as num_gabinete_andar,
    'EXERCICIO' AS situacao_atual,
    CASE 
        WHEN mandato_descricaoparticipacao like 'TITULAR' then mandato_descricaoparticipacao
        ELSE 'SUPLENTE'
    END AS condicao_eleitoral,
    identificacaoparlamentar_urlfotoparlamentar AS link_foto,
    data_carga
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
select * from renamed
