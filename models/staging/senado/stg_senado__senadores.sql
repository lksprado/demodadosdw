{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('senado','parlamento_senadores_raw') }}
),
renamed AS (
    SELECT
    identificacaoparlamentar_codigoparlamentar as id,
    identificacaoparlamentar_nomecompletoparlamentar as nome_civil,
    identificacaoparlamentar_nomeparlamentar as nome_eleitoral_atual,
    --identificacaoparlamentar_codigopubliconalegatual as id_senador_legislatura_atual,
    identificacaoparlamentar_siglapartidoparlamentar as partido_atual,
    identificacaoparlamentar_emailparlamentar as email_gabinete_atual,
    null as redesocial_x_twitter,
    null as redesocial_instagram,
    null as redesocial_facebook,
    null as redesocial_youtube,
    null as redesociais,
    identificacaoparlamentar_urlfotoparlamentar AS url_foto_atual,
    identificacaoparlamentar_urlpaginaparlamentar as url_website,
    mandato_ufparlamentar as uf_nascimento,
    (REPLACE(identificacaoparlamentar_telefones_telefone,'''','"')::jsonb -> 0 ->> 'NumeroTelefone') as telefone_gabinete_atual,
    null as cpf,
    CASE 
        WHEN identificacaoparlamentar_sexoparlamentar LIKE 'MASCULINO' then 'M'
        WHEN identificacaoparlamentar_sexoparlamentar LIKE 'FEMININO' then 'F'
    END AS sexo,
    null as data_nascimento,
    null as data_falecimento,
    null as geracao,
    null as municipio_nascimento,
    identificacaoparlamentar_ufparlamentar as uf_atual,
    null as escolaridade,
    mandato_primeiralegislaturadomandato_numerolegislatura as id_legislatura_atual,
    to_date(mandato_primeiralegislaturadomandato_datainicio,'YYYYMMDD') as posse_data,
    null as numero_gabinete_predio_atual,
    null as numero_gabinete_andar_atual,
    null as numero_gabinete_sala_atual,
    'EXERCICIO' AS situacao_atual,
    CASE 
        WHEN mandato_descricaoparticipacao like 'TITULAR' then mandato_descricaoparticipacao
        ELSE 'SUPLENTE'
    END AS condicao_eleitoral_atual,
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
    CASE 
        WHEN mandato_titular_nomeparlamentar LIKE 'NAN' THEN null
        ELSE mandato_titular_nomeparlamentar
    END AS nome_titular_mandato,
    --identificacaoparlamentar_urlpaginaparticular
    identificacaoparlamentar_urlfotoparlamentar as foto_atual,
    arquivo_origem
    data_carga
    FROM source
)
select * from renamed