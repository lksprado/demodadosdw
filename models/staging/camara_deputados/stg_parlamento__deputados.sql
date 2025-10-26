{{ config(materialized='view') }}
WITH source AS (
    SELECT * FROM {{ source('camara','raw_parlamento_deputados') }}
),
renamed AS (
    SELECT
        id,
        nomecivil AS nome,
        sexo,
        ufnascimento AS uf_nascimento,
        municipionascimento AS municipio_nascimento,
        escolaridade,
        CASE WHEN ultimostatus_siglapartido LIKE '%PODE%' then 'PODEMOS'
        ELSE ultimostatus_siglapartido 
        END AS partido,
        ultimostatus_siglauf AS uf_representacao,
        ultimostatus_idlegislatura AS id_legislatura,
        ultimostatus_urlfoto AS link_foto,
        ultimostatus_data::date AS data_posse,
        ultimostatus_nomeeleitoral AS nome_eleitoral,
        ultimostatus_gabinete_sala AS num_gabinete_sala,
        ultimostatus_gabinete_predio AS num_gabinete_predio,
        nullif(ultimostatus_gabinete_andar,'NONE') AS num_gabinete_andar,
        ultimostatus_gabinete_telefone AS telefone,
        ultimostatus_gabinete_email AS email,
        ultimostatus_situacao AS situacao_atual,
        ultimostatus_condicaoeleitoral AS condicao_eleitoral,
        CASE
            WHEN length((cpf::TEXT)) = 9 THEN '00' || (cpf::TEXT)
            WHEN length((cpf::TEXT)) = 10 THEN '0' || (cpf::TEXT)
            ELSE cpf::TEXT
        END AS cpf,
        replace(replace(replace(redesocial, '[', ''), ']', ''), '''', '')
        AS redesociais,
        to_date(datanascimento, 'YYYY-MM-DD') AS data_nascimento,
        to_date(cast(datafalecimento as text), 'YYYY-MM-DD') AS data_falecimento,
        case
            when datanascimento is null then null
            when (datanascimento)::date between date '1928-01-01' and date '1945-12-31' then 'Silenciosa'
            when (datanascimento)::date between date '1946-01-01' and date '1964-12-31' then 'Baby Boomer'
            when (datanascimento)::date between date '1965-01-01' and date '1980-12-31' then 'X'
            when (datanascimento)::date between date '1981-01-01' and date '1996-12-31' then 'Millennial'
            when (datanascimento)::date between date '1997-01-01' and date '2012-12-31' then 'Z'
            when (datanascimento)::date >=  date '2013-01-01' then 'Alpha'
            else 'OUTRA'
        end as geracao,
        uri as link_api_oficial,
        data_carga
    FROM source
),
redesocial_tratamento AS (
    SELECT
        id,
        trim(split_part(redesociais, ',', 1)) AS rede_1,
        trim(split_part(redesociais, ',', 2)) AS rede_2,
        trim(split_part(redesociais, ',', 3)) AS rede_3,
        trim(split_part(redesociais, ',', 4)) AS rede_4
    FROM renamed
),
redesocial_tratamento_2 AS (
    SELECT
        id,
        CASE
            WHEN rede_1 LIKE '%twitter%' THEN rede_1
            WHEN rede_2 LIKE '%twitter%' THEN rede_2
            WHEN rede_3 LIKE '%twitter%' THEN rede_3
            WHEN rede_4 LIKE '%twitter%' THEN rede_4
        END AS redesocial_x_twitter,
        CASE
            WHEN rede_1 LIKE '%facebook%' THEN rede_1
            WHEN rede_2 LIKE '%facebook%' THEN rede_2
            WHEN rede_3 LIKE '%facebook%' THEN rede_3
            WHEN rede_4 LIKE '%facebook%' THEN rede_4
        END AS redesocial_facebook,
        CASE
            WHEN rede_1 LIKE '%instagram%' THEN rede_1
            WHEN rede_2 LIKE '%instagram%' THEN rede_2
            WHEN rede_3 LIKE '%instagram%' THEN rede_3
            WHEN rede_4 LIKE '%instagram%' THEN rede_4
        END AS redesocial_instagram,
        CASE
            WHEN rede_1 LIKE '%youtube%' THEN rede_1
            WHEN rede_2 LIKE '%youtube%' THEN rede_2
            WHEN rede_3 LIKE '%youtube%' THEN rede_3
            WHEN rede_4 LIKE '%youtube%' THEN rede_4
        END AS redesocial_youtube
    FROM redesocial_tratamento
)
SELECT
    t1.id,
    t1.nome,
    t1.nome_eleitoral,
    t1.partido,
    t1.email,
    t2.redesocial_x_twitter,
    t2.redesocial_instagram,
    t2.redesocial_facebook,
    t2.redesocial_youtube,
    t1.telefone,
    t1.sexo,
    t1.data_nascimento,
    t1.geracao,
    t1.uf_nascimento,
    t1.municipio_nascimento,
    t1.uf_representacao,
    t1.escolaridade,
    t1.id_legislatura,
    t1.data_posse,
    t1.num_gabinete_predio,
    t1.num_gabinete_andar,
    t1.num_gabinete_sala,
    t1.situacao_atual,
    t1.condicao_eleitoral,
    t1.link_foto,
    t1.link_api_oficial,
    t1.data_carga
FROM renamed AS t1
INNER JOIN redesocial_tratamento_2 AS t2
    ON t1.id = t2.id
