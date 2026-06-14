{{ config(
    tags=["stg","camara","parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_deputados') }}
),

renamed AS (
    SELECT
        id,
        nomecivil,
        ultimostatus_nomeeleitoral,
        sexo,
        redesocial,
        datanascimento,
        datafalecimento,
        ufnascimento,
        municipionascimento,
        escolaridade,
        COALESCE(ultimostatus_email::TEXT, ultimostatus_gabinete_email::TEXT) AS email
    {# DESCONSIDERADOS
        uri
        cpf
        urlwebsite
        ultimostatus_id
        ultimostatus_uri
        ultimostatus_nome
        ultimostatus_siglapartido
        ultimostatus_uripartido
        ultimostatus_siglauf
        ultimostatus_idlegislatura
        ultimostatus_urlfoto
        ultimostatus_data
        ultimostatus_gabinete_nome
        ultimostatus_gabinete_predio
        ultimostatus_gabinete_sala
        ultimostatus_gabinete_andar
        ultimostatus_gabinete_telefone
        ultimostatus_situacao
        ultimostatus_condicaoeleitoral
        ultimostatus_descricaostatus
     #}
    FROM source
)

SELECT * FROM renamed
