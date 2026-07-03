{{ config(
    tags=["camara", "votacoes"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_votos_deputados') }}
),

renamed AS (
    SELECT
        deputado__id::INT                                               AS deputado_id_nk,
        deputado__nome                                                  AS nome,
        deputado__siglauf                                               AS uf,
        {{ clean_string("tipovoto","upper") }}                          AS voto,
        deputado__idlegislatura                                         AS legislatura_id_fk,
        SPLIT_PART(url_votos, '/', 7)                                   AS votacao_id_fk,
        split_part(deputado__uripartido, '/', 7)::INT                   AS partido_id_fk
    FROM source
)

SELECT * FROM renamed
