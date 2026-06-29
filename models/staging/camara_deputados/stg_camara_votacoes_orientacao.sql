{{ config(
    tags=["camara", "votacoes"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_votacoes_orientacao') }}
),

renamed AS (
    SELECT
        SPLIT_PART(url_votos, '/', 7)                                                                      AS votacao_id_nk,
        {{ clean_string("orientacaovoto","upper") }}                                                       AS orientacao_voto,
        codtipolideranca                                                                                   AS codigo_tipo_lideranca,
        codpartidobloco::INT                                                                               AS codigo_partido_bloco,
        REGEXP_REPLACE(TRIM({{ clean_string("siglapartidobloco","upper") }}), '[^a-zA-Z0-9À-ÿ ]', '', 'g') AS sigla_partido_bloco
    FROM source
)

SELECT * FROM renamed
