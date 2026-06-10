{{ config(
    tags=["stg","camara","votacoes"]
) }}


WITH source AS (
    SELECT * FROM {{ source('camara','raw_camara_votacoes_orientacao') }}
),

renamed AS (
    SELECT
        SPLIT_PART(url_votos, '/', 7) AS id_votacao,
        {{ clean_string("orientacaovoto","upper") }} AS orientacao_voto,
        codtipolideranca AS codigo_tipo_liderancao,
        codpartidobloco::INT AS codigo_partido_bloco,
        {{ clean_string("siglapartidobloco","upper") }} AS sigla_partido_bloco
    FROM source
)

SELECT * FROM renamed
