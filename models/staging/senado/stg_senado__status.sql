{{ config(
    tags=["stg","ecidadania","senado"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_status') }}
),
renamed AS (
    SELECT
    -- id as nk_id_proposicao,
    -- codigomateria as codigo_materia,
    {{ dbt_utils.generate_surrogate_key(['identificacao']) }} as sk_proposicao,
    identificacao as id_proposicao,
    apelido,
    casaidentificadora as codigo_casa,
    CASE WHEN casaidentificadora = 'SF' then 'Senado Federal' else 'Congresso Nacional' end as casa,
    upper(enteidentificador) as codigo_ente,
    tipoconteudo as tipo_conteudo,
    ementa,
    tipodocumento as tipo_proposicao,
    to_date(dataapresentacao,'YYYY-MM-DD') as data_apresentacao,
    autoria,
    tramitando,
    to_date(datadeliberacao,'YYYY-MM-DD') as data_deliberacao,
    siglatipodeliberacao as codigo_deliberacao,
    urldocumento as link_documento,
    objetivo,
    normagerada as norma_gerada,
    ultimainformacaoatualizada as ultima_informacao_atualizada
    FROM source
    where id is not null
)
select * from renamed
