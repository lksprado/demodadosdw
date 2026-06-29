{{ config(
    tags=["senado", "legislacao"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_processo') }}
)

SELECT
    id::INT                                    AS processo_id_nk,
    tramitando,
    autoria,
    codigomateria::INT                         AS codigo_materia,
    dataapresentacao::DATE                     AS data_apresentacao,
    datadeliberacao::DATE                      AS data_liberacao,
    datasituacaoatual::DATE                    AS data_situacao_atual,
    NULLIF(
        REGEXP_REPLACE(
            COALESCE(CAST(identificacao AS TEXT), ''),
            '[^[:alpha:]]',
            '',
            'g'
        ),
        ''
    )                                           AS codigo_tipo,
    identificacao,
    normagerada,
    {{ clean_string("objetivo","upper") }}      AS objetivo,
    siglatipodeliberacao                        AS sigla_tipo_deliberacao,
    {{ clean_string("situacaoatual","upper") }} AS situacao_atual,
    {{ clean_string("tipoconteudo","upper") }}  AS tipo_conteudo,
    {{ clean_string("tipodocumento","upper") }} AS tipo_documento
FROM source
