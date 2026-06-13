{{ config(
    materialized='incremental',
    tags=["stg","senado"]
) }}

{% if is_incremental() %}

SELECT * FROM {{ this }} WHERE FALSE

{% else %}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_votos_senadores') }}
)

SELECT
    codigosessaovotacao as codigo_sessao_votacao,
    codigosessao as codigo_sessao,
    datasessao::DATE as data_sessao,
    identificacao,
    sigla,
    numero,
    CASE
        WHEN resultadovotacao = 'A' THEN 'APROVADO'
        WHEN resultadovotacao = 'R' THEN 'REPROVADO'
    END AS resultado_votacao,
    codigoparlamentar as codigo_parlamentar,
    descricaovotoparlamentar as descricao_voto,
    nomeparlamentar as nome,
    siglapartidoparlamentar as sigla_partido,
    {{ clean_string("siglavotoparlamentar","upper") }} as sigla_voto,
    data_carga
{# DESCONSIDERADOS #}
    --sexoparlamentar as
    --siglaufparlamentar
    --,arquivo_origem
FROM source

{% endif %}
