{{ config(
    tags=["stg","senado"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_votos_senadores') }}
)

SELECT
    codigomateria::INT AS codigo_materia,
    codigosessao::INT AS codigo_sessao,
    codigosessaolegislativa AS codigo_sessao_legislativa,
    codigosessaovotacao AS codigo_sessao_votacao,
    codigovotacaosve::INT AS codigo_votacao,
    TO_DATE(datasessao, 'YYYY-MM-DD') AS data_sessao,
    idprocesso::INT AS id_processo,
    identificacao,
    numero,
    numerosessao::INT AS numero_sessao,
    sigla,
    siglatiposessao AS sigla_tipo_sessao,
    codigoparlamentar AS codigo_parlamentar,
    {{ clean_string("descricaovotoparlamentar","upper") }} AS descricaovotoparlamentar,
    {{ clean_string("siglapartidoparlamentar","upper") }} AS sigla_partido,
    {{ clean_string("siglavotoparlamentar","upper") }} AS sigla_voto,
    data_carga
{# DESCONSIDERADOS 
#}
FROM source
