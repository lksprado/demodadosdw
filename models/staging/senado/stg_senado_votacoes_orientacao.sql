{{ config(
    tags=["senado", "votacoes"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_votos_orientacao') }}
)

SELECT
    codigovotacaosve::INT                 AS codigo_votacao,
    siglatipomateria                      AS sigla_tipo_materia,
    numeromateria                         AS numero_materia,
    qtdvotossim                           AS total_votos_favor,
    qtdvotosnao                           AS total_votos_contra,
    qtdvotosabstencao                     AS total_votos_abstencao,
    datahora::DATE                        AS data,
    {{ clean_string("partido","upper") }} AS partido,
    CASE
        WHEN {{ clean_string("voto","upper") }} = 'LIVRE' THEN 'LIBERADO'
        ELSE {{ clean_string("voto","upper") }}
    END                                   AS orientacao_voto
{# DESCONSIDERADOS #}
--,descricaovotacao
--,datainiciovotacao
--,dataterminovotacao
FROM source
