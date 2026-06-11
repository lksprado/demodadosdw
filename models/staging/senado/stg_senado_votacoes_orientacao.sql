{{ config(
    tags=["stg","senado"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_votos_orientacao') }}
)
SELECT
codigovotacaosve::INT AS codigo_votacao,
siglatipomateria AS sigla_tipo_materia,
numeromateria AS numero_materia,
qtdvotossim AS total_votos_favor,
qtdvotosnao AS total_votos_contra,
qtdvotosabstencao AS total_votos_abstencao,
datahora::DATE AS data,
UPPER(partido) AS partido,
{{ clean_string("voto","upper") }} as voto,
data_carga
{# DESCONSIDERADOS #}
--,descricaovotacao
--,datainiciovotacao
--,dataterminovotacao
FROM source
