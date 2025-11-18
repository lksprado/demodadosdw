{{ config(
    tags=["stg","ecidadania","senado"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_tipos_projetos') }}
)
select 
sigla as sigla_proposicao,
descricao as descricao_proposicao,
"dataInicio" as data_inicio,
"dataFim" as data_fim
from source