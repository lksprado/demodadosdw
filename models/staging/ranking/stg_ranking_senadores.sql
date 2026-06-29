{{ config(
    tags=["ranking", "score"]
) }}

WITH source AS (
    SELECT * FROM {{ source('ranking', 'raw_ranking_senadores') }}
),

renamed AS (
    SELECT
        id                                                              AS ranking_id_nk,
        REGEXP_REPLACE(SPLIT_PART(url_foto, '/', 7), '\D', '', 'g')::INT AS congresso_id_fk,
        {{ clean_string("nome", "upper") }} AS nome,
        {{ clean_string("nome_civil", "upper") }} AS nome_civil,
        url_foto,
        cargo,
        {{ clean_string("partido", "upper") }} AS partido,
        {{ clean_string("situacao", "upper") }} AS situacao,
        {{ clean_string("uf", "upper") }} AS uf,
        slug,
        composicao_pontuacao_pontuacao,
        composicao_pontuacao_anos,
        composicao_pontuacao_ranking_geral,
        composicao_pontuacao_ranking_geral_variacao,
        composicao_pontuacao_ranking_casa,
        composicao_pontuacao_ranking_casa_variacao,
        composicao_pontuacao_ranking_partido,
        composicao_pontuacao_ranking_partido_variacao,
        composicao_pontuacao_ranking_estado,
        composicao_pontuacao_ranking_estado_variacao,
        composicao_pontuacao_ranking_casa_estado,
        composicao_pontuacao_ranking_casa_estado_variacao
    FROM source
)

SELECT * FROM renamed
