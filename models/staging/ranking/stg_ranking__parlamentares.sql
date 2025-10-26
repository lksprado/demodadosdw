{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('ranking','raw_ranking_parlamentares') }}
),

renamed AS (
    SELECT
        parliamentarianid as id_parlamentar_ranking,
        CASE 
            WHEN parliamentarian_register = 'NONE' THEN
                regexp_replace(
                    parliamentarian_otherinformations,
                    '.*([0-9]+)$',
                    '\1'
                )
            ELSE
                parliamentarian_register
        END ::int  AS id_parlamentar_congresso,
        parliamentarian_position as cargo,
        scoretotal as score_total,
        scoreprivileges as score_antiprivilegio,
        scorewastage as score_antidesperdicio,
        scoresavequotapercentage as pctg_economia,
        scoreprocess as score_processos,
        scoreinternal as score_outros,
        scorerankingbyposition as ranking_por_cargo,
        scorerankingbystate as ranking_por_estado,
        parliamentarianstatecount as cnt_parlamentares_estado,
        parliamentarianpositionstatecount as cnt_cargo_por_estado,
        parliamentariancount as total_parlamentares,
        parliamentarianpositioncount as total_parlamentar_por_casa,
        ROUND(parliamentarianquotatotal::numeric,2) as vlr_mandato_disponivel_total,
        ROUND(parliamentarianquotatotal::numeric-(parliamentarianquotatotal*(scoresavequotapercentage/100))::numeric,2) as vlr_mandato_gasto_total,
        ROUND((parliamentarianquotatotal*(scoresavequotapercentage/100))::numeric,2) as vlr_mandato_economizado_total,
        ROUND(parliamentarianquotamaxyear::numeric,2) as vlr_cota_parlamentar_disponivel_subtotal,
        ROUND(((parliamentarianquotatotal::numeric-(parliamentarianquotatotal*(scoresavequotapercentage/100)))-parliamentarianstaffamountused)::numeric,2) as vlr_cota_parlamentar_gasto_subtotal,
        ROUND(parliamentarianquotamaxyear::numeric-((parliamentarianquotatotal::numeric-(parliamentarianquotatotal*(scoresavequotapercentage/100)))-parliamentarianstaffamountused)::numeric,2) as vlr_cota_parlamentar_economizado_subtotal, 
        ROUND(parliamentarianstaffmaxyear::numeric,2) as vlr_verba_gabinete_disponivel_subtotal,
        ROUND(parliamentarianstaffamountused::numeric,2) as vlr_verba_gabinete_gasto_subtotal,
        ROUND((parliamentarianstaffmaxyear-parliamentarianstaffamountused)::numeric,2) as vlr_verba_gabinete_economizado_subtotal,
        parliamentarian_name as nome,
        parliamentarian_email as email_gabinete_atual,
        parliamentarian_otherinformations as link_api_oficial,
        parliamentarian_profession as profissao,
        CASE WHEN parliamentarian_academic like 'NONE' then null else parliamentarian_academic end as escolaridade,
        parliamentarian_phone as telefone_gabinete_atual,
        parliamentarian_instagram as redesocial_instagram,
        parliamentarian_twitter as redesocial_x_twitter,
        parliamentarian_facebook as redesocial_facebook,
        parliamentarian_youtube as redesocial_youtube,
        active as is_ativo,
        link as link_parlamentar,
        data_carga
    FROM source
)
select * from renamed


