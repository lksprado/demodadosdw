{{ config(
    tags=["stg","ranking", "parlamentar"]
) }}


WITH source AS (
    SELECT * FROM {{ source('ranking','raw_ranking_parlamentares') }}
),

renamed AS (
    SELECT
        parliamentarianid AS id_parlamentar_ranking,
        parliamentarian_position AS cargo,
        scoretotal AS score_total,
        scoreprivileges AS score_antiprivilegio,
        scorewastage AS score_antidesperdicio,
        (scoresavequotapercentage / 100)::NUMERIC(18, 2) AS perc_economia,
        scoreprocess AS score_processos,
        scoreinternal AS score_outros,
        scorerankingbyposition AS ranking_por_cargo,
        scorerankingbystate AS ranking_por_estado,
        parliamentarianstatecount AS cnt_parlamentares_estado,
        parliamentarianpositionstatecount AS cnt_cargo_por_estado,
        parliamentariancount AS total_parlamentares,
        parliamentarianpositioncount AS total_parlamentar_por_casa,
        parliamentarian_name AS nome,
        parliamentarian_email AS email,
        parliamentarian_otherinformations AS link_api_oficial,
        parliamentarian_state_prefix AS uf_representacao,
        parliamentarian_instagram AS redesocial_instagram,
        parliamentarian_twitter AS redesocial_x_twitter,
        parliamentarian_facebook AS redesocial_facebook,
        parliamentarian_youtube AS redesocial_youtube,
        active AS is_ativo,
        link AS link_parlamentar,
        data_carga,
        CASE
            WHEN parliamentarian_position LIKE 'SENADOR'
                THEN
                    REVERSE(
                        SPLIT_PART(
                            REVERSE(RTRIM(parliamentarian_otherinformations, '/')),
                            '/',
                            1
                        )
                    )::INT
            WHEN parliamentarian_position LIKE 'DEPUTADO FEDERAL' THEN parliamentarian_register::INT
        END AS id_parlamentar_congresso,
        ROUND(parliamentarianquotatotal::NUMERIC, 2) AS vlr_mandato_disponivel_total,
        ROUND(parliamentarianquotatotal::NUMERIC - (parliamentarianquotatotal * (scoresavequotapercentage / 100))::NUMERIC, 2) AS vlr_mandato_gasto_total,
        ROUND((parliamentarianquotatotal * (scoresavequotapercentage / 100))::NUMERIC, 2) AS vlr_mandato_economizado_total,
        ROUND(parliamentarianquotamaxyear::NUMERIC, 2) AS vlr_cota_parlamentar_disponivel_subtotal,
        ROUND(((parliamentarianquotatotal::NUMERIC - (parliamentarianquotatotal * (scoresavequotapercentage / 100))) - parliamentarianstaffamountused)::NUMERIC, 2) AS vlr_cota_parlamentar_gasto_subtotal,
        ROUND(parliamentarianquotamaxyear::NUMERIC - ((parliamentarianquotatotal::NUMERIC - (parliamentarianquotatotal * (scoresavequotapercentage / 100))) - parliamentarianstaffamountused)::NUMERIC, 2) AS vlr_cota_parlamentar_economizado_subtotal,
        ROUND(parliamentarianstaffmaxyear::NUMERIC, 2) AS vlr_verba_gabinete_disponivel_subtotal,
        ROUND(parliamentarianstaffamountused::NUMERIC, 2) AS vlr_verba_gabinete_gasto_subtotal,
        ROUND((parliamentarianstaffmaxyear - parliamentarianstaffamountused)::NUMERIC, 2) AS vlr_verba_gabinete_economizado_subtotal,
        NULLIF(parliamentarian_profession, 'NONE') AS profissao,
        CASE WHEN parliamentarian_academic LIKE 'NONE' THEN NULL ELSE parliamentarian_academic END AS escolaridade,
        NULLIF(parliamentarian_phone, 'NONE') AS telefone_gabinete_atual,
        TO_DATE(SUBSTRING(parliamentarian_datebirth, 0, 9), 'YYYYMMDD') AS data_nascimento
    FROM source
)

SELECT * FROM renamed
