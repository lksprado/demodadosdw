{{ config(
    tags=["senado", "score"]
) }}

WITH senador_score AS (
    SELECT
        'SENADO'                                 AS casa,
        ranking_id_nk,
        congresso_id_fk,
        nome,
        nome_civil,
        partido,
        situacao,
        slug,
        composicao_pontuacao_pontuacao           AS pontuacao_geral,
        composicao_pontuacao_ranking_geral       AS ranking_geral,
        composicao_pontuacao_ranking_casa        AS ranking_casa,
        composicao_pontuacao_ranking_partido     AS ranking_partido,
        composicao_pontuacao_ranking_estado      AS ranking_estado,
        composicao_pontuacao_ranking_casa_estado AS ranking_casa_estado,
        CASE
            WHEN uf = 'ACRE' THEN 'AC'
            WHEN uf = 'ALAGOAS' THEN 'AL'
            WHEN uf = 'AMAPA' THEN 'AP'
            WHEN uf = 'AMAZONAS' THEN 'AM'
            WHEN uf = 'BAHIA' THEN 'BA'
            WHEN uf = 'CEARA' THEN 'CE'
            WHEN uf = 'DISTRITO FEDERAL' THEN 'DF'
            WHEN uf = 'ESPIRITO SANTO' THEN 'ES'
            WHEN uf = 'GOIAS' THEN 'GO'
            WHEN uf = 'MARANHAO' THEN 'MA'
            WHEN uf = 'MATO GROSSO' THEN 'MT'
            WHEN uf = 'MATO GROSSO DO SUL' THEN 'MS'
            WHEN uf = 'MINAS GERAIS' THEN 'MG'
            WHEN uf = 'PARA' THEN 'PA'
            WHEN uf = 'PARAIBA' THEN 'PB'
            WHEN uf = 'PARANA' THEN 'PR'
            WHEN uf = 'PERNAMBUCO' THEN 'PE'
            WHEN uf = 'PIAUI' THEN 'PI'
            WHEN uf = 'RIO DE JANEIRO' THEN 'RJ'
            WHEN uf = 'RIO GRANDE DO NORTE' THEN 'RN'
            WHEN uf = 'RIO GRANDE DO SUL' THEN 'RS'
            WHEN uf = 'RONDONIA' THEN 'RO'
            WHEN uf = 'RORAIMA' THEN 'RR'
            WHEN uf = 'SANTA CATARINA' THEN 'SC'
            WHEN uf = 'SAO PAULO' THEN 'SP'
            WHEN uf = 'SERGIPE' THEN 'SE'
            WHEN uf = 'TOCANTINS' THEN 'TO'
        END                                      AS uf
    FROM {{ ref('stg_ranking_senadores') }}
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'congresso_id_fk']) }} AS sk_parlamentar,
        *
    FROM senador_score
)

SELECT * FROM final
