{{ config(
    tags=["camara", "score"]
) }}

WITH source AS (
    SELECT
        'CAMARA'                                 AS casa,
        ranking_id_nk,
        congresso_id_fk,
        nome,
        nome_eleitoral,
        nome_civil,
        url_foto,
        cargo,
        partido,
        situacao,
        slug,
        composicao_pontuacao_anos,
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
    FROM {{ ref('stg_ranking_deputados') }}
),

parsed AS (
    SELECT
        *,
        REPLACE(composicao_pontuacao_anos, '''', '"')::JSONB AS ranking_json
    FROM source
),

exploded AS (
    SELECT
        parsed.*,
        ranking_item
    FROM parsed
    CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(parsed.ranking_json) AS ranking_item
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'congresso_id_fk']) }}                    AS sk_parlamentar,
        casa,
        ranking_id_nk,
        congresso_id_fk,
        nome,
        nome_civil,
        partido,
        situacao,
        uf,
        slug,
        pontuacao_geral,
        ranking_geral,
        ranking_casa,
        ranking_partido,
        ranking_estado,
        ranking_casa_estado,
        (ranking_item ->> 'ano')::INT                                                       AS ano,

        (ranking_item -> 'nota_base' ->> 'votacoes')::NUMERIC(18, 4)                        AS nota_base_votacoes,
        (ranking_item -> 'nota_base' ->> 'gastos')::NUMERIC(18, 4)                          AS nota_base_gastos,
        (ranking_item -> 'nota_base' ->> 'presenca')::NUMERIC(18, 4)                        AS nota_base_presenca,
        (ranking_item -> 'nota_base' ->> 'privilegios')::NUMERIC(18, 4)                     AS nota_base_privilegios,

        (ranking_item -> 'bonus_penalidades' ->> 'processos')::NUMERIC(18, 4)               AS bonus_processos,
        (ranking_item -> 'bonus_penalidades' ->> 'producao_legislativa')::NUMERIC(18, 4)    AS bonus_producao_legislativa,
        (ranking_item -> 'bonus_penalidades' ->> 'articulacao_legislativa')::NUMERIC(18, 4) AS bonus_articulacao_legislativa,

        (ranking_item ->> 'pontuacao')::NUMERIC(18, 4)                                      AS pontuacao

    FROM exploded
)

SELECT *
FROM renamed
