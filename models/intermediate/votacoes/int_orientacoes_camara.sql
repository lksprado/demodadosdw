{{ config(
    tags=["camara","votacoes"]
) }}


WITH
camara_votos AS (
    SELECT
        *,
        'CAMARA' AS casa
    FROM {{ ref('stg_camara_votacoes_orientacao') }}
),

ajustes AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_votacao', 'casa']) }} AS sk_votacao,
        casa,
        id_votacao,
        orientacao_voto,
        CASE
            WHEN codigo_tipo_lideranca = 'B' THEN 'BANCADA'
            WHEN codigo_tipo_lideranca = 'P' THEN 'PARTIDO'
        END AS tipo_lideranca,
        CASE
            WHEN sigla_partido_bloco IN ('SD', 'SDD', 'SOLIDARIED', 'SOLIDARIEDADE') THEN 'SOLIDARIEDADE'
            WHEN sigla_partido_bloco IN ('PATRI', 'PATRIOTA') THEN 'PATRIOTA'
            WHEN sigla_partido_bloco IN ('FDR PSDBCIDADAN', 'FDR PSDBCIDADANIA') THEN 'FDR PSDBCIDADANIA'
            ELSE sigla_partido_bloco
        END AS sigla_partido_bloco
    FROM camara_votos
    WHERE orientacao_voto IS NOT NULL
),

correcao_lideranca AS (
    SELECT DISTINCT
        tipo_lideranca,
        sigla_partido_bloco
    FROM ajustes
    WHERE tipo_lideranca IS NOT NULL
),

final AS (
    SELECT
        t1.sk_votacao,
        t1.id_votacao,
        t1.casa,
        t1.orientacao_voto,
        t2.tipo_lideranca,
        t1.sigla_partido_bloco
    FROM ajustes AS t1
    LEFT JOIN correcao_lideranca AS t2
        ON t1.sigla_partido_bloco = t2.sigla_partido_bloco
    WHERE orientacao_voto NOT IN ('ABSTENCAO', 'LIBERADO')
)

SELECT * FROM final
