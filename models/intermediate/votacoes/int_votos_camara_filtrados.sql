{{ config(
    tags=["camara", "votacoes"]
) }}


WITH
camara_votos AS (
    SELECT
        *,
        'CAMARA' AS casa
    FROM {{ ref('stg_camara_votos_deputados') }}
),

votos_filtrados AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'deputado_id_nk','votacao_id_fk']) }} AS sk_voto,
        CASE
            WHEN deputado_id_nk IS NULL THEN '{{ var("null_key") }}'
            ELSE {{ dbt_utils.generate_surrogate_key(['casa', 'deputado_id_nk']) }}
        END                                                                          AS sk_parlamentar,
        {{ dbt_utils.generate_surrogate_key(['casa', 'votacao_id_fk']) }}                   AS sk_votacao,
        casa,
        deputado_id_nk,
        partido_id_fk,
        votacao_id_fk,
        CASE
            WHEN voto = 'FAVORAVEL COM RESTRICOES' THEN 'SIM'
            ELSE voto
        END                                                                          AS voto
    FROM camara_votos
    WHERE voto NOT IN ('ARTIGO 17', 'BRANCO', 'ABSTENCAO')
),
votos_com_partidos AS (
    SELECT 
        t1.sk_voto,
        t1.sk_parlamentar,
        t1.sk_votacao,
        t1.casa,
        t1.deputado_id_nk,
        t1.partido_id_fk,
        t1.votacao_id_fk,
        {{ clean_string("REPLACE(t2.sigla_conformada,'*','')", "upper") }} AS partido,
        {{ clean_string("t2.nome", "upper") }}          AS partido_nome,
        t1.voto
    FROM votos_filtrados t1
    LEFT JOIN {{ ref('raw_partidos') }} t2
    ON t1.partido_id_fk = t2.id_camara
)

SELECT * FROM votos_com_partidos
