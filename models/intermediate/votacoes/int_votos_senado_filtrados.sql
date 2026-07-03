{{ config(
    tags=["senado", "votacoes"]
) }}


WITH
senado_votos AS (
    SELECT
        *,
        'SENADO' AS casa
    FROM {{ ref('stg_senado_votos_senadores') }}
),

votos_tratados AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['casa', 'senador_id_nk','codigo_votacao']) }} AS sk_voto,
        CASE
            WHEN senador_id_nk IS NULL THEN '{{ var("null_key") }}'
            ELSE {{ dbt_utils.generate_surrogate_key(['casa', 'senador_id_nk']) }}
        END                                                                                AS sk_parlamentar,
        {{ dbt_utils.generate_surrogate_key(['casa', 'codigo_votacao']) }}                 AS sk_votacao,
        casa,
        senador_id_nk,
        codigo_votacao                                                                     AS votacao_id_nk,
        sigla_partido,
        data_sessao,
        identificacao,
        CASE
            WHEN sigla_voto IN ('SIM', 'SIM - PRESIDENTE ART.48 INCISO XXIII') THEN 'SIM'
            WHEN sigla_voto = 'NAO' THEN 'NAO'
            WHEN sigla_voto = 'ABSTENCAO' THEN 'ABSTENCAO'
            WHEN sigla_voto IN ('OBSTRUCAO', 'P-OD') THEN 'OBSTRUCAO'
        END                                                                                     AS voto,
        CAST(TO_CHAR(data_sessao, 'YYYYMMDD') AS INTEGER)                                       AS sk_data
    FROM senado_votos
),

votos_filtrados AS (
    SELECT * FROM votos_tratados
    WHERE
        voto IS NOT NULL
        AND voto <> 'ABSTENCAO'
        AND votacao_id_nk IS NOT NULL
),

-- partidos normalizados (acento + caixa) para casar com a sigla do voto
partidos_norm AS (
    SELECT
        id_senado,
        id_camara,
        {{ clean_string('sigla_senado', 'upper') }}        AS sigla_senado,
        {{ clean_string('sigla_conformada', 'upper') }}    AS partido_sigla,
        {{ clean_string('nome', 'upper') }}                AS partido_nome,
        data_criacao,
        COALESCE(data_extincao, DATE '9999-12-31')         AS data_extincao
    FROM {{ ref('raw_partidos') }}
),

-- span de vigencia por id_senado (a seed tem grao por id_camara: varias linhas por id_senado)
partidos_vigencia AS (
    SELECT
        id_senado,
        MIN(data_criacao)  AS data_criacao,
        MAX(data_extincao) AS data_extincao
    FROM partidos_norm
    GROUP BY id_senado
),

-- nome/sigla canonicos do registro camara mais recente (reflete o rebrand vigente do partido)
partidos_canonico AS (
    SELECT DISTINCT ON (id_senado)
        id_senado,
        sigla_senado,
        partido_sigla,
        partido_nome
    FROM partidos_norm
    ORDER BY id_senado, id_camara DESC
),

-- lookup final colapsado a um registro por id_senado
partidos_senado AS (
    SELECT
        c.id_senado,
        c.sigla_senado,
        c.partido_sigla,
        c.partido_nome,
        v.data_criacao,
        v.data_extincao
    FROM partidos_canonico c
    JOIN partidos_vigencia v USING (id_senado)
),

-- casa sigla normalizada + data da sessao dentro do periodo de vigencia do partido,
-- desambiguando reutilizacoes de sigla (PL 25 vs 523, PSD 89 vs 557) pelo periodo
votos_com_partidos AS (
    SELECT
        t1.sk_voto,
        t1.sk_parlamentar,
        t1.sk_votacao,
        t1.casa,
        t1.senador_id_nk,
        t1.votacao_id_nk,
        t2.partido_sigla                                        AS partido,
        COALESCE(t2.partido_nome, '{{ var("null_string") }}')   AS partido_nome,
        t1.data_sessao,
        t1.identificacao,
        t1.voto,
        t1.sk_data
    FROM votos_filtrados t1
    LEFT JOIN partidos_senado t2
        ON t2.sigla_senado = t1.sigla_partido
       AND t1.data_sessao BETWEEN t2.data_criacao AND t2.data_extincao
)

SELECT * FROM votos_com_partidos
