{{ config(
    tags=["mrt","parlamentar"]
) }}

WITH presidentes AS (
    SELECT
        presidente,
        mandato,
        TO_DATE(inicio, 'DD/MM/YYYY') AS inicio,
        TO_DATE(fim,    'DD/MM/YYYY') AS fim
    FROM {{ ref('raw_executivo_presidente') }}
),

mandatos AS (
    SELECT DISTINCT ON (sk_parlamentar)
        sk_parlamentar,
        partido,
        uf_representacao
    FROM {{ ref('int_fct_mandatos') }}
    ORDER BY sk_parlamentar, id_legislatura DESC NULLS LAST
),

parlamentares AS (
    SELECT sk_parlamentar, nome_eleitoral
    FROM {{ ref('int_dim_parlamentares') }}
),

camara AS (
    SELECT
        v.sk_voto,
        v.sk_parlamentar,
        'camara'                                              AS casa,
        vt.data_votacao                                       AS data_voto,
        EXTRACT(YEAR FROM vt.data_votacao)::INT               AS ano,
        date_trunc('quarter', vt.data_votacao)::DATE          AS trimestre,
        vt.proposicao_objeto                                  AS descricao_votacao,
        vt.sigla_orgao,
        vt.aprovado                                           AS votacao_aprovada,
        v.voto_deputado                                       AS voto_parlamentar,
        v.voto_governo,
        v.alinhado_ao_governo
    FROM {{ ref('int_fct_votos_alinhados_camara') }} AS v
    LEFT JOIN {{ ref('int_fct_votacoes') }} AS vt
        ON v.sk_votacao = vt.sk_votacao
),

senado AS (
    SELECT
        vs.sk_voto,
        vs.sk_parlamentar,
        'senado'                                              AS casa,
        vs.data_sessao                                        AS data_voto,
        EXTRACT(YEAR FROM vs.data_sessao)::INT                AS ano,
        date_trunc('quarter', vs.data_sessao)::DATE           AS trimestre,
        vs.identificacao                                      AS descricao_votacao,
        vs.sigla                                              AS sigla_orgao,
        CASE vs.resultado_votacao
            WHEN 'APROVADO'  THEN TRUE
            WHEN 'REPROVADO' THEN FALSE
        END                                                   AS votacao_aprovada,
        vs.voto_senador                                       AS voto_parlamentar,
        vs.voto_governo,
        vs.alinhado_ao_governo
    FROM {{ ref('int_fct_votos_senado') }} AS vs
),

todas_casas AS (
    SELECT * FROM camara
    UNION ALL
    SELECT * FROM senado
)

SELECT
    t.sk_voto,
    t.sk_parlamentar,
    t.casa,
    p.nome_eleitoral,
    m.partido,
    m.uf_representacao,
    t.data_voto,
    t.ano,
    t.trimestre,
    t.descricao_votacao,
    t.sigla_orgao,
    t.votacao_aprovada,
    t.voto_parlamentar,
    t.voto_governo,
    t.alinhado_ao_governo,
    pr.presidente,
    pr.mandato
FROM todas_casas AS t
LEFT JOIN parlamentares AS p  ON t.sk_parlamentar = p.sk_parlamentar
LEFT JOIN mandatos AS m       ON t.sk_parlamentar = m.sk_parlamentar
LEFT JOIN presidentes AS pr   ON t.data_voto BETWEEN pr.inicio AND pr.fim
