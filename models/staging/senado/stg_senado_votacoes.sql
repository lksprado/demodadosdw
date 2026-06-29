{{ config(
    tags=["senado", "votacoes"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_votacoes') }}
)

SELECT
    codigomateria::INT                AS codigo_materia,
    codigosessao::INT                 AS codigo_sessao,
    codigosessaolegislativa           AS codigo_sessao_legislativa,
    codigosessaovotacao               AS codigo_sessao_votacao,
    codigovotacaosve::INT             AS codigo_votacao,
    idprocesso::INT                   AS processo_id_nk,
    identificacao,
    numero,
    numerosessao::INT                 AS numero_sessao,
    sigla,
    descricaovotacao                  AS descricao_votacao,
    siglatiposessao                   AS sigla_tipo_sessao,
    totalvotosabstencao::INT          AS total_votos_abstencao,
    totalvotosnao::INT                AS total_votos_contra,
    totalvotossim::INT                AS total_votos_favor,
    TO_DATE(datasessao, 'YYYY-MM-DD') AS data_sessao,
    CASE
        WHEN resultadovotacao = 'A' THEN 'APROVADO'
        WHEN resultadovotacao = 'R' THEN 'REPROVADO'
        WHEN resultadovotacao = 'P' THEN 'PREJUDICADO'
        WHEN resultadovotacao = 'E' THEN 'EMPATE'
    END                               AS resultado_votacao,
    CASE
        WHEN votacaosecreta = 'N' THEN 'NAO'
        WHEN votacaosecreta = 'S' THEN 'SIM'
    END                               AS votacao_secreta
{# DESCONSIDERADOS #}
--,numerosessao
--,dataapresentacao
--,ementa
--,numero
--,sequencialsessao
--,informelegislativo_casacolegiado
--,informelegislativo_casaenteadm
--,informelegislativo_codigocolegiado
--,informelegislativo_data
--,informelegislativo_id
--,informelegislativo_identeadm
--,informelegislativo_idevento
--,informelegislativo_nomecolegiado
--,informelegislativo_nomeenteadm
--,informelegislativo_numeroautuacao
--,informelegislativo_siglacolegiado
--,informelegislativo_siglaenteadm
--,informelegislativo_texto
--,informelegislativo
--,arquivo_origem
FROM source
