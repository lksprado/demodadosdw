{{ config(
    tags=["stg","senado"]
) }}

WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_votacoes') }}
)

SELECT
    codigomateria::INT AS codigo_materia,
    codigosessao::INT AS codigo_sessao,
    codigosessaolegislativa AS codigo_sessao_legislativa,
    codigosessaovotacao AS codigo_sessao_votacao,
    TO_DATE(datasessao,'YYYY-MM-DD') AS data_sessao,
    idprocesso::INT AS id_processo,
    identificacao,
    sigla,
    siglatiposessao AS sigla_tipo_sessao,
    totalvotosabstencao::INT AS total_votos_abstencao,
    totalvotosnao::INT AS total_votos_contra,
    totalvotossim::INT AS total_votos_favor,
    CASE
        WHEN resultadovotacao = 'A' THEN 'APROVADO'
        WHEN resultadovotacao = 'R' THEN 'REPROVADO'
    END AS resultado_votacao,
    CASE
        WHEN votacaosecreta = 'N' THEN 'NAO'
        WHEN votacaosecreta = 'S' THEN 'SIM'
    END AS votacao_secreta,
    data_carga
{# DESCONSIDERADOS #}
--,numerosessao
--,dataapresentacao
--,codigovotacaosve
--,descricaovotacao
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
