{{ config(
    tags=["stg","ecidadania","senado"]
) }}


WITH source AS (
    SELECT * FROM {{ source('senado','raw_senado_tipos_decisao') }}
),
renamed AS (
    SELECT
    sigla as codigo_deliberacao,
    descricao as descricao_deliberacao,
    CASE 
        WHEN sigla = 'APROVADA_NO_PLENARIO' THEN 'A favor'
        WHEN sigla = 'APROVADA_EM_COMISSAO_TERMINATIVA' THEN 'A favor'
        WHEN sigla = 'APROVADA_PARCIALMENTE' THEN 'A favor'
        WHEN sigla = 'APROVADO_CDIR' THEN 'A favor'
        WHEN sigla = 'APROVADO_NA_INTEGRA' THEN 'A favor'
        WHEN sigla = 'APROVADO_PLV' THEN 'A favor'
        WHEN sigla = 'APROVADO_PLV_COM_EMENDAS' THEN 'A favor'
        WHEN sigla = 'DEFERIDO_CDIR' THEN 'A favor'
        WHEN sigla = 'DEFERIDO_PRESIDENCIA_ART_101_RISF' THEN 'A favor'
        WHEN sigla = 'DEFERIDO_PRESIDENCIA_ART_214_RISF' THEN 'A favor'
        WHEN sigla = 'DEFERIDO_PRESIDENCIA_ART_215_RISF' THEN 'A favor'
        WHEN sigla = 'DEFERIDO_PRESIDENCIA_ART_41_RISF' THEN 'A favor'
        WHEN sigla = 'PUBLICADO' THEN 'A favor'
        WHEN sigla = 'REEDITADA' THEN 'A favor'
        WHEN sigla = 'TRANSF_IND' THEN 'A favor'
        WHEN sigla = 'TRANSF_PROJ_LEI_SEN' THEN 'A favor'
        WHEN sigla = 'TRANSF_PROJ_RES_SEN' THEN 'A favor'
        WHEN sigla = 'TRANSF_PROPOSTA_DE_EC' THEN 'A favor'
        WHEN sigla = 'ARQUIVADO_FIM_LEGISLATURA' THEN 'Contra'
        WHEN sigla = 'DEVOLVIDO_CD_ACAO_JUD' THEN 'Contra'
        WHEN sigla = 'IMPUGNADO_PRESIDENCIA' THEN 'Contra'
        WHEN sigla = 'INADIMITIDA_URGENCIA' THEN 'Contra'
        WHEN sigla = 'PERDA_EFICACIA' THEN 'Contra'
        WHEN sigla = 'PREJUDICADO' THEN 'Contra'
        WHEN sigla = 'REJEITADO_COMISSAO_NAO_TERM' THEN 'Contra'
        WHEN sigla = 'REJEITADO_COMISSAO_TERM' THEN 'Contra'
        WHEN sigla = 'REJEITADO_INCONSTICIONALIDADE_CCJ' THEN 'Contra'
        WHEN sigla = 'REJEITADO_PLENARIO' THEN 'Contra'
        WHEN sigla = 'REJEITADO_PLENARIO_CD' THEN 'Contra'
        WHEN sigla = 'RETIRADA_DE_ASSINATURA' THEN 'Contra'
        WHEN sigla = 'RETIRADO_PELO_AUTOR' THEN 'Contra'
        WHEN sigla = 'REVOGADO' THEN 'Contra'
        WHEN sigla = 'SEM_EFICACIA' THEN 'Contra'
        WHEN sigla = 'CONHECIDA' THEN 'Neutro'
        WHEN sigla = 'PARA_PUBLICACAO' THEN 'Neutro'
        WHEN sigla = 'REAUTUADO' THEN 'Neutro'
    ELSE 'Desconhecido'
    END AS tipo_deliberacao
    FROM source
)
select * from renamed

