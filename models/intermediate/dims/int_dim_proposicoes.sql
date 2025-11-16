{{ config(
    tags=["dim","senado"]
) }}

with 
status_proposicoes as (
    select * from {{ ref('stg_senado__status')}}
),
tipos_decisao as (
    select * from {{ ref('stg_senado__tipos_decisao')}}
),
tipos_entes as (
    select * from {{ ref('stg_senado__tipos_entes')}}
),
tab_join as (
    select 
    t1.sk_proposicao,
    t1.id_proposicao,
    t1.apelido,
    t1.ementa,
    t1.tramitando,
    t1.codigo_casa,
    t1.casa,
    t1.codigo_ente,
    t3.nome_ente,
    t3.tipo_ente,
    t1.tipo_conteudo,
    t1.tipo_proposicao,
    t1.data_apresentacao,
    t1.autoria,
    t1.codigo_deliberacao,
    t2.descricao_deliberacao,
    t2.tipo_deliberacao,
    t1.data_deliberacao,
    t1.link_documento,
    t1.objetivo,
    t1.norma_gerada,
    t1.ultima_informacao_atualizada
    from status_proposicoes t1 
    left join tipos_decisao t2 
    on t1.codigo_deliberacao = t2.codigo_deliberacao
    left join tipos_entes t3
    on t1.codigo_ente = t3.codigo_ente and t1.codigo_casa = t3.codigo_casa
)
select * from tab_join