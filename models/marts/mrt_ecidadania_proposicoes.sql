{{ config(
    tags=["mrt","ecidadania","senado"]
) }}

WITH
proposicoes AS (
    SELECT * FROM {{ ref('int_fct_ecidadania_proposicoes')}}
),
status as (
    select * from {{ ref('int_dim_proposicoes')}}
),
tipo_de_proposicoes AS (
    SELECT * FROM {{ ref('int_dim_tipo_proposicao')}}
),
final as (
    select 
    t1.data_extracao,
    t1.sk_proposicao,
    t1.id_proposicao,
    t1.ano_proposicao,
    t2.apelido,
    t1.ementa,
    CASE WHEN t2.tramitando IS NULL THEN 'Não informado' ELSE t2.tramitando END as tramitando,
    t1.votos_sim,
    t1.votos_nao,
    t1.total_votos,
    t1.vontade_popular,
    COALESCE(t2.tipo_deliberacao,'Sem deliberação') as tipo_deliberacao,
    CASE 
        WHEN t2.tipo_deliberacao IS NULL AND t2.tramitando = 'Sim' THEN 'A decidir'    
        WHEN t1.vontade_popular = t2.tipo_deliberacao THEN 'Convergente'
        WHEN t1.vontade_popular = 'A favor' AND t2.tipo_deliberacao = 'Contra' THEN 'Divergente'
        WHEN t1.vontade_popular = 'Contra' AND t2.tipo_deliberacao = 'A favor' THEN 'Divergente'
        WHEN t1.vontade_popular = 'Empate' AND t2.tipo_deliberacao IN ('A favor', 'Contra') THEN 'Neutro'
        WHEN t1.vontade_popular = 'Empate' AND t2.tipo_deliberacao = 'Neutro' THEN 'Não avaliado'
        ELSE 'Não avaliado'
    END AS aderencia_vontade_popular,
    t1.flag_mais_votado_no_dia,
    t2.casa,
    CASE WHEN t2.sk_proposicao is not null then true else false END::boolean as status_consultado,
    t2.nome_ente,
    t3.descricao_proposicao,
    t2.data_apresentacao,
    t2.autoria,
    t2.descricao_deliberacao,
    t2.data_deliberacao,
    t2.norma_gerada,
    t2.link_documento,
    t1.link as link_votacao
    from proposicoes t1 
    left join status t2 
    on t1.sk_proposicao = t2.sk_proposicao
    left join tipo_de_proposicoes t3 
    on t1.sigla_proposicao = t3.sigla_proposicao
    ORDER BY total_votos DESC
)
select * from final