WITH
de_para as (
    select
    *
    from {{ ref('int_map_parlamentares') }}
),
senadores AS (
    SELECT * FROM {{ ref('stg_parlamento__senadores')}}
),
enrichment as (
    select * from {{ ref('stg_ranking__parlamentares')}}
),
final AS (
    SELECT
    t1.sk_parlamentar,
    t2.nome,
    t2.partido,
    t1.casa,
    t2.nome_eleitoral,
    t2.email,
    t3.redesocial_x_twitter,
    t3.redesocial_instagram,
    t3.redesocial_facebook,
    t3.redesocial_youtube,
    t2.telefone,
    t2.sexo,
    t3.data_nascimento::date,
        case
            when t3.data_nascimento is null then null
            when (t3.data_nascimento)::date between date '1928-01-01' and date '1945-12-31' then 'Silenciosa'
            when (t3.data_nascimento)::date between date '1946-01-01' and date '1964-12-31' then 'Baby Boomer'
            when (t3.data_nascimento)::date between date '1965-01-01' and date '1980-12-31' then 'X'
            when (t3.data_nascimento)::date between date '1981-01-01' and date '1996-12-31' then 'Millennial'
            when (t3.data_nascimento)::date between date '1997-01-01' and date '2012-12-31' then 'Z'
            when (t3.data_nascimento)::date >=  date '2013-01-01' then 'Alpha'
            else 'OUTRA'
        end as geracao,
    t2.uf_nascimento,
    t2.municipio_nascimento,
    t2.uf_representacao,
    t2.escolaridade,
    t2.id_legislatura,
    t2.data_posse,
    t2.num_gabinete_predio,
    t2.num_gabinete_andar,
    t2.num_gabinete_sala,
    t2.situacao_atual,
    t2.condicao_eleitoral,
    t3.link_api_oficial,
    t2.link_foto,
    t2.data_carga
    from de_para t1
    left join senadores t2
    on t1.id_nk = t2.id
    left join enrichment t3 
    on t1.id_parlamentar_ranking = t3.id_parlamentar_ranking
    where t1.casa = 'senado'
)
SELECT * FROM final
