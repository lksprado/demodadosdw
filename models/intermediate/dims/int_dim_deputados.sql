{{
    config(
        unique_key = 'sk_deputado',
        tags = ['intermediate','dimension'],
    )
}}


WITH
deputados_camara AS (
    SELECT * FROM {{ ref('stg_camara__deputados')}}
),
deputado_radar AS (
    SELECT * FROM {{ ref('stg_radarcongresso__parlamentares')}}
),
join_deputados as (
    select
    t1.*,
    t2.id_parlamentar_radar as fk_id_parlamentar_radar
    from deputados_camara t1
    left join 
    deputado_radar t2 
    on t1.id = t2.id_parlamentar_congresso
),
final as (
    select
    -- chave substituta
    {{ dbt_utils.generate_surrogate_key(['id']) }} as sk_deputado,
    id as id_parlamentar_congresso,
    fk_id_parlamentar_radar,
    nome_civil,
    nome_eleitoral_atual,
    partido_atual,
    email_gabinete_atual,
    redesocial_x_twitter,
    redesocial_instagram,
    redesocial_facebook,
    redesocial_youtube,
    redesociais,
    telefone_gabinete_atual,
    cpf,
    sexo,
    data_nascimento,
    geracao,
    data_falecimento,
    uf_nascimento,
    municipio_nascimento,
    uf_atual,
    escolaridade,
    id_legislatura_atual,
    posse_data,
    numero_gabinete_predio_atual,
    numero_gabinete_andar_atual,
    numero_gabinete_sala_atual,
    situacao_atual,
    condicao_eleitoral_atual,
    foto_atual,
    data_carga
    from join_deputados
)
SELECT * FROM final
