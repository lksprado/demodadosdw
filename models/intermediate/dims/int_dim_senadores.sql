{{
    config(
        unique_key = 'sk_parlamentar',
        tags = ['intermediate','dimension'],
    )
}}


WITH
senadores AS (
    SELECT * FROM {{ ref('stg_parlamento__senadores') }}
),
map_radar AS (
    -- Mapeia ID do Senado ao ID do Radar para permitir o join do fato
    SELECT 
        id_parlamentar_congresso,
        id_parlamentar_radar
    FROM {{ ref('stg_radarcongresso__parlamentares') }}
    WHERE tipo_mandato = 'senador'
),
final AS (
    SELECT
        -- SCD1: chave substituta estável por chave de negócio (id do Senado)
        {{ dbt_utils.generate_surrogate_key(['s.id']) }} AS sk_parlamentar,
        s.id AS id_parlamentar_congresso,
        s.nome_civil,
        s.nome_eleitoral_atual,
        s.partido_atual,
        s.email_gabinete_atual,
        s.redesocial_x_twitter,
        s.redesocial_instagram,
        s.redesocial_facebook,
        s.redesocial_youtube,
        s.redesociais,
        s.telefone_gabinete_atual,
        s.cpf,
        s.sexo,
        s.data_nascimento,
        s.geracao,
        s.data_falecimento,
        s.uf_nascimento,
        s.municipio_nascimento,
        s.uf_atual,
        s.escolaridade,
        s.id_legislatura_atual,
        s.posse_data,
        s.numero_gabinete_predio_atual,
        s.numero_gabinete_andar_atual,
        s.numero_gabinete_sala_atual,
        s.situacao_atual,
        s.condicao_eleitoral_atual,
        s.foto_atual,
        s.data_carga
    FROM senadores s
    LEFT JOIN map_radar m
        ON m.id_parlamentar_congresso = s.id
)
SELECT * FROM final
