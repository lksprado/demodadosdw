# CONVENTIONS

Convenções técnicas do repositório. Complementa o `PRD.md`, que define arquitetura e organização de domínio.

Cada seção é marcada como **obrigatório** ou **recomendação** para deixar claro o que bloqueia o Definition of Done e o que é apenas boa prática.

---

## Nomenclatura de tabelas — obrigatório

| Tipo | Padrão | Exemplo |
|---|---|---|
| Intermediate | `int_<domínio>_<verbo>` | `int_person_unioned`, `int_prescription_dedup` |
| Dimensão | `dim_<entidade>` | `dim_person`, `dim_product` |
| Snapshot (dbt) | `snap_<entidade>` | `snap_product`, `snap_address` |
| Fato (transação) | `fct_<processo>` | `fct_prescription` |
| Presentation / OBT | `prs_<domínio>_<descrição>` | `prs_receita_mrr_mensal` |
| Bridge | `bridge_<entidade_a>_<entidade_b>` | `bridge_person_product` |

---

## Nomenclatura de colunas — obrigatório

| Tipo | Prefixo / Convenção | Exemplo |
|---|---|---|
| Surrogate key (PK da dim) | `sk_<entidade>` | `sk_parlamentar`, `sk_voto` |
| Natural Key | Adiciona sufixo `_nk` `<entidade>_id_nk` | `deputado_id_nk`, `votacao_id_nk` |
| Foreign Key na fact | mesmo nome da PK da dim referenciada | `sk_parlamentar` na fct aponta para `sk_parlamentar` da dim |
| Foreign Key na dim | Adiciona sufixo `_fk` `<entidade>_id_fk` | `legislatura_id_fk` |
| Data | `data_<evento>` | `data_votacao`, `data_nascimento` |
| Timestamp | `<evento>_em` em UTC | `criado_em`, `atualizado_em` |
| Booleano | `fl_<condição>` | `fl_valido`, `fl_ativo` |
| Métrica | nome descritivo em snake_case com unidade | `ctg_votos`, `soma_votos` |

---

## Surrogate keys — obrigatório

### Contrato

Toda `dim_` tem uma surrogate key como Primary Key com as seguintes propriedades:

- **Imutável** — após atribuída a uma linha.
- **Única por linha** — não pode haver duplicações, inclusive entre versões SCD da mesma entidade.
- **Livre de colisão entre fontes** — incorpora o sistema de origem.
- **Determinística** — mesmo input sempre produz o mesmo hash, sem necessidade de lookup.
- **Dummy** — valor `0` para linhas que representam "desconhecido" [ver abaixo](#tratamento-de-nulos--obrigatório).

### Padrão de geração

```sql
{{ dbt_utils.generate_surrogate_key([col1, col2]) }} AS sk_<entidade>
```
`dim_dates` é a única exceção: usa PK inteira no formato `YYYYMMDD` (ex: `20240115`), que facilita particionamento das tabela fatos.

### Natural keys

A chave operacional de cada sistema de origem é preservada como atributo na dimensão com sufixo `_nk`. Serve para rastreabilidade e debug — não usar como chave de JOIN na Gold.

Para entidades presentes em múltiplos sistemas de origem, uma coluna `_id` por fonte:

```sql
id_voto        AS voto_id_nk,
partido_id     AS partido_id_nk,
```
---

## Estrutura de CTE nos modelos — obrigatório

Todo modelo usa CTEs com nomes descritivos. Sem subqueries aninhadas. `SELECT *` apenas nas CTEs iniciais que referenciam as fontes ou no select CTE final.

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'deputados') }}
),

-- ou, quando depende de outro modelo Silver:
person_unioned AS (
    SELECT * FROM {{ ref('int_camara_votos') }}
),

renamed AS (
    SELECT
        ---------- surrogate keys
        {{ dbt_utils.generate_surrogate_key([casa, deputado_id]) }}    AS sk_parlamentar,

        ---------- natural keys
        id                                         AS voto_id,

        ---------- strings
        nome_eleitoral,

        ---------- booleans
        e_atualizado,

        ---------- dates
        data_votacao

    FROM source
),

final AS (
    SELECT * FROM renamed
)

SELECT * FROM final
```

### Ordem de colunas — recomendação

```
-------------------------- CHAVES
surrogate keys
natural keys
foreign keys
-------------------------- ATRIBUTOS
strings
numerics
booleans
-------------------------- DATAS
dates
timestamps
-------------------------- AUDITORIA (se houver)
colunas dbt SCD
```

### Tipagem explícita — recomendação

Aplicar nas CTEs iniciais ou CTE final

```sql
id::STRING    AS deputado_id_nk,
quantity::INT AS quantity
```

---

## Grão das tabela fatos — obrigatório

Cada `fct_` tem o grão declarado como **primeira linha da descrição** no `schema.yml`. Define o que representa exatamente uma linha da tabela. Grãos distintos nunca coexistem na mesma tabela fato.

```yaml
models:
  - name: prs_governismo
    description: >
      Grão: um voto por parlamentar por votação onde o Governo emitiu orientação.
      Cobre Câmara (desde 1991) e Senado. Permite calcular perc_governismo para
      qualquer período (ano, trimestre, legislatura) e rastrear a evolução temporal
      do alinhamento de cada parlamentar ao governo.
      Enriquecimento com nome, partido, presidente e mandato ainda não implementado — join com dim_parlamentares e seed raw_executivo_presidente pendente.
```

Modelos Gold de consolidação cross-processo também declaram o grão resultante.


**Toda FK de dimensão é `NOT NULL`.** Para eventos sem dimensão aplicável, a dimensão correspondente tem uma **linha dummy** carregada uma vez e nunca alterada:

```sql
UNION ALL
SELECT
    {{ var('null_key') }}      AS sk_campaign, -- '0'
    {{ var('null_string') }}   AS campaign_name, -- 'desconhecido'
    {{ var('null_string') }}   AS campaign_type -- 'desconhecido'
```

O `COALESCE(c.sk_partido, {{ var('null_key') }})` na fact garante que o LEFT JOIN sem correspondência caia na linha dummy.

---

## Tratamento de nulos — obrigatório

| Contexto | Regra |
|---|---|
| FK em tabela fato | Proibido `NULL` — usar `COALESCE` com `{{ var('null_key') }}` → 0 |
| Atributo de dimensão (texto) | Substituir `NULL` por `{{ var('null_string') }}` → `'desconhecido'` |
| Atributo de dimensão (numérico) | Manter `NULL` ou substituir por `0` |
| Métrica em tabela fato | Permitir `NULL` — `SUM`/`AVG`/`MIN`/`MAX` tratam corretamente; substituir por `0` distorce agregações |

Nulls em atributos de dimensão desaparecem silenciosamente em filtros de Power BI e Genie e geram inconsistência em `GROUP BY` entre engines.

### Variáveis de valor padrão para nulos

Declarado no `dbt_project.yml` para uso em todo o repositório:

```yaml
vars:
  null_key: '0'
  null_string: 'desconhecido'
```

`null_key` é declarado como **string `'0'`** porque as surrogate keys deste projeto são
hashes MD5 (texto) gerados por `dbt_utils.generate_surrogate_key` — a linha dummy precisa
ser do mesmo tipo `text`. Em projetos com SK inteira (ex.: `dim_date` com PK `YYYYMMDD`),
o valor dummy seria o inteiro `0`.

---

## Tratamento de strings — recomendação

Campos de texto podem ser normalizados em lower case e sem caracteres especiais quando aplicável. O projeto não bloqueia por ausência dessa transformação. Verificar macros prontas no repositório antes de implementar manualmente.

---

## Estrutura das dimensões — obrigatório

Hierarquias fixas são achatadas na própria `dim_` — sem snowflaking:

```sql
-- CORRETO: hierarquia achatada em dim_partido
partido_id, partido_nome, partido_coalizao, partido_posicao

-- ERRADO: snowflake / centipede
product_id → sk_coalizao → sk_posicao
```

---
---

## Integração entre processos — obrigatório

Modelos Gold nunca contêm JOIN direto entre duas tabela fatos no grão atômico. O JOIN deve ocorrer após cada fact ser agregada de forma independente, evitando cardinalidade N:N e métricas duplicadas:

```sql
-- ERRADO: join no grão atômico entre duas facts
FROM fct_prescription p
JOIN fct_revenue r ON p.sk_person = r.sk_person

-- CORRETO: cada fact agregada independentemente antes do join
WITH prescricoes_por_medico AS (
    -- grão: 1 linha por sk_professional
    SELECT sk_professional, count(*) AS total_prescricoes
    FROM {{ ref('fct_prescription') }}
    GROUP BY sk_professional
),

receita_por_medico AS (
    -- grão: 1 linha por sk_professional
    SELECT sk_professional, sum(revenue_brl) AS total_receita
    FROM {{ ref('fct_revenue') }}
    GROUP BY sk_professional
),

final AS (
    -- join entre dois conjuntos já no mesmo grão — seguro
    SELECT
        p.sk_professional,
        p.total_prescricoes,
        r.total_receita
    FROM prescricoes_por_medico  p
    LEFT JOIN receita_por_medico r ON p.sk_professional = r.sk_professional
)

SELECT * FROM final
```

---

## Bridge tables — obrigatório quando aplicável

Uma bridge table é usada quando uma dimensão tem múltiplos valores para uma única entidade. Exemplo: um cliente pertence a vários segmentos simultaneamente. Sem bridge, o JOIN direto entre `dim_customer` e `fct_revenue` duplicaria as linhas da fact — cada linha de receita apareceria N vezes, uma por segmento.

A solução é introduzir um `sk_customer_group` que representa o **conjunto de segmentos** de um cliente. A fact aponta para o grupo, não para os segmentos individuais. A bridge expande o grupo em suas partes.

```sql
-- fct_revenue
sk_customer_group   BIGINT   NOT NULL   -- aponta para bridge_customer_segment.sk_customer_group
revenue_brl         DOUBLE   NOT NULL

-- bridge_customer_segment
sk_customer_group   BIGINT   NOT NULL   -- PK do grupo; é a FK que a fact referencia
sk_customer         BIGINT   NOT NULL   -- FK para dim_customer
sk_segment          BIGINT   NOT NULL   -- FK para dim_segment
weighting_factor    DOUBLE   NOT NULL   -- 1/N onde N = número de segmentos do grupo
```

Exemplo de dados: \
fct_revenue \
sk_customer_group = 99,  revenue_brl = 300 \
bridge_customer_segment \
sk_customer_group = 99,  sk_customer = 1,  sk_segment = 10 (oncologia),    weighting_factor = 0.333 \
sk_customer_group = 99,  sk_customer = 1,  sk_segment = 11 (cardiologia),  weighting_factor = 0.333 \
sk_customer_group = 99,  sk_customer = 1,  sk_segment = 12 (pediatria),    weighting_factor = 0.333 


```sql
-- receita por segmento via bridge
SELECT
    s.segment_name,
    SUM(f.revenue_brl * b.weighting_factor) AS revenue_brl  -- 300 * 0.333 = 100 por segmento

FROM {{ ref('fct_revenue') }}            f
JOIN {{ ref('bridge_customer_segment') }} b ON f.sk_customer_group = b.sk_customer_group
JOIN {{ ref('dim_segment') }}             s ON b.sk_segment        = s.sk_segment
JOIN {{ ref('dim_customer') }}            c ON b.sk_customer       = c.sk_customer

GROUP BY s.segment_name
```

Sem o `weighting_factor`, `SUM(revenue_brl)` retornaria R$ 900 (300 × 3 segmentos) em vez de R$ 300. O JOIN com a bridge **sempre** passa pela `sk_customer_group` — nunca diretamente pelas SKs individuais de dimensão.

---

## Testes de contrato

| Teste | Onde aplicar | Obrigatório |
|---|---|---|
| `unique` | PK de toda `dim_` e `fct_` | ✓ |
| `not_null` | PK de toda `dim_` e `fct_` | ✓ |
| `not_null` | Toda FK nas tabela fatos | ✓ |
| `relationships` | FK nas facts → PK da dimensão | recomendação |
| `accepted_values` | `is_current`, `business_line`, colunas de classificação | recomendação |

```yaml
- name: fct_prescription
  columns:
    - name: sk_prescription
      tests: [unique, not_null]
    - name: sk_person
      tests:
        - not_null
        # recomendação:
        - relationships:
            to: ref('dim_person')
            field: sk_person
    - name: sk_campaign
      tests:
        - not_null  # garantido pelo COALESCE com a linha dummy
```

---

## Tags — recomendação

Toda tag deve cumprir seu papel: permitir **seleção transversal** que a estrutura de pastas não oferece (ex.: `dbt build -s tag:senado`, `dbt test -s tag:votacoes`). Para isso, as tags seguem **duas facetas ortogonais e um vocabulário fechado**. Não se usa tag de camada (`stg`/`int`/`dim`/`fct`/`prs`): a camada já é selecionável por caminho (`-s staging.*`, `-s marts.*`).

| Faceta | Valores permitidos | Significado |
|---|---|---|
| **Fonte** (origem do dado) | `camara`, `senado`, `ecidadania`, `ranking` | Sistema de origem. Modelos que unem fontes recebem **todas** (ex.: `dim_parlamentares` → `camara`, `senado`). |
| **Assunto** (área de negócio) | `parlamentar`, `votacoes`, `legislacao`, `score`, `participacao`, `datas` | Tema transversal às camadas. |

Regras:

- Todo modelo recebe **uma faceta de fonte** (ou várias, se combinar fontes) **e uma faceta de assunto**. Exceção: dimensões utilitárias sem fonte (ex.: `dim_dates` / `int_dates` → apenas `datas`).
- Vocabulário **fechado**: nada de sinônimos (`votacao` vs `votacoes`) nem termos novos sem antes adicioná-los a esta tabela.
- `votacoes` = eventos de votação, votos e orientações. `legislacao` = proposições, processos e tabelas de tipos/referência legislativa. `score` = pontuação/ranking. `participacao` = plataforma e-Cidadania. `parlamentar` = deputados, senadores e legislaturas.
- A tag de fonte deve refletir o dado real do modelo, não a pasta — atenção a copy-paste (ex.: um modelo de Senado nunca leva `camara`).

Aplicação no bloco de config:

```sql
{{ config(
    tags=["senado", "votacoes"]
) }}
```

---

## Outras regras de qualidade — obrigatório

- Zero `SELECT *` — colunas sempre explícitas, exceto nas CTEs iniciais de fonte.
- Zero `source()` entre camadas analíticas — apenas `ref()`.
- Grants centralizados em `vars` do `dbt_project.yml` — sem GUIDs hardcoded em SQL.
- Todas as colunas documentadas em PT-BR com descrição de negócio no `schema.yml`.
- Sem subqueries aninhadas — sempre CTEs nomeadas com verbos descritivos.

---

## Definition of Done por modelo

Um modelo está pronto quando:

1. **Grão declarado** como primeira linha da descrição no `schema.yml` — obrigatório para `fct_` e Gold.
2. **Testes obrigatórios passando:** `unique` e `not_null` na PK; `not_null` em todas as FKs das facts.
3. Colunas documentadas em PT-BR com descrição de negócio.
4. Sem `SELECT *` fora das CTEs iniciais de fonte; sem `source()` para dependências internas.
5. CTEs com nomes descritivos, sem subqueries aninhadas.
