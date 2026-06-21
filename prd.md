# PRD — `demodadosdw` (Data Warehouse)

Documento de produto e arquitetura do repositório `demodadosdw`. Define **propósito,
escopo, arquitetura e organização de domínio**. As regras técnicas de implementação
(nomenclatura, surrogate keys, CTEs, nulos, testes, _Definition of Done_) ficam no
[`CONVENTIONS.md`](./CONVENTIONS.md), que complementa este documento.

---

## 1. Visão Geral

Levantamento e modelagem de dados para **análises políticas** com foco em cidadania,
democracia e _advocacy_ no Brasil. O ecossistema usa Python, Airflow, PostgreSQL e dbt
para estruturar, modelar e publicar dados públicos de forma acessível e confiável.

Este repositório trata **exclusivamente da camada de Transformação**: parte dos dados
já ingeridos no schema `raw` de um PostgreSQL local (database `demodados`) e os modela
até as camadas analíticas (Silver e Gold). O `demodadosdw` é consumido como _submodule_
do repositório de orquestração.

---

## 2. Contexto e Repositórios

O projeto Demodados é composto por três repositórios:

| Repositório | Papel | Saída |
|---|---|---|
| **[Ingestor](https://github.com/lksprado/demodados)** (`demodados`) | Pipelines de ingestão | Camadas Raw e Bronze |
| **[Data Warehouse](https://github.com/lksprado/demodadosdw)** (`demodadosdw`, este repo) | Modelagem SQL com dbt | Silver e Gold no DW |
| **[Orquestrador](https://github.com/lksprado/demodados_orq)** (`demodados_orq`) | Orquestração (Airflow) | Agendamento e execução |

Conexão configurada via `~/.dbt/profiles.yml` (profile: `demodadosdw`), apontando para o
database `demodados` no Postgres local.

---

## 3. Objetivos do Produto

- Disponibilizar dados políticos **confiáveis e modelados dimensionalmente** (Star Schema)
  para consumo analítico (ex.: Power BI, Genie).
- Unificar Câmara e Senado em uma identidade de parlamentar única e estável, permitindo
  análises _cross-house_.
- Rastrear **governismo em nível de voto** (vote-level) para ambas as casas, permitindo
  calcular o percentual de alinhamento ao Governo por qualquer recorte temporal
  (ano, trimestre, legislatura).
- Seguir as boas práticas de estruturação do dbt
  ([how-we-structure](https://docs.getdbt.com/best-practices/how-we-structure/2-staging))
  e as convenções técnicas definidas em [`CONVENTIONS.md`](./CONVENTIONS.md).

---

## 4. Arquitetura de Dados

Arquitetura de **quatro camadas** analíticas, mais os seeds de referência:

| Camada | Pasta | Schema | Materialização | Prefixo |
|---|---|---|---|---|
| Staging (Bronze) | `models/staging/` | `staging` | table | `stg_` |
| Intermediate (Silver) | `models/intermediate/` | `intermediate` | view | `int_` |
| Marts (Gold) | `models/marts/dims/` + `models/marts/fcts/` | `marts` | view | `dim_`, `fct_` |
| Presentation (Platinum) | `models/presentation/` | `presentation` | table | `prs_` |
| Seeds | `seeds/` | `raw` | table | `raw_` |

**Responsabilidades por camada:**

- **Staging** — alinhamento 1:1 com as fontes brutas; limpeza e tipagem mínimas.
- **Intermediate** — passos de transformação _purpose-built_; alimentam dims/fcts ou
  outros intermediates.
- **Marts** — modelos dimensionais conformados: dimensões (`dim_`) e fatos (`fct_`),
  organizados por domínio. Materializados como _views_ (leves).
- **Presentation** — tabelas largas / OBT enriquecidas com contexto de negócio completo.
  Materializadas como _tables_.

O override em `macros/generate_schema_name.sql` garante que os modelos sejam criados no
schema definido em `+schema` diretamente (sem prefixar o schema do target).

---

## 5. Organização por Domínio

Os modelos são organizados por domínio/área temática. Domínios atuais:

| Domínio | Conteúdo | Status |
|---|---|---|
| **camara** | Câmara dos Deputados — deputados, legislaturas, votações, orientação de voto, votos | Ativo |
| **senado** | Senado Federal — senadores, tipos de decisão/entes/projetos, votações, orientação, votos | Ativo |
| **ecidadania** | Plataforma e-Cidadania — proposições, _bignumbers_, mais votados | Ativo |
| **ranking** | Ranking dos Políticos — scores de desempenho e métricas financeiras (deputados e senadores) | Ativo |
| **radar_congresso** | Radar Congresso — scores de alinhamento ao governo | **Desabilitado** (`+enabled: false` no `dbt_project.yml`) |

---

## 6. Padrões-chave de Design

- **Surrogate key unificada (`sk_parlamentar`)** — gerada a partir de `(id, tipo de casa
  legislativa)` para criar uma identidade estável e unificada entre Câmara e Senado.
  Todas as tabelas fato e a `dim_parlamentares` fazem join por essa chave.
- **Dimensão unificada de parlamentar** — `dim_parlamentares` (em `marts/dims/`) é a UNION
  de `int_deputados` e `int_senadores`. É a fonte para consultas _cross-house_.
- **Tabela de apresentação principal** — `prs_governismo` é a principal tabela voltada ao
  analista. Grão: **um voto por parlamentar por votação onde o Governo emitiu orientação**,
  cobrindo Câmara e Senado, com contexto de parlamentar e dados do presidente. Novas
  métricas cross-domínio devem partir dela.

Demais regras (geração de SK, linha _dummy_, tratamento de nulos, grão das fatos,
integração entre processos, bridge tables) estão em [`CONVENTIONS.md`](./CONVENTIONS.md).

---

## 7. Fontes de Dados

Todos os cinco domínios leem do schema `raw` no database `demodados`. Além das tabelas
ingeridas, há seeds estáticos usados como tabelas de referência:

- `raw_executivo_presidente` — presidentes do executivo (para contexto de governismo).
- `raw_legislaturas` — legislaturas.
- `raw_senado_tipos_decisao`, `raw_senado_tipos_entes`, `raw_senado_tipos_projetos` —
  tabelas de tipo do Senado.

---

## 8. Critérios de Sucesso (Definition of Done)

Um modelo está pronto quando atende ao _Definition of Done_ definido em
[`CONVENTIONS.md`](./CONVENTIONS.md):

1. **Grão declarado** como primeira linha da descrição no `schema.yml` (obrigatório para
   `fct_` e modelos Gold/Presentation).
2. **Testes obrigatórios passando** — `unique` e `not_null` na PK; `not_null` em todas as
   FKs das tabelas fato.
3. **Colunas documentadas em PT-BR** com descrição de negócio no `schema.yml`.
4. **Sem `SELECT *`** fora das CTEs iniciais de fonte; **sem `source()`** para
   dependências internas (apenas `ref()`).
5. **CTEs com nomes descritivos**, sem subqueries aninhadas.

---

## 9. Como Rodar e Escopo

```bash
dbt deps      # Instala packages após clonar
dbt seed      # Carrega seeds CSV no schema raw
dbt debug     # Valida a conexão
dbt run       # Roda todos os modelos
dbt test      # Roda todos os testes
dbt docs generate && dbt docs serve   # Documentação local
```

Os comandos assumem que você está dentro da pasta `demodadosdw` e que o `profiles.yml`
está configurado corretamente.

**Fora de escopo deste repositório:** a ingestão dos dados (responsabilidade do
[Ingestor](https://github.com/lksprado/demodados)) e a orquestração/agendamento
(responsabilidade do [Orquestrador](https://github.com/lksprado/demodados_orq)).
