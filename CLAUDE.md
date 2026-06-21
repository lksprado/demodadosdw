# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Two companion docs are the source of truth and should be consulted when in doubt:
- **[`prd.md`](./prd.md)** — purpose, scope, architecture, domain organization.
- **[`CONVENTIONS.md`](./CONVENTIONS.md)** — technical implementation rules (naming, surrogate keys, CTEs, nulls, tests, Definition of Done). Each rule is marked **obrigatório** (blocks Definition of Done) or **recomendação**.

## Project Overview

This is a **dbt** data transformation project for Brazilian political analysis (citizenship, democracy, advocacy). It handles **exclusively the Transformation layer**: reading data already ingested in the `raw` schema and modeling it up to the analytical layers (Silver and Gold).

It is part of a three-repo system, and is consumed as a **submodule** of the orchestrator:
- **[demodados](https://github.com/lksprado/demodados)** (Ingestor): Raw/Bronze ingestion pipelines.
- **demodadosdw** (this repo): SQL modeling with dbt — Silver and Gold layers.
- **[demodados_orq](https://github.com/lksprado/demodados_orq)** (Orchestrator): Airflow scheduling and execution.

The warehouse database is `demodados` in a local Postgres instance. Connection is configured via `~/.dbt/profiles.yml` (profile name: `demodadosdw`).

**Out of scope:** data ingestion (Ingestor repo) and orchestration/scheduling (Orchestrator repo).

## Commands

```bash
dbt deps              # Install packages after cloning
dbt seed              # Load CSV seeds to raw schema
dbt debug             # Validate connection

dbt run               # Run all models
dbt run -s <model>    # Run a specific model
dbt run -s staging.*  # Run a full layer
dbt test              # Run all tests
dbt test -s <model>   # Test a specific model

dbt docs generate && dbt docs serve   # Local documentation
```

## Architecture

Four-layer architecture, plus reference seeds:

| Layer | Folder | Schema | Materialization | Prefix |
|---|---|---|---|---|
| Staging (Bronze) | `models/staging/` | `staging` | table | `stg_` |
| Intermediate (Silver) | `models/intermediate/` | `intermediate` | view | `int_` |
| Marts (Gold) | `models/marts/dims/` + `models/marts/fcts/` | `marts` | view | `dim_`, `fct_` |
| Presentation (Platinum) | `models/presentation/` | `presentation` | table | `prs_` |
| Seeds | `seeds/` | `raw` | table | `raw_` |

**Layer responsibilities:**
- **Staging**: 1:1 alignment with raw sources; minimal cleaning and typing.
- **Intermediate**: purpose-built transformation steps. Models here feed dims/fcts or other intermediates.
- **Marts**: conformed dimensional models (`dim_`) and fact tables (`fct_`) — organized by domain. Materialized as views (lightweight).
- **Presentation**: wide/OBT tables enriched with full business context. Materialized as tables.

The `macros/generate_schema_name.sql` override ensures models are placed in the `+schema` config value directly (not prefixed with the target schema name).

The data is modeled dimensionally (Star Schema) for analytical consumption (e.g. Power BI, Genie), following the dbt [how-we-structure](https://docs.getdbt.com/best-practices/how-we-structure/2-staging) best practices.

## Data Sources & Domains

Models are organized by domain. All domains read from the `raw` schema in `demodados`:

| Domain | Content | Status |
|---|---|---|
| **camara** | Chamber of Deputies — deputies, legislatures, votes, voting orientation, votos | Active |
| **senado** | Federal Senate — senators, decision/entity/project types, votes, orientation, votos | Active |
| **ecidadania** | E-Cidadania platform — propositions, bignumbers, most-voted | Active |
| **ranking** | Ranking dos Políticos — performance scores and financial metrics (deputies & senators) | Active |
| **radar_congresso** | Radar Congresso — government alignment scores | **Disabled** (`+enabled: false` in `dbt_project.yml`) |

**Reference seeds** (static lookup tables): `raw_executivo_presidente` (presidents, for governism context), `raw_legislaturas` (legislatures), `raw_senado_tipos_decisao`, `raw_senado_tipos_entes`, `raw_senado_tipos_projetos`.

## Key Design Patterns

**Surrogate key**: `sk_parlamentar` is generated from `(id, legislative house type)` to create a stable, unified identity across the Câmara and Senado sources. All fact tables and `dim_parlamentares` join through this key.

**Unified parliamentarian dimension**: `dim_parlamentares` (in `marts/dims/`) is a UNION of `int_deputados` and `int_senadores`. Use this for cross-house queries.

**Primary presentation table**: `prs_governismo` is the main analyst-facing table. Grain: **one vote per parliamentarian per voting event where the Government issued an orientation**, covering Câmara (since 1991) and Senado, with parliamentarian context and president data. New cross-domain metrics should come from here.

## Naming Conventions

**Tables** — `obrigatório`:

| Type | Pattern | Example |
|---|---|---|
| Intermediate | `int_<domain>_<verb>` | `int_person_unioned`, `int_prescription_dedup` |
| Dimension | `dim_<entity>` | `dim_parlamentares` |
| Snapshot (dbt) | `snap_<entity>` | `snap_product` |
| Fact (transaction) | `fct_<process>` | `fct_prescription` |
| Presentation / OBT | `prs_<domain>_<description>` | `prs_governismo` |
| Bridge | `bridge_<entity_a>_<entity_b>` | `bridge_person_product` |

**Columns** — `obrigatório`:

- `snake_case` throughout.
- Surrogate key (PK of dim): `sk_<entity>` (e.g. `sk_parlamentar`, `sk_voto`).
- Natural key: `<entity>_id` (e.g. `deputado_id`) — kept as a dimension attribute for traceability/debug; **never used as a JOIN key in Gold**. One `_id` per source system for cross-source entities.
- FK in a fact: same name as the referenced dim's PK.
- Date: `data_<event>` (e.g. `data_votacao`); ETL load time is `data_carga`.
- Timestamp: `<event>_em` in UTC (e.g. `criado_em`, `atualizado_em`).
- Boolean: `fl_<condition>` (e.g. `fl_ativo`).
- Financial values: `vlr_<description>`; percentages: `perc_<description>`; social media: `redesocial_<platform>`.

## Modeling Conventions (from CONVENTIONS.md)

**Surrogate keys** — `obrigatório`. Every `dim_` has a SK as PK: immutable, unique per row, collision-free across sources, deterministic (MD5 hash, no lookup), with a **dummy value `'0'`** for unknown rows. Generate via `{{ dbt_utils.generate_surrogate_key([col1, col2]) }}`. `dim_dates` is the only exception (integer PK in `YYYYMMDD` format).

**CTE structure** — `obrigatório`. Descriptive CTE names, no nested subqueries. `SELECT *` only in the initial source CTEs or the final select CTE. Recommended column order: surrogate keys → natural keys → FKs → strings → numerics → booleans → dates → timestamps → audit/SCD columns.

**Fact grain** — `obrigatório`. Every `fct_` (and Gold/Presentation model) declares its grain as the **first line of the description** in `schema.yml`. Distinct grains never coexist in the same fact.

**Null handling** — `obrigatório`. Default vars in `dbt_project.yml`: `null_key: '0'`, `null_string: 'desconhecido'`.
- FK in a fact: never `NULL` — `COALESCE(..., {{ var('null_key') }})`. Every dimension carries a **dummy row** (`sk = '0'`, attributes `'desconhecido'`) loaded once and never changed, so unmatched LEFT JOINs land on it.
- Dimension text attribute: replace `NULL` with `{{ var('null_string') }}`.
- Fact metric: keep `NULL` (replacing with `0` distorts `SUM`/`AVG`).

`null_key` is the string `'0'` because SKs are MD5 text hashes — the dummy row must be the same `text` type.

**Dimensions** — `obrigatório`. Fixed hierarchies are flattened into the `dim_` itself (no snowflaking).

**Cross-process integration** — `obrigatório`. Gold models never JOIN two facts at atomic grain. Aggregate each fact independently first, then JOIN at the same grain, to avoid N:N cardinality and duplicated metrics.

**Bridge tables** — `obrigatório` when a dimension has multiple values per entity. The fact points to a group SK (`sk_<entity>_group`); the bridge expands the group with a `weighting_factor` (1/N). JOINs always go through the group SK, never the individual dimension SKs.

## Tests & Definition of Done

Required tests (`obrigatório`): `unique` + `not_null` on every `dim_`/`fct_` PK; `not_null` on every FK in facts. Recommended: `relationships` (FK → dim PK), `accepted_values` on classification columns.

A model is **done** when:
1. Grain declared as the first description line in `schema.yml` (required for `fct_` and Gold).
2. Required tests passing.
3. All columns documented in **PT-BR** with business descriptions in `schema.yml`.
4. No `SELECT *` outside initial source CTEs; no `source()` for internal dependencies (use `ref()` only).
5. Descriptive named CTEs, no nested subqueries.

Other quality rules: grants centralized in `dbt_project.yml` `vars` (no hardcoded GUIDs in SQL).

## Packages

- `dbt-labs/dbt_utils` (v1.3.0) — surrogate keys, union helpers
- `metaplane/dbt_expectations` (v0.10.8) — extended data quality tests
- `godatadriven/dbt_date` (v0.15.0) — date utilities (timezone: `America/Sao_Paulo`)
