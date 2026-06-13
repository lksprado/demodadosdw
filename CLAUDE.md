# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **dbt** data transformation project for Brazilian political analysis. It is part of a three-repo system:
- **demodados** (Ingestor): Raw/Bronze ingestion pipelines
- **demodadosdw** (this repo): SQL modeling — Silver and Gold layers
- **demodados_orq**: Orchestration

The warehouse database is `demodados` in BigQuery. Connection is configured via `~/.dbt/profiles.yml` (profile name: `demodadosdw`).

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

Four-layer architecture:

| Layer | Folder | Schema | Materialization | Prefix |
|---|---|---|---|---|
| Staging (Bronze) | `models/staging/` | `staging` | table | `stg_` |
| Intermediate (Silver) | `models/intermediate/` | `intermediate` | view | `int_dim_`, `int_fct_`, `int_map_` |
| Marts (Gold) | `models/marts/dims/` + `models/marts/fcts/` | `marts` | view | `dim_`, `fct_` |
| Presentation (Platinum) | `models/presentation/` | `presentation` | table | `prs_` |
| Seeds | `seeds/` | `raw` | table | `raw_` |

**Layer responsibilities:**
- **Intermediate**: pure transforms and key mappings — never queried directly by analysts. Models here feed dims/fcts or other intermediates.
- **Marts**: conformed dimensional models (`dim_`) and fact tables (`fct_`) — analyst-accessible, organized by domain. Materialized as views (lightweight).
- **Presentation**: wide/OBT tables enriched with full business context — the primary analyst-facing layer. Materialized as tables.

The `macros/generate_schema_name.sql` override ensures models are placed in `+schema` config value directly (not prefixed with the target schema name).

## Data Sources

Five source domains, all reading from the `raw` schema in `demodados`:

- **camara**: Chamber of Deputies — deputies, legislatures, votes, voting orientation
- **senado**: Federal Senate — senators, proposition types, decision types
- **radar**: Radar Congresso — government alignment scores for deputies and senators
- **ecidadania**: E-Cidadania petition platform — proposals, voting counts
- **ranking**: Ranking dos Políticos — politician performance scores and financial metrics

## Key Design Patterns

**Surrogate key**: `sk_parlamentar` is generated in `int_map_parlamentares` from (id, house type) to create a stable, unified identity across the Câmara and Senado sources. All fact tables and `dim_parlamentares` join through this key.

**Unified parliamentarian dimension**: `dim_parlamentares` (in `marts/dims/`) is a UNION of `int_dim_deputados` and `int_dim_senadores`. Use this for cross-house queries.

**Primary presentation table**: `prs_governismo` is the main analyst-facing table — vote-level governism tracking for both chambers with parliamentarian context, party, UF, and president data. New cross-domain metrics should land here.

**Temporal granularity**: `prs_governismo` exposes `ano` and `trimestre` columns, enabling aggregation at any desired period. Governism % = `count_if(alinhado_ao_governo=true) / count_if(alinhado_ao_governo is not null)`.

**Multi-source enrichment**: Dimensions fill gaps using secondary sources (e.g., social media links in `int_dim_deputados` fall back to Ranking data when official API data is null).

## Naming Conventions

- Columns: `snake_case`
- Surrogate keys: `sk_<entity>` (e.g., `sk_parlamentar`)
- Social media columns: `redesocial_<platform>` (e.g., `redesocial_x_twitter`)
- Financial values: `vlr_<description>` (e.g., `vlr_mandato_disponivel_total`)
- Party alignment percentages: `perc_governismo`
- Timestamps: `data_carga` for ETL load time

## Packages

- `dbt-labs/dbt_utils` (v1.3.0) — surrogate keys, union helpers
- `metaplane/dbt_expectations` (v0.10.8) — extended data quality tests
- `godatadriven/dbt_date` (v0.15.0) — date utilities (timezone: `America/Sao_Paulo`)
