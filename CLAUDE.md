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

Three-layer medallion architecture:

| Layer | Folder | Schema | Materialization | Prefix |
|---|---|---|---|---|
| Staging (Bronze) | `models/staging/` | `staging` | table | `stg_` |
| Intermediate (Silver) | `models/intermediate/` | `intermediate` | view | `int_dim_`, `int_fct_`, `int_map_` |
| Marts (Gold) | `models/marts/` | `marts` | table | `mrt_` |
| Seeds | `seeds/` | `raw` | table | `raw_` |

The `macros/generate_schema_name.sql` override ensures models are placed in `+schema` config value directly (not prefixed with the target schema name).

## Data Sources

Five source domains, all reading from the `raw` schema in `demodados`:

- **camara**: Chamber of Deputies — deputies, legislatures, votes, voting orientation
- **senado**: Federal Senate — senators, proposition types, decision types
- **radar**: Radar Congresso — government alignment scores for deputies and senators
- **ecidadania**: E-Cidadania petition platform — proposals, voting counts
- **ranking**: Ranking dos Políticos — politician performance scores and financial metrics

## Key Design Patterns

**Surrogate key**: `sk_parlamentar` is generated in `int_map_parlamentares` from (id, house type) to create a stable, unified identity across the Câmara and Senado sources. All fact tables and the unified dimension `int_dim_parlamentares` join through this key.

**Unified parliamentarian dimension**: `int_dim_parlamentares` is a UNION of `int_dim_deputados` and `int_dim_senadores`. Use this for cross-house queries.

**One Big Table**: `mrt_obt_parlamentares` is a wide denormalized table joining all dimensions and facts — intended as the primary analyst-facing table. New cross-domain metrics should land here.

**Temporal granularity**: Government alignment facts exist at two levels — `_total` (all-time aggregates) and `_trimestre` (quarterly). Both exist for deputies and senators.

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
