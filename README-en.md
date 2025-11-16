# demodados
[Portuguese Version](https://github.com/lksprado/demodadosdw/blob/master/README.md)

## Project
Data collection and preparation for political analysis focused on citizenship, democracy, and advocacy topics in Brazil.\
This project uses Python, Airflow, PostgreSQL, and dbt to structure, model, and publish public data in an accessible and reliable way.

This repository is exclusively dedicated to the data transformation layer.

## Demodados Project Repository Structure

**[Ingestor](https://github.com/lksprado/demodados)** Repository for ingestion pipelines → produces Raw and Bronze layers.\
**[Data Warehouse](https://github.com/lksprado/demodadosdw)** (This repo): Repository for SQL modeling with dbt → produces Silver and Gold layers in the DW.\
**[Orchestrator](https://github.com/lksprado/demodados_orq)** Repository for orchestration.


## `demodadosdw` Repository Structure
This repository follows, whenever possible, the best practices from the official dbt documentation:  
https://docs.getdbt.com/best-practices/how-we-structure/2-staging

Main directories:

- `models/`: dbt models organized by domain / business area.
- `models/staging/`: staging models (Bronze layer), aligned with raw sources.
- `models/intermediate/`: dimensional models (Silver layer), aligned with the star schema.
- `models/marts/`: business models (Gold layer), focused on analytical consumption.
- `seeds/`: static files (CSV) used as reference tables.
- `snapshots/`: snapshot definitions (when applicable).
- `tests/`: additional tests that complement native dbt tests.

## How to Run Locally

1. Create and activate a Python virtual environment (optional but recommended).
2. Install dbt and other dependencies (for example via `dbt deps`, when available).
3. Configure the dbt profile (`profiles.yml`) pointing to your PostgreSQL instance.
4. Validate the project:
   - `dbt debug`
5. Run the models:
   - `dbt run`
6. Execute the tests:
   - `dbt test`

The commands above assume you are inside the `demodadosdw` folder and that `profiles.yml` is correctly configured.

## Best Practices and Contribution

- Keep model names descriptive and consistent with the business domain.
- Always document new tables and fields using `_<source>__sources.yml`.
- Add data quality tests (unique, not null, relationship) whenever you create new models.
- Prefer incremental models when it makes sense for performance.
- Open small pull requests, focused on a set of models or a specific theme.

