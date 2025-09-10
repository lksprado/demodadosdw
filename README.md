# demodados
## Projeto
Levantamento de dados para análises políticas  com foco em temas de cidadania, democracia e advocacy no Brasil.\
Este projeto utiliza Python, Airflow, PostgreSQL e dbt para estruturar, modelar e publicar dados públicos de forma acessível e confiável.

Este repositório trata exclusivamente do processo de Transformação dos dados.


## Estrutura de Repositórios  do Projeto

**[Ingestor](https://github.com/lksprado/demodados)** Repo para pipelines de ingestão -> gera camadas Raw e Bronze
**[Data Warehouse](https://github.com/lksprado/demodadosdw)** (Este repo): Repo para modelagem SQL com dbt -> gera Silver e Gold no DW.\
**[Orquestrador](https://github.com/lksprado/demodados_orq)** Repo para orquestração.

## Ferramentas do Projeto
| Ferramenta    | Uso                                          |
|---------------|----------------------------------------------|
| Python        | Pipelines de ETL                             |
| Airflow       | Orquestração e agendamento de pipelines      |
| PostgreSQL    | Armazenamento e versionamento dos dados      |
| dbt           | Modelagem e documentação dos dados           |
| Pandera       | Validação de schema para carga raw           |


## Estrutura Repositório demodados
Segue documentação para estrutura de projetos com dbt: https://docs.getdbt.com/best-practices/how-we-structure/2-staging