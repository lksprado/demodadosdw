# demodados
[English Version](https://github.com/lksprado/demodadosdw/blob/master/README-en.md)

## Projeto
Levantamento de dados para análises políticas  com foco em temas de cidadania, democracia e advocacy no Brasil.\
Este projeto utiliza Python, Airflow, PostgreSQL e dbt para estruturar, modelar e publicar dados públicos de forma acessível e confiável.

Este repositório trata exclusivamente do processo de Transformação dos dados.


## Estrutura de Repositórios do Projeto Demodados

**[Ingestor](https://github.com/lksprado/demodados)** Repo para pipelines de ingestão -> gera camadas Raw e Bronze \
**[Data Warehouse](https://github.com/lksprado/demodadosdw)** (Este repo): Repo para modelagem SQL com dbt -> gera Silver e Gold no DW.\
**[Orquestrador](https://github.com/lksprado/demodados_orq)** Repo para orquestração.


## Estrutura do Repositório `demodadosdw`
Este repositório segue, sempre que possível, as boas práticas da documentação oficial do dbt:  
https://docs.getdbt.com/best-practices/how-we-structure/2-staging

Principais diretórios:

- `models/`: modelos dbt organizados por domínio/área temática.
- `models/staging/`: modelos de _staging_ (camada Bronze), alinhados às fontes brutas.
- `models/intermediate/`: modelos dimensionais (camada Silver), alinhados ao Star Schema.
- `models/marts/`: modelos de negócio (camada Gold), voltados para consumo analítico.
- `seeds/`: arquivos estáticos (CSV) usados como tabelas de referência.
- `snapshots/`: definição de snapshots (quando aplicável).
- `tests/`: testes adicionais que complementam os _tests_ nativos do dbt.

## Como rodar localmente

1. Crie e ative um ambiente virtual Python (opcional, mas recomendado).
2. Instale as dependências do dbt e demais libs (por exemplo via `dbt deps`, se disponível).
3. Configure o perfil do dbt (`profiles.yml`) apontando para o seu PostgreSQL.
4. Valide o projeto:
   - `dbt debug`
5. Execute os modelos:
   - `dbt run`
6. Rode os testes:
   - `dbt test`

Os comandos acima assumem que você está dentro da pasta `demodadosdw` e que o `profiles.yml` foi configurado corretamente.

## Boas práticas e contribuição

- Manter nomes de modelos descritivos e consistentes com o domínio de negócio.
- Documentar sempre novas tabelas e campos usando `_<origem>__sources.yml`.
- Adicionar testes de qualidade de dados (único, não nulo, relacionamento) sempre que criar novos modelos.
- Preferir _incremental models_ quando fizer sentido para desempenho.
- Abrir _pull requests_ pequenos, focados em um conjunto de modelos ou um tema específico.
