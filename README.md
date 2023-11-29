# Ecommerce Analytics

## Objetivo

O objetivo deste projeto é realizar uma análise exploratória de dados de uma empresa de e-commerce, com o intuito de identificar possíveis oportunidades de negócio.

Além disso, vamos construir um Lakehouse utilizando o Delta Lake, que é uma tecnologia open source desenvolvida pela Databricks, que combina os melhores aspectos dos Data Lakes e Data Warehouses, permitindo que os dados sejam armazenados em um formato colunar altamente otimizado, com suporte a ACID transactions, schema enforcement e versioning.

### Objetivos secundários

1) Trabalhar com commites semânticos (como Conventional Commits)

O que são commits semânticos?
Commits semânticos são uma convenção para adicionar metadados aos commits. O objetivo é facilitar a criação de ferramentas automatizadas para analisar o histórico do projeto. 
 
O formato é o seguinte:
```bash
<tipo>: <descrição>
```

Os tipos disponíveis são:
- feat: nova funcionalidade
- fix: correção de bug
- docs: alteração na documentação
- style: formatação de código, ponto e vírgula faltando, etc; - não altera o significado
- refactor: refatoração de código, sem alterar a semântica
- test: adição ou correção de testes
- chore: alterações no processo de build, atualização de - dependências, etc; não altera o código em si

Exemplo 1: 
```bash
feat: acidicionado camada de Delta Lake
```

Exemplo 2:
```bash
fix: corrigido bug de escrita no Delta Lake
```

Exemplo 3:
```bash
docs: adicionado documentação sobre Delta Lake
```

Para isso vamos usar a seguinte estratégia

Vamos utilizar o [Commitizen](https://github.com/commitizen-tools/commitizen) para padronizar os commits.

```bash
poetry add commitizen --group dev
```

Na maior parte do tempo só vamos usar o comando `cz bump` para fazer os commits

```bash
cz bump
```
