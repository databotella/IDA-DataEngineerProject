# IDA DataMart - Análise de Desempenho de Operadoras de Telecomunicações

## Visão Geral

O **IDA DataMart** é um sistema de análise do **Índice de Desempenho no Atendimento (IDA)** da ANATEL, desenvolvido como solução de Data Engineering para monitoramento da qualidade do atendimento das principais operadoras de telecomunicações do Brasil.

### Objetivo Principal
Produzir uma **view SQL pivotada** que mostra a **taxa de variação mensal** da métrica "Taxa de Respondidas em 5 dias úteis", comparando o desempenho de cada grupo econômico com a média geral do mercado.

**Fórmula de Cálculo:** `((IDA mês atual - IDA mês anterior) / IDA mês anterior) × 100`

---

## Arquitetura da Solução

### Stack Tecnológica
- **Database:** PostgreSQL 17.5-bookworm
- **ETL:** Python 3.11.12-bookworm
- **Containerização:** Docker Compose
- **Modelo:** Data Mart Estrela
- **Fonte de Dados:** Portal Dados Abertos Gov.br

### Modelo de Dados (Estrela)

```
DIMENSÕES:
├── dim_tempo (granularidade mensal)
├── dim_grupo_economico (ALGAR, CLARO, VIVO, TIM, etc.)
├── dim_servico (SMP, STFC, SCM)
└── dim_variavel (métricas do IDA)

FATO CENTRAL:
└── fact_ida (todas as métricas de desempenho)

VIEW PRINCIPAL:
└── vw_taxa_variacao (resultado pivotado final)
```

---

## Dados e Métricas

### Serviços Analisados
- **SMP** - Serviço Móvel Pessoal (Telefonia Celular)
- **STFC** - Serviço Telefônico Fixo Comutado (Telefonia Fixa)
- **SCM** - Serviço de Comunicação Multimídia (Banda Larga Fixa)

### Grupos Econômicos
| Código | Nome Completo |
|--------|---------------|
| ALGAR | ALGAR TELECOM S/A |
| CLARO | CLARO S.A. |
| VIVO | TELEFÔNICA BRASIL S.A. |
| TIM | TIM S.A. |
| OI | OI S.A. |
| NET | NET SERVIÇOS DE COMUNICAÇÃO S.A. |
| SKY | SKY BRASIL SERVIÇOS LTDA. |
| EMBRATEL | EMPRESA BRASILEIRA DE TELECOMUNICAÇÕES S.A. |
| NEXTEL | NEXTEL TELECOMUNICAÇÕES LTDA. |
| SERCOMTEL | SERCOMTEL S/A TELECOMUNICAÇÕES |

### Métricas Principais do IDA
- **IDA** - Indicador de Desempenho no Atendimento
- **TAXA_RESP_5DIAS** - Taxa de Respondidas em 5 dias Úteis (métrica principal)
- **INDICE_RECL** - Índice de Reclamações
- **TAXA_REABERTAS** - Taxa de Reabertas
- **TAXA_RESP_PERIODO** - Taxa de Respondidas no Período

---

## Como Executar

### Pré-requisitos
- Docker e Docker Compose instalados
- Conexão com internet (para download dos dados)

### Execução Completa

```bash
# 1. Clone o repositório
git clone https://github.com/databotella/IDA-DataEngineerProject.git
cd IDA-DataEngineerProject

# 2. Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com suas configurações

# 3. Execute o pipeline completo
docker-compose up

# 4. Acesse a documentação (opcional)
# http://localhost:8756
```

### Configuração de Ambiente (.env)
```bash
# Database
DB_HOST=postgres
DB_PORT=5432
DB_NAME=idadatamart
DB_USER=postgres
DB_PASSWORD=sua_senha_aqui

# ETL
BATCH_SIZE=1000
MAX_RETRIES=3
LOG_LEVEL=INFO
API_KEY=sua_chave_api_dados_gov_br
```

---

## Componentes do Sistema

### 1. Inicialização do Banco (`init_db.py`)
- Recria o database `idadatamart`
- Aplica o schema completo (`schema_star.sql`)
- Configura dimensões e estruturas iniciais

### 2. Schema do Data Mart (`schema_star.sql`)
- **Modelo Estrela** otimizado para análise
- Índices estratégicos para performance
- View principal `vw_taxa_variacao` com cálculos complexos
- Documentação completa das tabelas e colunas

### 3. Pipeline ETL (`etl_ida.py`)

#### Arquitetura Orientada a Objetos:
- **`BaseExtractor`** - Classe abstrata para extratores
- **`ODSExtractor`** - Processamento especializado de arquivos ODS
- **`DataTransformer`** - Normalização e limpeza de dados
- **`PostgreSQLLoader`** - Carga otimizada com batch processing
- **`ETLPipeline`** - Orquestração completa do processo

#### Funcionalidades Avançadas:
- **Download direto da API** dados.gov.br (sem armazenamento em disco)
- **Processamento em memória** de arquivos ODS
- **Detecção automática** de headers e estruturas
- **Batch processing** para otimização de memória
- **Controle de duplicatas** via hash MD5
- **Logs detalhados** para auditoria

---

## Resultado Final

### Query Principal
```sql
SELECT * FROM ida.vw_taxa_variacao ORDER BY mes;
```

### Exemplo de Saída
| mes | taxa_variacao_media | algar | claro | vivo | tim | oi | net | sky |
|-----|--------------------:|------:|------:|-----:|----:|---:|----:|----:|
| 2018-02 | 10.15 | 1.10 | -2.50 | 0.80 | -1.20 | 3.40 | -0.60 | 2.10 |
| 2018-03 | 13.22 | -1.50 | 0.20 | -0.30 | 2.80 | -2.10 | 1.40 | -0.90 |

**Interpretação:**
- **taxa_variacao_media**: Variação percentual média de todas as operadoras
- **Colunas das operadoras**: Diferença entre a variação da operadora e a média geral
  - Valores **positivos**: Operadora teve desempenho **acima da média**
  - Valores **negativos**: Operadora teve desempenho **abaixo da média**

---

## Análise dos Dados

### Principais Insights Identificados

**Qualidade dos Dados:**
- Dados consistentes de 2017-2019
- Cobertura completa dos principais grupos econômicos
- Métricas padronizadas conforme regulamentação ANATEL

**Padrões Observados:**
- **Sazonalidade**: Variações mensais refletem campanhas de melhoria
- **Competitividade**: Diferenças significativas entre operadoras
- **Compliance**: Algumas operadoras consistentemente acima/abaixo da média

**Limitações:**
- Dados limitados ao período 2017-2019
- Algumas métricas com valores ausentes em períodos específicos
- Necessidade de contextualização com eventos do setor

---

## Arquitetura Técnica

### Pipeline ETL Detalhado

```
1. EXTRACT (Extração)
   ├── API dados.gov.br
   ├── Download direto para memória
   ├── Processamento ODS com pandas
   └── Validação de estrutura

2. TRANSFORM (Transformação)
   ├── Normalização de grupos econômicos
   ├── Padronização de variáveis
   ├── Limpeza de dados
   └── Criação de registros normalizados

3. LOAD (Carga)
   ├── Upsert de dimensões
   ├── Batch processing otimizado
   ├── Controle de duplicatas
   └── Logs de auditoria
```

### Otimizações Implementadas

**Performance:**
- Índices estratégicos nas chaves de junção
- Batch processing configurável
- Connection pooling PostgreSQL
- Processamento em memória (sem I/O disco)

**Confiabilidade:**
- Controle de duplicatas via hash MD5
- Transações ACID
- Retry automático em falhas
- Logs detalhados para debugging

**Escalabilidade:**
- Arquitetura modular e extensível
- Configuração via variáveis de ambiente
- Suporte a novos extratores de dados
- Schema evolutivo com comentários

---

## Documentação Técnica

### Documentação Automática
O projeto gera documentação técnica automaticamente via **pydoc**:

- **URL Local**: http://localhost:8756
- **VPS Demo**: http://212.85.21.2:8756
- **Arquivos**: 
  - `etl_ida.html` - Documentação do pipeline ETL
  - `init_db.html` - Documentação da inicialização

### Comentários no Código
- **Docstrings Python**: Padrão Google/Sphinx
- **Comentários SQL**: `COMMENT ON TABLE/COLUMN`
- **Type Hints**: Tipagem completa Python 3.11+

---

## Validação e Testes

### Health Checks Implementados
- Verificação de conectividade do banco
- Validação de schema existente
- Teste de inserção de dados

### Logs de Auditoria
```python
# Exemplo de log gerado
2025-06-01 10:30:15 - ETLPipeline - INFO - Recurso SMP 2018: 1247 registros
2025-06-01 10:30:16 - PostgreSQLLoader - INFO - Batch 1: 1000/1000 registros inseridos
2025-06-01 10:30:17 - ETLPipeline - INFO - Total de registros carregados: 15847
```

---

### Casos de Uso
1. **Análise Regulatória**: Monitoramento de compliance ANATEL
2. **Benchmarking**: Comparação entre operadoras
3. **Tendências**: Identificação de padrões temporais
4. **Alertas**: Detecção de degradação de performance

---

## Evolução e Roadmap

### Melhorias Implementadas
- Processamento 100% em memória (zero dependência de disco)
- Auto-discovery de recursos via API
- Tratamento robusto de estruturas variáveis dos arquivos ODS
- Documentação técnica automatizada

---

## Contribuição

Este projeto foi desenvolvido como demonstração de capacidades em **Data Engineering**. 

**Autor**: Henrique Botella  
**Data**: 01.06.2025  
**Versão**: 2.0.0

### Tecnologias Demonstradas
- **Python**: OOP, pandas, requests, psycopg2
- **SQL**: PostgreSQL, Window Functions, CTEs, Views
- **Docker**: Multi-container, health checks, volumes
- **Data Engineering**: ETL, Data Warehousing, API Integration

---

## Licença

Este projeto é disponibilizado para fins educacionais e de demonstração técnica.

---

**Links Úteis:**
- [Repositório GitHub](https://github.com/databotella/IDA-DataEngineerProject)
- [Documentação Técnica](http://212.85.21.2:8756)
- [Portal Dados Abertos](https://dados.gov.br)
- [ANATEL - IDA](https://www.anatel.gov.br)
