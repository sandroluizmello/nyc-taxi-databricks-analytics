# 🚕 NYC Yellow Taxi Analytics & Prediction Pipeline

> Pipeline completo de análise, feature engineering e previsão de tarifas usando PySpark, Delta Lake, e Machine Learning em Databricks

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Objetivos](#objetivos)
- [Stack Tecnológico](#stack-tecnológico)
- [Arquitetura](#arquitetura)
- [Dataset](#dataset)
- [Estrutura de Pastas](#estrutura-de-pastas)
- [Fases do Projeto](#fases-do-projeto)
- [Requisitos](#requisitos)
- [Setup Inicial](#setup-inicial)
- [Como Rodar](#como-rodar)
- [Resultados Esperados](#resultados-esperados)
- [Contribuições](#contribuições)
- [Licença](#licença)

---

## 🎯 Visão Geral

Este projeto implementa um **pipeline end-to-end** de dados em Databricks (Free Tier) que demonstra as melhores práticas para:

✅ Ingestão de dados em larga escala (7,39GB CSV → 1,06GB Delta Lake, ~86% de compressão)
✅ Processamento distribuído com PySpark
✅ Armazenamento otimizado com Delta Lake
✅ Engenharia de features de qualidade
✅ Análise exploratória e visualizações
✅ Machine Learning com versionamento (MLflow)
✅ Validação temporal de modelos
✅ Automação e deployment em produção

**Público-alvo:** Data Engineers, Data Scientists, Iniciantes em Big Data

**Duração esperada:** 6-8 semanas (1-2h/dia)

> 💡 **Estratégia de execução:** Este projeto é desenvolvido **100% dentro do Databricks** — código, testes, execução e commits (via Databricks Repos). Não é necessário instalar Python, PySpark, Git ou qualquer ferramenta localmente. O único passo fora do Databricks é baixar os CSVs originais do Kaggle para depois fazer upload na plataforma. Veja detalhes em [Requisitos](#requisitos).

---

## 🎓 Objetivos

### Objetivo Principal
Criar um sistema de **previsão de tarifas de taxi** (Fare Amount) em NYC com modelo de ML treinado em ~46,9 milhões de registros reais.

### Objetivos Secundários
- ✅ Entender fluxo completo de dados em Databricks
- ✅ Praticar PySpark para transformações distribuídas
- ✅ Dominar Delta Lake e otimizações
- ✅ Realizar análise exploratória profissional
- ✅ Treinar múltiplos modelos de regressão
- ✅ Versionar código e modelos (GitHub + MLflow)
- ✅ Implementar pipeline automatizado

### Resultado Final
**Modelo de previsão de tarifa** com:
- RMSE esperado: ~$2.00-2.50
- R² esperado: ~0.90-0.95
- 37 features no total (17 novas criadas na Fase 3)
- Versionado em MLflow
- Testado com validação temporal

---

## 🛠️ Stack Tecnológico

```
┌─────────────────────────────────────────────────────────┐
│                     DATABRICKS (Free Tier)              │
├─────────────────────────────────────────────────────────┤
│  ▶ Cluster Spark (computação distribuída)              │
│  ▶ SQL Warehouse (queries interativas)                 │
│  ▶ Repos (sincronização GitHub)                        │
│  ▶ Jobs (automação e agendamento)                      │
│  ▶ MLflow (versionamento de modelos)                   │
│  ▶ Visualizações nativas                               │
└─────────────────────────────────────────────────────────┘

┌──────────────────────────┐
│   PROCESSAMENTO & DADOS  │
├──────────────────────────┤
│  • PySpark (ETL)         │
│  • SQL (queries)         │
│  • Delta Lake (storage)  │
│  • Parquet (compressão)  │
└──────────────────────────┘

┌──────────────────────────┐
│    VERSIONAMENTO         │
├──────────────────────────┤
│  • GitHub (código)       │
│  • Databricks Repos      │
│  • MLflow (modelos)      │
│  • Delta (dados)         │
└──────────────────────────┘

┌──────────────────────────┐
│   MACHINE LEARNING       │
├──────────────────────────┤
│  • Scikit-learn          │
│  • Linear Regression     │
│  • Random Forest         │
│  • Gradient Boosting     │
└──────────────────────────┘

┌──────────────────────────┐
│   VISUALIZAÇÃO           │
├──────────────────────────┤
│  • Databricks charts     │
│  • Matplotlib/Seaborn    │
│  • SQL aggregations      │
└──────────────────────────┘
```

---

## 🏗️ Arquitetura

```
                    ┌─────────────────┐
                    │  GitHub Repo    │
                    │ (Código fonte)  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Databricks Repos│
                    │  (Sincronizado) │
                    └────────┬────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
      ┌──────────┐    ┌──────────┐    ┌──────────┐
      │Notebook 1│    │Notebook 2│ ...│Notebook 7│
      │  Setup   │    │Ingestão  │    │ Deploy   │
      └────┬─────┘    └────┬─────┘    └────┬─────┘
           │               │               │
           └───────────────┼───────────────┘
                           ▼
                    ┌──────────────────┐
                    │    PySpark       │
                    │  Transformações  │
                    └────────┬─────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
      ┌───────────┐   ┌──────────────┐  ┌────────────┐
      │  DBFS     │   │ Delta Lake   │  │  Parquet   │
      │ (raw)     │   │ (processed)  │  │(compressed)│
      └───────────┘   └──────────────┘  └────────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
      ┌──────────┐    ┌──────────┐    ┌──────────┐
      │   EDA    │    │Analytics │    │   ML     │
      │Analysis  │    │Dashboard │    │ MLflow   │
      └──────────┘    └──────────┘    └──────────┘
            │                ▼                │
            │         ┌──────────────┐       │
            └────────▶│  Dashboards  │◀──────┘
                      │Visualizações │
                      └──────────────┘
```

---

## 📊 Dataset

### Contexto Oficial (segundo o autor do dataset no Kaggle)

O dataset é mantido pela **NYC Taxi & Limousine Commission (TLC)**, que disponibiliza dados de 4 tipos de veículo (Yellow Taxi, Green Taxi, FHV, entre outros). Este projeto usa exclusivamente **Yellow Taxi**.

Dois pontos importantes esclarecidos pelo autor do dataset:

1. **O recorte Jan/2015 + Jan-Mar/2016 é intencional**, não uma amostra aleatória — foi escolhido deliberadamente para permitir tanto análises de **clustering espacial** quanto de **série temporal**.
2. **Este dataset usa o formato antigo da TLC**, com coordenadas de latitude/longitude de pickup e dropoff. Versões mais recentes do site oficial da TLC substituíram essas coordenadas por **IDs de zona** (por privacidade), o que inviabilizaria os exercícios de clustering geoespacial que fazem parte do propósito do dataset. Por isso, o autor optou por manter esse formato mais antigo.

> 💡 Isso explica por que temos `pickup_latitude`/`pickup_longitude` diretamente disponíveis — em datasets mais recentes da TLC, isso não existiria mais.

### Características

| Aspecto | Detalhes |
|--------|----------|
| **Fonte** | [Kaggle - NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data) |
| **Veículo** | Yellow Taxi (táxis amarelos icônicos de NYC, hail-only) |
| **Período** | Jan/2015 + Jan-Mar/2016 (recorte intencional do autor, não consecutivo) |
| **Volume** | ~46,9 milhões de registros (confirmado após ingestão) |
| **Tamanho Original** | 7,39GB (4 arquivos CSV — confirmado) |
| **Tamanho Processado** | 1,06GB (Delta Lake comprimido — confirmado) |
| **Compressão** | ~86% de redução (confirmado) |

### Arquivos do Dataset

O dataset no Kaggle disponibiliza **4 arquivos CSV**, cobrindo um mês de 2015 e três meses de 2016:

```
yellow_tripdata_2015-01.csv   (Janeiro/2015)
yellow_tripdata_2016-01.csv   (Janeiro/2016)
yellow_tripdata_2016-02.csv   (Fevereiro/2016)
yellow_tripdata_2016-03.csv   (Março/2016)
```

> 💡 **Sobre análises temporais:** os 3 arquivos de 2016 (Jan, Fev, Mar) formam uma sequência contínua, boa para tendência mês a mês. O arquivo de 2015 é útil para comparação ano a ano (ex: Janeiro/2015 vs Janeiro/2016 — como fizemos na Fase 2), mas não deve ser tratado como parte de uma sequência com os meses de 2016.

### ⚠️ Correção Importante: `trip_distance` está em MILHAS, não km

O dicionário oficial de dados da NYC TLC confirma:

> *"Trip_distance: The elapsed trip distance in **miles** reported by the taximeter."*

Isso corrige uma suposição implícita nas análises da Fase 2 (onde tratamos os valores como km). **Os cálculos continuam corretos** — só o rótulo da unidade precisa ser ajustado. Ex: mediana de `1.70` é **1,70 milhas** (≈ 2,74 km), não 1,70 km.

> 📌 Na Fase 3, vamos criar uma coluna adicional `trip_distance_km` (conversão de milhas para km) para facilitar leitura, mantendo a coluna original (`trip_distance`, em milhas) intacta.

### Colunas Principais (Dicionário Oficial da NYC TLC)

```
Coluna                  | Tipo      | Descrição
------------------------|-----------|----------------------------------------
VendorID                | int       | Fornecedor do TPEP: 1=Creative Mobile
                        |           | Technologies, 2=VeriFone Inc.
tpep_pickup_datetime     | timestamp | Data/hora em que o taxímetro foi ativado
tpep_dropoff_datetime    | timestamp | Data/hora em que o taxímetro foi desativado
passenger_count          | int       | Nº de passageiros (valor inserido pelo motorista)
trip_distance            | float     | Distância percorrida em MILHAS (não km!)
pickup_longitude         | float     | Longitude de onde o taxímetro foi ativado
pickup_latitude          | float     | Latitude de onde o taxímetro foi ativado
dropoff_longitude        | float     | Longitude de onde o taxímetro foi desativado
dropoff_latitude         | float     | Latitude de onde o taxímetro foi desativado
RatecodeID                | int       | Tarifa final: 1=Standard, 2=JFK, 3=Newark,
                        |           | 4=Nassau/Westchester, 5=Negotiated fare
store_and_fwd_flag       | string    | Y=corrida guardada no veículo por falta de
                        |           | conexão antes de enviar; N=enviada em tempo real
payment_type             | int       | 1=Cartão de crédito, 2=Dinheiro, 3=Sem cobrança,
                        |           | 4=Disputa, 5=Desconhecido, 6=Corrida anulada
fare_amount              | float     | Tarifa calculada pelo taxímetro (tempo+distância)
extra                    | float     | Taxas extras (rush hour $0.50, noturna $1.00)
mta_tax                  | float     | Taxa fixa de $0.50 (MTA)
improvement_surcharge    | float     | Taxa fixa de $0.30 (vigente desde 2015)
tip_amount               | float     | Gorjeta — só registra gorjetas em CARTÃO;
                        |           | gorjetas em dinheiro NÃO aparecem aqui
tolls_amount             | float     | Total de pedágios pagos na corrida
total_amount             | float     | Total cobrado do passageiro — NÃO inclui
                        |           | gorjetas em dinheiro
```

> ⚠️ **Dois pontos de atenção para a Fase 3 e Fase 5 (ML):**
> 1. `tip_amount` e `total_amount` só refletem gorjetas pagas em **cartão** — corridas pagas em dinheiro sempre mostram `tip_amount = 0`, o que não significa "sem gorjeta", e sim "gorjeta não registrada pelo sistema". Modelos de previsão de gorjeta devem considerar filtrar apenas `payment_type = 1` (cartão) para não aprender um padrão artificial.
> 2. `RatecodeID` já identifica viagens de aeroporto diretamente (`2 = JFK`, `3 = Newark`) — não é necessário calcular isso via distância até coordenadas do aeroporto, como havíamos cogitado inicialmente.

### Download

```bash
# Fazer download localmente (manual)
# https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data

# Ou usar Kaggle CLI
kaggle datasets download -d elemento/nyc-yellow-taxi-trip-data

# Extrair
unzip nyc-yellow-taxi-trip-data.zip
```

---

## 📁 Estrutura de Pastas

> ⚠️ **Importante:** este projeto tem **duas estruturas separadas, com propósitos diferentes** — não são cópias uma da outra:
>
> | | O que guarda | Onde vive | Vai para o GitHub? |
> |---|---|---|---|
> | **1. Código** | Notebooks, funções Python, documentação | GitHub ↔ Databricks Repos (sincronizados automaticamente) | ✅ Sim |
> | **2. Dados** | CSVs, tabelas Delta Lake, modelos treinados | Unity Catalog Volume (armazenamento interno da plataforma) | ❌ Nunca |
>
> **Por quê a separação?** O GitHub é feito para versionar **código** (arquivos de texto, pequenos). Nossos dados chegam a GBs — subir isso pro Git seria péssima prática (limite de 100MB/arquivo, deixaria o repo lento e pesado) e não traria benefício algum, já que o Delta Lake tem seu próprio versionamento interno. Por isso: código no GitHub, dados no Volume. São dois "HDs" diferentes que o mesmo notebook acessa.

### 1️⃣ Estrutura de Código (GitHub ↔ Databricks Repos)

Isso é **um único repositório**, espelhado nos dois lugares. Quando você conecta o GitHub ao Databricks Repos, ele **clona automaticamente** essa estrutura — você não precisa recriá-la manualmente em nenhum dos dois lados.

```
nyc-taxi-databricks-analytics/          (GitHub e Databricks Repos — mesma coisa)
├── .gitignore                          # Arquivos ignorados no Git
├── README.md                           # Este arquivo
├── LICENSE                             # Licença MIT
├── requirements.txt                    # Dependências Python
│
├── docs/                               # Documentação do projeto
│   ├── ARCHITECTURE.md
│   ├── SETUP_GUIDE.md
│   ├── DATA_DICTIONARY.md
│   ├── PHASES_OVERVIEW.md
│   ├── PHASE_1_INGESTAO.md
│   ├── PHASE_2_EDA.md
│   ├── PHASE_3_TRANSFORM.md
│   ├── PHASE_4_INSIGHTS.md
│   ├── PHASE_5_ML_MODELS.md
│   ├── GITHUB_WORKFLOW.md
│   └── TROUBLESHOOTING.md
│
├── notebooks/                          # Notebooks Databricks
│   ├── 01_setup.py
│   ├── 02_ingestao.py
│   ├── 03_eda.py
│   ├── 04_transform.py
│   ├── 05_analise.py
│   ├── 06_ml.py
│   └── 07_deploy.py
│
├── src/                                # Código modular (funções reutilizáveis)
│   ├── __init__.py
│   ├── config.py
│   ├── data_ingestion.py
│   ├── transformations.py
│   ├── ml_utils.py
│   └── monitoring.py
│
├── config/                             # Arquivos config (apenas texto/YAML)
│   ├── paths.yaml                      # Guarda os CAMINHOS do DBFS como texto
│   ├── features.yaml
│   └── ml_config.yaml
│
└── tests/                              # Testes (rodados no Databricks)
    ├── test_transformations.py
    └── test_ml.py
```

> 📌 Repare que `config/paths.yaml` guarda apenas o **caminho** (texto) de onde os dados ficam no Volume — ex: `"/Volumes/workspace/default/nyc_taxi/raw/delta/taxi_2016-01"`. Isso sim pode ir pro GitHub, porque é só uma referência, não o dado em si.

### 2️⃣ Estrutura de Dados (Unity Catalog Volume — não sincroniza com GitHub)

> ⚠️ **Atualização:** o DBFS tradicional (`/mnt/...`) vem **desativado por padrão** no Databricks Free Edition (`"Public DBFS root is disabled"`). O armazenamento de dados agora usa **Unity Catalog Volumes**, o padrão atual da plataforma. A lógica é a mesma — só muda o prefixo do caminho.

Essa estrutura **não existe no GitHub**. Ela é criada **pelos próprios notebooks**, quando você os executa (via `spark.sql("CREATE VOLUME ...")`, `dbutils.fs.mkdirs()`, `df.write.format("delta").save(...)`, etc). É o "armazém" de dados que o código, versionado no GitHub, lê e escreve durante a execução.

Criamos um Volume chamado `nyc_taxi`, dentro do catálogo `workspace` e schema `default` (ambos padrão do Free Edition):

```
/Volumes/workspace/default/nyc_taxi/    (Só existe dentro do Databricks — NUNCA no GitHub)
├── raw/
│   ├── csv/                            # CSVs temporários (upload manual)
│   │   ├── yellow_tripdata_2015-01.csv (deletado após fase 1)
│   │   ├── yellow_tripdata_2016-01.csv (deletado após fase 1)
│   │   ├── yellow_tripdata_2016-02.csv (deletado após fase 1)
│   │   └── yellow_tripdata_2016-03.csv (deletado após fase 1)
│   └── delta/                          # Delta Lake raw
│       ├── taxi_2015-01/
│       ├── taxi_2016-01/
│       ├── taxi_2016-02/
│       └── taxi_2016-03/
│
├── processed/
│   ├── cleaned/                        # Dados limpos
│   ├── featured/                       # Com features
│   │   └── taxi_featured/
│   └── combined/                       # Os 4 arquivos combinados
│       └── taxi_combined_all/
│
├── analytics/
│   └── aggregations/                   # Agregações para dashboard
│       ├── hourly_trends.parquet
│       ├── daily_trends.parquet
│       ├── monthly_trends.parquet
│       ├── top_routes.parquet
│       ├── payment_analysis.parquet
│       └── zone_analysis.parquet
│
└── ml/
    ├── models/                         # Modelos treinados (MLflow)
    ├── predictions/                    # Previsões em batch
    └── experiments/                    # Tracking MLflow
```

**Como visualizar essa estrutura na interface:** menu lateral → **Catalog** → catálogo `workspace` → schema `default` → **Volumes** → `nyc_taxi`. As pastas aparecem navegáveis ali, como um explorador de arquivos.

### 🔗 Como as Duas Estruturas se Relacionam

```
GitHub / Databricks Repos                    Unity Catalog Volume (Databricks)
────────────────────────                    ──────────────────────────────────
notebooks/02_ingestao.py   ──── contém ────▶  código que LÊ
  (só código, poucos KB)                       "/Volumes/workspace/default/nyc_taxi/raw/csv/..."
                                                e ESCREVE em
                                                "/Volumes/workspace/default/nyc_taxi/raw/delta/..."
                                                (dados, GBs — fica só aqui)
```

O notebook (código) **fica no GitHub**. Quando ele **executa** dentro do Databricks, ele lê e grava arquivos no Volume — mas essas GBs de dados nunca "sobem" para o GitHub, só o texto do código que as manipula.

---

## 📊 Fases do Projeto

### ⏱️ Timeline Total: 6-8 semanas

| # | Fase | Duração | Tarefas | Status |
|---|------|---------|--------|--------|
| 0 | **Setup & Preparação** | 2-3h | 7 | ✅ Concluído |
| 1 | **Ingestão & Delta Lake** | 2-4h | 9 | ✅ Concluído |
| 2 | **Análise Exploratória (EDA)** | 6-8h | 10 | ✅ Concluído |
| 3 | **Feature Engineering** | 10-14h | 12 | ✅ Concluído |
| 4 | **Análise & Visualização** | 8-10h | 10 | ✅ Concluído |
| 5 | **Machine Learning (MLflow)** | 14-18h | 12 | ⬜ Não iniciado |
| 6 | **Validação & Avaliação** | 2-3h | 3 | ⬜ Não iniciado |
| 7 | **Deployment & Automação** | 4-6h | 5 | ⬜ Não iniciado |

---

### 📍 Fase 0: Setup & Preparação (2-3 horas) ✅ Concluído

**Objetivo:** Preparar ambiente Databricks, GitHub e estrutura inicial

**Tarefas:**
- [x] Criar repositório GitHub (via navegador)
- [x] Conectar GitHub ao Databricks Repos
- [x] Estruturar pastas do projeto (direto no Databricks Repos)
- [x] Criar estrutura de dados (Unity Catalog Volume `nyc_taxi`)
- [x] Avaliar ambiente Databricks (Free Edition, Serverless)
- [x] Setup MLflow experiment (validado — versão 3.8.1)
- [x] Documentação inicial (este README)

**Output:** Ambiente pronto, GitHub sincronizado, MLflow funcionando

**Arquivo:** `notebooks/01_setup.py`

---

### 📍 Fase 1: Ingestão & Conversão para Delta Lake (2-4 horas) ✅ Concluído

**Objetivo:** Upload de 4 CSVs e transformação em Delta Lake comprimido

**Tarefas:**
- [x] Upload dos 4 CSVs para o Volume (~7,39GB reais)
- [x] Exploração rápida do schema (20 colunas)
- [x] Transformação CSV → Delta (limpeza mínima: `trip_distance > 0`, `fare_amount > 0`, `tpep_pickup_datetime` não nulo)
- [x] Validação dos dados convertidos (linhas + dias distintos por partição)
- [x] Deleção dos CSVs originais (7,39GB liberados)
- [x] Relatório de ingestão

**Output:** 4 tabelas Delta, ~46,9M registros preservados

**Arquivo:** `notebooks/02_ingestao.py`

**Resultados Reais:**
```
Arquivo    Linhas          CSV      Delta    Compressão
─────────────────────────────────────────────────────
2015-01    12.664.586      1,99 GB  0,31 GB    84%
2016-01    10.837.487      1,71 GB  0,24 GB    86%
2016-02    11.309.140      1,78 GB  0,25 GB    86%
2016-03    12.134.119      1,91 GB  0,26 GB    86%
─────────────────────────────────────────────────────
Total      46.945.332      7,39 GB  1,06 GB    86%
```

> 📌 **Nota:** a estimativa inicial no planejamento (680M registros) foi baseada em uma suposição incorreta sobre o tamanho médio de cada arquivo. O volume real do dataset é ~46,9M registros — ainda assim, um volume expressivo para exercitar processamento distribuído com PySpark e Delta Lake.

> ⚠️ **Limitação encontrada:** no Databricks Serverless Compute, chamadas que usam a API de RDD (como `df.rdd.getNumPartitions()`) não são suportadas (`PySparkNotImplementedError: NOT_IMPLEMENTED`). A solução foi usar apenas a API de DataFrame/SQL (ex: `df.select(...).distinct().count()`), que é 100% compatível com Serverless. Essa restrição vale para todas as próximas fases.

---

### 📍 Fase 2: Análise Exploratória (EDA) (6-8 horas) ✅ Concluído

**Objetivo:** Entender dados através de análise exploratória detalhada

**Tarefas:**
- [x] Carregar dados Delta em tabela SQL combinada (46,9M linhas)
- [x] Estatísticas descritivas (mean, std, percentis)
- [x] Análise de valores nulos
- [x] Análise de duplicatas
- [x] Análise temporal (padrões por hora/dia da semana)
- [x] Análise geográfica (top zonas de pickup)
- [x] Análise de tarifa e gorjeta (por distância)
- [x] Análise de pagamento
- [x] Investigação adicional: outliers e diferença 2015 vs 2016
- [x] Análise extra: viagens por RatecodeID (aeroportos)
- [x] Documentar insights

**Output:** Notebook `03_eda.py` com 14 células de análise + insights documentados

**Arquivo:** `notebooks/03_eda.py`

**Insights Reais Descobertos:**

> ✏️ **Nota de correção:** os valores abaixo foram originalmente registrados como "km" durante a análise, mas o dicionário oficial da TLC confirmou que `trip_distance` é medido em **milhas**. Os números abaixo já foram corrigidos para refletir a unidade correta (milhas).

**Qualidade dos dados:**
```
✅ Nulos: praticamente inexistentes em todas as colunas
✅ Duplicatas: apenas 4 em 46,9M linhas (irrelevante)
✅ Outliers extremos: menos de 300 registros no total (0,0006%)
   - tip_amount negativo: 3 registros
   - total_amount negativo: 5 registros
   - fare_amount > $500: 105 registros
   - trip_distance > 200 milhas (~322km): 184 registros
⚠️ RatecodeID tem 2 códigos não documentados no dicionário oficial
   (6 e 99), com volume irrelevante: 607 registros (0,0013%)
```

**Percentis reais (após remover outliers extremos):**
```
Métrica          Mediana      p90        p95         p99
trip_distance     1,70 mi     6,40 mi    10,20 mi    18,46 mi
fare_amount       $9,00       $23,00     $33,00      $52,00
```

**Padrões temporais:**
- Pico de viagens: 18h-19h (final de tarde)
- Menor volume: 3h-4h da madrugada
- Sábado tem o maior volume de viagens, mas a menor tarifa média

**Descoberta — Comparação Janeiro/2015 vs Janeiro/2016:**
- Médias de distância muito diferentes (13,55 mi vs 4,68 mi) **pareciam** indicar erro de unidade
- Investigação com medianas revelou: **1,70 milhas em ambos os anos** — dados consistentes, a diferença nas médias era causada só pelos outliers extremos concentrados no arquivo de 2015

**Correlação distância-tarifa:**
```
❌ Com outliers:  0,01  (parecia não haver relação)
✅ Sem outliers:  0,95  (relação forte, como esperado)
```
Isso confirmou que os outliers extremos, embora raríssimos, distorciam completamente as métricas agregadas — reforçando a importância de tratá-los na Fase 3.

**Gorjetas por tipo de pagamento:**
- Cartão de crédito: 21,3% de gorjeta média
- Dinheiro: 0% de gorjeta média (**não é erro** — gorjetas em dinheiro não passam pelo sistema da NYC TLC, então nunca são registradas)

**Viagens de aeroporto (via `RatecodeID`):**

| RatecodeID | Tipo | Viagens | Tarifa Média | Distância Média |
|---|---|---|---|---|
| 1 | Standard | 45.899.612 | $11,37 | 7,20 mi |
| 2 | JFK | 891.844 | **$52,54** | 20,72 mi |
| 3 | Newark | 70.036 | $66,04 | 17,04 mi |
| 5 | Negotiated fare | 65.139 | $74,52 | 12,12 mi |
| 4 | Nassau/Westchester | 18.094 | $64,17 | 106,36 mi ⚠️ |
| 99 | Não documentado | 445 | $18,62 | 10,09 mi |
| 6 | Não documentado | 162 | $9,21 | 2,62 mi |

- ✅ **Validação de qualidade dos dados:** a tarifa média de $52,54 para JFK bate quase exatamente com a tarifa fixa histórica real (flat rate Manhattan↔JFK era $52) — forte evidência de que os dados são confiáveis
- ⚠️ **Ponto a investigar na Fase 3:** a distância média de 106 milhas para Nassau/Westchester é suspeita (esperado seria ~20-30 milhas); como o grupo tem volume pequeno (18k viagens), provavelmente há outliers extremos concentrados nessa categoria
- 💡 `RatecodeID` já identifica viagens de aeroporto diretamente — dispensa o cálculo de distância até coordenadas do aeroporto que havíamos planejado originalmente

**Regras de limpeza definidas para a Fase 3:**
```python
REGRAS_LIMPEZA = {
    "trip_distance_min": 0.1,
    "trip_distance_max": 200,      # baseado no p99.9 observado
    "fare_amount_min": 2.5,        # tarifa mínima NYC
    "fare_amount_max": 500,        # baseado no p99.9 observado
    "tip_amount_min": 0,           # remove os 3 registros negativos
    "total_amount_min": 0,         # remove os 5 registros negativos
}
```

---

### 📍 Fase 3: Feature Engineering (10-14 horas) ✅ Concluído

**Objetivo:** Preparar dados de alta qualidade com features enriquecidas, com base nas regras definidas na Fase 2

**Tarefas:**
- [x] Definir regras de limpeza (baseadas nos percentis reais da Fase 2)
- [x] Diagnóstico individual de cada regra de limpeza (transparência total)
- [x] Conversão de unidade: trip_distance (milhas) → trip_distance_km
- [x] Extrair features temporais (7 novas)
- [x] Extrair feature de aeroporto via RatecodeID (2 novas, sem cálculo geoespacial)
- [x] Extrair features de velocidade e duração, com flag de qualidade (4 novas)
- [x] Extrair features de tarifa e gorjeta, respeitando payment_type (3 novas)
- [x] Salvar dados transformados em Delta Lake (particionado por ano/mês, com mergeSchema)
- [x] Combinar os 4 arquivos em tabela única
- [x] Validar leitura dos dados salvos
- [ ] Criar módulo src/transformations.py (adiado — código está no notebook por ora)
- [x] Documentar dicionário de features (`docs/DATA_DICTIONARY.md`)

**Output:** 17 features novas (37 no total), 46.882.150 registros, tabela única combinada

**Arquivo:** `notebooks/04_transform.py`

**Resultados Reais da Limpeza:**
```
Linhas antes:   46.945.332
Linhas depois:  46.882.150
Removidas:      63.182 (0,1346%)
```

**Diagnóstico — contribuição de cada regra (não são mutuamente exclusivas):**
```
trip_distance entre 0 e 0.1mi:     53.888   ← maior contribuinte, de longe
passenger_count inválido (0 ou >8): 6.628
fare_amount < $2.5:                 2.600
trip_distance > 200mi:                184
fare_amount > $500:                   105
total_amount < 0:                       5
tip_amount < 0:                         3
```
> 💡 A soma dessas linhas passa do total removido porque um mesmo registro pode violar mais de uma regra ao mesmo tempo (contado 2x no diagnóstico, mas removido 1x na limpeza real).

**Features Criadas (17 novas):**
```
Conversão:      trip_distance_km (milhas → km)
Temporais:      pickup_hour, pickup_day_of_week, pickup_month, pickup_year,
                is_weekend, is_rush_hour, time_of_day
Aeroporto:      is_airport_trip, airport_type (via RatecodeID — sem cálculo geoespacial)
Velocidade:     trip_duration_minutes, duracao_valida, speed_kmh, speed_category
Tarifa:         fare_per_km, tip_percentage, tip_category
```

> 📌 `tip_percentage`/`tip_category` só são calculados para `payment_type = 1` (cartão); dinheiro recebe `tip_category = "nao_aplicavel"`, já que gorjetas em dinheiro não são registradas pelo sistema (ver seção Dataset).

**Achado — Qualidade da Duração de Viagem:**
```
171.592 viagens (0,37%) sinalizadas como speed_category = "duracao_invalida"
(duração fora da faixa de 1 a 300 minutos)
```
Essas linhas **não foram removidas** — só a velocidade calculada é marcada como não confiável (`speed_kmh = NULL`), preservando tarifa, distância e demais dados válidos da viagem.

**Achado — Distribuição de Gorjetas:**
```
alta (≥20%):        22.466.554  (48% de todas as viagens)
media (10-20%):      5.777.578
baixa (<10%):        1.429.213
sem_gorjeta:         1.078.562
nao_aplicavel (dinheiro): 16.130.243
```
A concentração em "alta" reflete os botões de gorjeta pré-definidos (20%/25%/30%) nas máquinas de cartão dos táxis de NY.

**Dados salvos em:** `/Volumes/workspace/default/nyc_taxi/processed/featured/taxi_featured` (Delta Lake, particionado por `pickup_year`, `pickup_month`)

---

### 📍 Fase 4: Análise & Visualização (8-10 horas) ✅ Concluído

**Objetivo:** Gerar insights executivos usando os dados já enriquecidos na Fase 3

**Tarefas:**
- [x] Análises por período (2015 vs 2016, e Jan→Mar/2016)
- [x] Padrões horários e diários (com filtro correto de payment_type)
- [x] Análise de top zonas de pickup
- [x] Análise de gorjeta por segmento (dia, hora, aeroporto)
- [x] Análise por tipo de pagamento
- [x] Análise detalhada de viagens de aeroporto (via RatecodeID)
- [x] Comparação Standard vs JFK vs Newark
- [x] Criar agregações para dashboard (5 tabelas)
- [x] Visualizações principais (padrão horário, comparação de tipos)
- [x] Documentar insights principais

**Output:** 5 agregações em Delta Lake, insights documentados, validação cruzada com a Fase 3

**Arquivo:** `notebooks/05_analise.py`

**Agregações Salvas (Delta Lake):**
```
hourly_trends       → 24 linhas  (padrões por hora do dia)
daily_trends        → 7 linhas   (padrões por dia da semana)
top_zonas           → 20 linhas  (top zonas de pickup)
payment_analysis    → 5 linhas   (análise por tipo de pagamento)
gorjeta_segmento    → 32 linhas  (gorjeta por dia/hora/aeroporto)
```

**Validação Cruzada com a Fase 3:**
```
Soma de tip_category (Fase 3) = total de payment_type=1 (Fase 4):
30.751.907 viagens em ambos os cálculos — consistência confirmada
```

**Confirmação do Achado da Fase 2:**

Após a limpeza da Fase 3, a diferença de distância média entre os anos ficou muito menor do que a observada originalmente na Fase 2 (13,55 vs 4,68 milhas, causada por outliers):
```
2015-01: distância média 4,51 km
2016-01: distância média 4,70 km
```
Isso confirma que os outliers extremos, já removidos, eram de fato a causa da discrepância.

**Gorjeta por Dia da Semana (com filtro correto de `payment_type = 1`):**
```
Maior gorjeta média: Domingo (25,4%)
Menor gorjeta média: Sábado (20,8%)
```
> 📌 O achado da Fase 2 sobre "sexta ter maior gorjeta" foi calculado sem filtrar `payment_type`, então o resultado atual (Fase 4) é o mais confiável.

**Aeroportos — Estabilidade da Tarifa Fixa:**
```
JFK: tarifa entre $51,70 e $51,99 ao longo de todas as 24 horas do dia
```
Variação mínima ao longo do dia, consistente com o conceito de tarifa fixa (flat rate) — mais uma validação de qualidade dos dados.

**Comparação Standard vs Aeroporto:**

| Tipo | Viagens | Tarifa Média | Distância Média | Duração Média |
|---|---|---|---|---|
| Standard | 45.926.683 | $11,48 | 4,15 km | 12,5 min |
| JFK | 885.874 | $51,98 | 28,84 km | 43,3 min |
| Newark | 69.593 | $66,32 | 27,59 km | 36,1 min |

**Padrões Gerais Confirmados:**
- Pico de viagens: 18h-19h (~2,4M viagens/hora)
- Menor volume: madrugada 3h-4h (~500-680k viagens/hora)
- Sábado tem o maior volume de viagens totais, mas a menor gorjeta média

---

### 📍 Fase 5: Machine Learning (14-18 horas)

**Objetivo:** Treinar e versionar modelos preditivos com MLflow

**Tarefas:**
- [ ] Definir problemas de negócio (regressão de tarifa)
- [ ] Preparar dataset (train/val/test temporal)
- [ ] Feature selection (correlação + importância)
- [ ] Setup MLflow experiment
- [ ] Treinar Linear Regression (baseline)
- [ ] Treinar Random Forest
- [ ] Treinar Gradient Boosting
- [ ] Comparar performance (RMSE, MAE, R²)
- [ ] Validação cruzada temporal (3 folds)
- [ ] Análise de erros e residuais
- [ ] Feature importance
- [ ] Registrar melhor modelo em MLflow
- [ ] Fazer previsões em batch

**Output:** 3 modelos treinados, melhor em produção, 100k+ previsões

**Arquivo:** `notebooks/06_ml.py`

**Resultados Esperados:**
```
Modelo                  RMSE    MAE     R²
─────────────────────────────────────────
Linear Regression       $2.85   $1.92   0.87
Random Forest           $2.15   $1.45   0.92
Gradient Boosting ✓     $2.05   $1.38   0.93
```

---

### 📍 Fase 6: Validação & Avaliação (2-3 horas)

**Objetivo:** Validar robustez do modelo

**Tarefas:**
- [ ] Cross-validation temporal (time series split)
- [ ] Análise detalhada de residuais
- [ ] Erros por segmento (hora, dia, zona)
- [ ] Verificação de normalidade de erros

**Output:** Validação temporal, análise de erros

**Arquivo:** `notebooks/06_ml.py` (adicionar)

---

### 📍 Fase 7: Deployment & Automação (4-6 horas)

**Objetivo:** Preparar pipeline para produção

**Tarefas:**
- [ ] Criar notebook de deployment
- [ ] Criar Job agendado (diário)
- [ ] Implementar monitoramento
- [ ] Sistema de alertas (performance degradation)
- [ ] README final
- [ ] Commit final no GitHub

**Output:** Pipeline automatizado, job em produção

**Arquivo:** `notebooks/07_deploy.py`

---

## 💾 Requisitos

> ✅ **Este projeto roda 100% no Databricks.** Não é necessário instalar Python, PySpark, Git, IDE ou configurar ambiente virtual localmente. Tudo — código, testes, execução e versionamento — acontece dentro da plataforma.

### Conta Databricks
- ✅ **Free Edition** (substituiu o antigo "Community Edition", aposentado em jan/2026)
- ✅ Sem custos adicionais, sem necessidade de cartão de crédito
- ✅ Compute Serverless já incluso (sem precisar configurar cluster)
- ⚠️ Limitações:
  - Cotas de uso (compute pausa ao atingir limite diário/mensal, e reseta depois — não gera cobrança)
  - Armazenamento via Unity Catalog Volumes (não DBFS tradicional)
  - Acesso à internet restrito a domínios confiáveis
  - Sem suporte prioritário

**Como criar:** https://www.databricks.com/signup/free-edition

### GitHub
- ✅ Conta pública (gratuita)
- ✅ Repositório público (seu portfólio)
- ✅ Usado apenas via **Databricks Repos** (sem Git local necessário)

**Como criar:** https://github.com/signup

### No seu computador (mínimo necessário)
- ✅ Navegador web (Chrome, Firefox, Edge, etc)
- ✅ ~8GB de espaço em disco (apenas para baixar os CSVs do Kaggle temporariamente, antes do upload no Databricks)
- ✅ Conexão internet estável

**Não precisa:**
- ❌ Python instalado
- ❌ PySpark instalado
- ❌ Git instalado
- ❌ IDE (VSCode, PyCharm, etc)
- ❌ Ambiente virtual (venv, conda)
- ❌ Jupyter Notebook local

### Conhecimentos Prévios
- ⚠️ **Python básico** (loops, funções, classes)
- ⚠️ **SQL básico** (SELECT, WHERE, GROUP BY)
- ⚠️ **Noções de Git** (conceitos de commit/push — usados via interface do Databricks)
- ⚠️ **Conceitos de ML** (train/test, RMSE, R²)

---

## 🚀 Setup Inicial

> Todos os passos abaixo são feitos via **navegador**, na interface do GitHub e do Databricks — sem terminal local, sem instalação.

### Passo 1: Criar Repositório no GitHub (via navegador)

```
1.1 Acesse https://github.com/new

1.2 Preencha:
    Nome: nyc-taxi-databricks-analytics
    Descrição: (ver seção de descrição do projeto)
    Visibilidade: Público (para portfólio)
    Inicializar com: README, .gitignore (Python), LICENSE (MIT)

1.3 Clique em "Create repository"

1.4 Faça upload deste README.md
    → Add file → Upload files → arraste o README.md → Commit
```

### Passo 2: Conectar GitHub ao Databricks (via navegador)

```
2.1 Acesse o Databricks Free Edition
    → https://www.databricks.com/signup/free-edition
    (crie a conta se ainda não tiver — não pede cartão de crédito)

2.2 Faça login no seu workspace
    (confirme no canto superior esquerdo: deve aparecer "Free Edition")

2.3 Workspace → Repos → Add Repo
    Repository URL: https://github.com/seu-usuario/nyc-taxi-databricks-analytics
    Git provider: GitHub
    Repository name: nyc-taxi-databricks-analytics

2.4 Compute: o Free Edition já vem com Serverless Compute
    pronto por padrão — não é necessário criar/configurar cluster

2.5 Abra Workspace → Repos → seu-repo
    A estrutura do GitHub aparece sincronizada automaticamente
```

### Passo 3: Estrutura de Pastas — Código (já vem do GitHub) e Dados (criada via código)

```
3.1 A estrutura de CÓDIGO (notebooks/, src/, docs/, config/) 
    já foi criada no Passo 1 (GitHub) e chega pronta no Databricks
    assim que você conecta o Repo — não precisa recriá-la.

3.2 A estrutura de DADOS usa um Unity Catalog Volume, não o
    DBFS tradicional (que vem desativado por padrão no Free
    Edition). Ela é criada pelo próprio notebook 01_setup.py,
    rodando:
    - spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.nyc_taxi")
    - dbutils.fs.mkdirs("/Volumes/workspace/default/nyc_taxi/raw/csv")
    direto no Databricks.

3.3 Resumindo: você não cria pastas de dados manualmente — 
    o código (que está no GitHub) faz isso automaticamente 
    na primeira execução.

3.4 Para visualizar as pastas criadas: menu lateral → Catalog 
    → workspace → default → Volumes → nyc_taxi
```

### Passo 4: Download dos Dados do Kaggle (único passo fora do Databricks)

```
4.1 Acesse no navegador:
    https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data

4.2 Baixe os 4 arquivos CSV (~7.5GB total):
    - yellow_tripdata_2015-01.csv
    - yellow_tripdata_2016-01.csv
    - yellow_tripdata_2016-02.csv
    - yellow_tripdata_2016-03.csv
    (ficam temporariamente no seu computador)

4.3 Depois, faça upload direto no Databricks:
    Workspace → Data → Add data → Upload File
    (ou via notebook, com dbutils.fs)

4.4 Após o upload e conversão para Delta Lake (Fase 1),
    os CSVs podem ser deletados do DBFS e do seu computador
```

### Passo 5: Validar Ambiente

Dentro de um notebook no Databricks, rode um teste rápido:

```python
# Teste 1: Spark
spark.range(10).collect()  # Deve funcionar

# Teste 2: MLflow
import mlflow
mlflow.set_experiment("/test")
print("✅ Ambos funcionando!")
```

---

## 🏃 Como Rodar

### Sequência Recomendada

Execute os notebooks **nesta ordem**:

```
1️⃣  01_setup.py              (2-3h)
    └─ Ambiente pronto

2️⃣  02_ingestao.py           (2-4h)
    └─ Dados em Delta Lake

3️⃣  03_eda.py                (6-8h)
    └─ Insights explorados

4️⃣  04_transform.py          (10-14h)
    └─ Features criadas

5️⃣  05_analise.py            (8-10h)
    └─ Visualizações prontas

6️⃣  06_ml.py                 (14-18h)
    └─ Modelos treinados

7️⃣  07_deploy.py             (4-6h)
    └─ Pipeline em produção
```

### Executar um Notebook

```
No Databricks UI:

1. Workspace → Repos → seu-repo
2. Abra: notebooks/01_setup.py
3. Clique em "Run all" (play ▶️)
4. Aguarde conclusão
5. Verifique outputs
6. Passe para próximo notebook
```

### Monitorar Progresso

```
Dashboard de Progresso:

Fase  Status      Saída
───────────────────────────────────────
0     ✅ Completo Ambiente pronto
1     ⏳ Rodando... (10/20 min)
2     ⬜ Não iniciado
3     ⬜ Não iniciado
...
```

---

## 📈 Resultados Esperados

### Fase 1: Ingestão ✅ Concluído
```
✅ 4 tabelas Delta criadas
✅ 46,9M registros preservados
✅ Tamanho original: 7,39GB (CSV) → 1,06GB (Delta)
✅ Compressão: ~86%
✅ Espaço liberado: 7,39GB (CSVs deletados após conversão)
```

### Fase 2: EDA
```
✅ 15+ insights descobertos
✅ 10+ visualizações criadas
✅ Padrões temporais identificados
✅ Qualidade de dados validada
```

### Fase 3: Transform ✅ Concluído
```
✅ 17 features novas criadas (37 no total)
✅ 46.882.150 registros (63.182 removidos, 0,13%)
✅ Duração de viagem inválida sinalizada, não descartada (171.592 registros)
✅ Dados prontos para ML
```

### Fase 4: Análise ✅ Concluído
```
✅ 5 agregações salvas em Delta Lake
✅ Validação cruzada com a Fase 3 (payment_type=1: 30,75M em ambos os cálculos)
✅ Confirmação do achado de outliers da Fase 2 (distâncias 2015 vs 2016 convergiram)
✅ Insights de aeroporto documentados (tarifa fixa estável ao longo do dia)
```

### Fase 5: ML
```
✅ 3 modelos treinados
✅ Melhor modelo: Gradient Boosting
✅ Métricas: RMSE $2.05, R² 0.93
✅ 100k+ previsões em batch
```

### Fase 6: Validação
```
✅ Cross-validation temporal
✅ Análise de residuais
✅ Erros por segmento
✅ Modelo robusto confirmado
```

### Fase 7: Deploy
```
✅ Job diário agendado
✅ Monitoramento ativo
✅ Sistema de alertas
✅ Pipeline em produção
```

---

## 📚 Documentação Adicional

| Documento | Descrição |
|-----------|-----------|
| [`docs/SETUP_GUIDE.md`](docs/SETUP_GUIDE.md) | Guia detalhado de setup |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Diagrama de arquitetura |
| [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) | Dicionário de features |
| [`docs/PHASES_OVERVIEW.md`](docs/PHASES_OVERVIEW.md) | Visão geral das 7 fases |
| [`docs/GITHUB_WORKFLOW.md`](docs/GITHUB_WORKFLOW.md) | Como usar Git neste projeto |
| [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) | Problemas comuns e soluções |

---

## 🔄 Workflow de Desenvolvimento

> Todo o workflow abaixo acontece **dentro da interface do Databricks** — o botão "Git" no notebook substitui os comandos de terminal.

### Dia a Dia

```
1. Sincronize com o GitHub
   Databricks Repos → botão "Pull" (canto superior)

2. Abra o notebook
   Workspace → Repos → seu-repo → notebook.py

3. Faça alterações
   (Databricks auto-save a cada execução de célula)

4. Teste e valide outputs
   (Rode as células direto no cluster)

5. Commit e push para o GitHub
   Databricks Repos → botão "Git" → escreva a mensagem 
   → Commit & Push
   (sincroniza direto com o GitHub, sem terminal)
```

### Branches

```
main                    (produção)
├─ Fase 0 completa
├─ Fase 1 completa
├─ ...
└─ Feature branches (opcional)
  ├─ feature/new-model
  ├─ fix/data-quality
  └─ docs/add-guide
```

### Commits

**Convenção de mensagens:**

```
feat: implementar feature engineering
fix: corrigir bug em limpeza de dados
docs: adicionar guia de setup
refactor: otimizar queries SQL
test: adicionar testes de transformação
```

---

## 🤝 Contribuições

### Para Seu Próprio Projeto
- Fork este repositório (via GitHub, no navegador)
- No Databricks Repos, crie uma branch (botão de branch no topo do Repo)
- Faça alterações nos notebooks e commit via botão "Git"
- Push da branch para o GitHub (mesma interface)
- Abra um Pull Request no GitHub quando quiser mesclar

### Para Melhorar Este Template
Sugestões e issues são bem-vindas! (em versões públicas)

---

## ⚠️ Troubleshooting Rápido

### Problema: "Storage limit exceeded"
```
Solução:
1. Verifique espaço usado: dbutils.fs.ls("/Volumes/workspace/default/nyc_taxi")
2. Delete tabelas intermediárias não usadas
3. Comprima com VACUUM se usar Delta
```

### Problema: "Cluster keeps disconnecting"
```
Solução:
1. Aumente auto-terminate time
2. Use cluster maior (se budget permitir)
3. Processe em chunks menores
```

### Problema: "Model not found in MLflow"
```
Solução:
1. Verifique experiment name
2. Confirme que modelo foi logado
3. Check Model Registry para versão
```

### Problema: "Git merge conflicts" (na sincronização do Databricks Repos)
```
Solução:
1. No Databricks Repos, clique em "Pull" para trazer mudanças
2. Se houver conflito, o Databricks sinaliza o arquivo afetado
3. Abra o arquivo e resolva manualmente as diferenças
4. Clique em "Git" → escreva a mensagem de commit
5. Commit & Push para sincronizar com o GitHub
```

Mais soluções em: [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)

---

## 📊 Métricas de Sucesso

| Métrica | Meta | Status |
|---------|------|--------|
| Código versionado | 100% no GitHub | ⬜ |
| Features criadas | 40+ | ⬜ |
| Modelo RMSE | < $2.50 | ⬜ |
| Dados processados | 46,9M registros | ✅ |
| Compressão CSV → Delta | 86% (7,39GB→1,06GB) | ✅ |
| Dashboard criado | 1 interativo | ⬜ |
| Jobs agendados | 1 diário | ⬜ |

---

## 🎓 Aprendizados Esperados

Após completar este projeto, você será capaz de:

✅ **Databricks**
- Configurar workspace e clusters
- Usar Databricks Repos com GitHub
- Entender arquitetura free tier

✅ **PySpark**
- Transformações distribuídas
- Otimizações de performance
- Particionamento estratégico

✅ **Delta Lake**
- Criação e versionamento de tabelas
- Time travel e schema enforcement
- ACID transactions

✅ **SQL**
- Queries complexas em Big Data
- Window functions e CTEs
- Aggregations e joins

✅ **ML**
- Pipeline completo de features
- Treinamento de múltiplos modelos
- Validação temporal de séries

✅ **MLflow**
- Rastreamento de experimentos
- Versionamento de modelos
- Model Registry

✅ **Git & GitHub**
- Workflow profissional
- Commits significativos
- Portfólio público

---

## 📞 Suporte

### Recursos Úteis

| Recurso | Link |
|---------|------|
| Documentação Databricks | https://docs.databricks.com |
| PySpark API | https://spark.apache.org/docs/latest/api/python/ |
| Delta Lake | https://docs.delta.io |
| MLflow | https://mlflow.org/docs |
| Kaggle Dataset | https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data |

### Comunidades

- [Databricks Community (fórum)](https://community.databricks.com)
- [Stack Overflow - databricks](https://stackoverflow.com/questions/tagged/databricks)
- [Reddit r/databricks](https://reddit.com/r/databricks)

---

## 📝 Licença

Este projeto é licenciado sob a **MIT License** - veja [`LICENSE`](LICENSE) para detalhes.

MIT License permite:
✅ Uso comercial
✅ Modificação
✅ Distribuição
✅ Uso privado

Com obrigação de:
⚠️ Incluir licença
⚠️ Indicar mudanças

---

## 👤 Autor

**Seu Nome** (ajustar após criar)

- GitHub: [@seu-usuario](https://github.com/seu-usuario)
- Email: seu-email@example.com
- LinkedIn: [linkedin.com/in/seu-usuario](https://linkedin.com/in/seu-usuario)

---

## 🙏 Agradecimentos

- Databricks pela plataforma
- Kaggle pelo dataset
- Comunidade open-source Python/Spark
- Referências e documentação

---

## 📋 Changelog

### v1.0.0 - 2024
- ✅ Projeto inicial estruturado
- ✅ 7 fases completas definidas
- ✅ Documentação abrangente
- ✅ README finalizado

---

## 🚀 Próximos Passos

1. **Agora:** Faça upload deste README no Databricks
2. **Depois:** Comece pela Fase 0 (Setup)
3. **Progresso:** Siga sequência definida
4. **Fim:** Celebrate! 🎉

---

**Última atualização:** 2024-08
**Versão:** 1.0.0
**Status:** Ready to use ✅

---

## 📞 Quick Links

- 🌐 [GitHub](https://github.com/seu-usuario/nyc-taxi-databricks-analytics)
- 🔗 [Databricks Free Edition](https://www.databricks.com/signup/free-edition)
- 📊 [Dataset](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- 📚 [Docs](./docs)

---

**Aproveite o projeto! 🚕✨**

Dúvidas? Abra uma issue no GitHub ou consulte a documentação.