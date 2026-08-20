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
- 40+ features enriquecidas
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

### Características

| Aspecto | Detalhes |
|--------|----------|
| **Fonte** | [Kaggle - NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data) |
| **Período** | Jan/2015 + Jan-Mar/2016 (4 arquivos, não consecutivos) |
| **Volume** | ~46,9 milhões de registros (confirmado após ingestão) |
| **Tamanho Original** | 7,39GB (4 arquivos CSV — confirmado) |
| **Tamanho Processado** | 1,06GB (Delta Lake comprimido — confirmado) |
| **Compressão** | ~86% de redução (confirmado) |

### Arquivos do Dataset

O dataset no Kaggle disponibiliza **4 arquivos CSV**, cobrindo um mês de 2015 e três meses de 2016 (não são 4 meses consecutivos):

```
yellow_tripdata_2015-01.csv   (Janeiro/2015)
yellow_tripdata_2016-01.csv   (Janeiro/2016)
yellow_tripdata_2016-02.csv   (Fevereiro/2016)
yellow_tripdata_2016-03.csv   (Março/2016)
```

> ⚠️ **Atenção:** por misturar um mês de 2015 com três meses de 2016, análises temporais que comparam "mês a mês" (ex: tendência jan→fev→mar) devem considerar apenas os 3 arquivos de 2016 como sequência contínua. O arquivo de 2015 entra como um período isolado, útil para comparações ano a ano (2015 vs 2016), mas não para tendência sequencial.

### Colunas Principais

```
Coluna                    | Tipo      | Descrição
--------------------------|-----------|----------------------------------
VendorID                  | int       | ID do vendedor
tpep_pickup_datetime      | timestamp | Data/hora de início
tpep_dropoff_datetime     | timestamp | Data/hora de término
passenger_count           | int       | Número de passageiros
trip_distance             | float     | Distância em km
pickup_longitude          | float     | Longitude de pickup
pickup_latitude           | float     | Latitude de pickup
dropoff_longitude         | float     | Longitude de dropoff
dropoff_latitude          | float     | Latitude de dropoff
fare_amount               | float     | Tarifa em $
tip_amount                | float     | Gorjeta em $
total_amount              | float     | Total em $
payment_type              | int       | Tipo de pagamento
```

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
| 2 | **Análise Exploratória (EDA)** | 6-8h | 10 | 🔵 Em andamento |
| 3 | **Feature Engineering** | 10-14h | 12 | ⬜ Não iniciado |
| 4 | **Análise & Visualização** | 8-10h | 10 | ⬜ Não iniciado |
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

### 📍 Fase 2: Análise Exploratória (EDA) (6-8 horas) 🔵 Em andamento

**Objetivo:** Entender dados através de análise exploratória detalhada

**Tarefas:**
- [ ] Carregar dados Delta em tabelas SQL
- [ ] Estatísticas descritivas (mean, std, percentis)
- [ ] Análise de valores nulos
- [ ] Análise de duplicatas
- [ ] Análise temporal (padrões por hora/dia/mês)
- [ ] Análise geográfica (zonas, rotas)
- [ ] Análise de tarifa e gorjeta
- [ ] Análise de pagamento
- [ ] Visualizações principais (10+)
- [ ] Documentar insights

**Output:** Relatório EDA, 15+ insights, 10+ visualizações

**Arquivo:** `notebooks/03_eda.py`

**Exemplos de Insights:**
- "Pickups aumentam 300% durante rush hours (7-9am, 5-7pm)"
- "Gorjeta média é 18% mas varia: 22% sexta, 12% segunda"
- "80% das viagens saem de Manhattan"
- "Correlação distância-tarifa: 0.85 (forte positiva)"

---

### 📍 Fase 3: Feature Engineering (10-14 horas)

**Objetivo:** Preparar dados de alta qualidade com 40+ features enriquecidas

**Tarefas:**
- [ ] Definir regras de limpeza (outliers, nulos)
- [ ] Extrair features temporais (8 novas)
- [ ] Extrair features de localização (6 novas)
- [ ] Extrair features de velocidade (3 novas)
- [ ] Extrair features de tarifa (4 novas)
- [ ] Imputação de valores nulos
- [ ] Remoção de outliers (IQR)
- [ ] Salvar dados transformados em Delta
- [ ] Combinar os 4 arquivos em tabela única
- [ ] Criar Delta Lake (opcional, versioning)
- [ ] Criar módulo src/transformations.py
- [ ] Documentar dicionário de features

**Output:** 40+ features, 630M registros limpos, tabela única com os 4 arquivos

**Arquivo:** `notebooks/04_transform.py`

**Features Criadas:**
```
Temporais:      pickup_hour, day_of_week, is_weekend, is_rush_hour
Localização:    lat_bucket, lng_bucket, haversine_distance
Tarifa:         fare_per_km, tip_percentage, tip_category
Velocidade:     trip_duration_minutes, speed_kmh, speed_category
```

---

### 📍 Fase 4: Análise & Visualização (8-10 horas)

**Objetivo:** Gerar insights executivos e dashboard interativo

**Tarefas:**
- [ ] Análises por período (2015 vs 2016, e Jan→Mar/2016)
- [ ] Padrões horários e diários
- [ ] Análise de top rotas
- [ ] Análise de gorjeta por segmento
- [ ] Análise por tipo de pagamento
- [ ] Criar agregações para dashboard (6 tabelas)
- [ ] Visualizações principais (12+)
- [ ] Dashboard executivo (5 abas)
- [ ] Documentar insights principais
- [ ] Salvar agregações em Delta

**Output:** Dashboard interativo, 6 agregações, 12+ visualizações

**Arquivo:** `notebooks/05_analise.py`

**Aggregações Salvas:**
```
hourly_trends.parquet    → 24 linhas
daily_trends.parquet     → 7 linhas
monthly_trends.parquet   → 4 linhas
top_routes.parquet       → 100 linhas
payment_analysis.parquet → 5 linhas
zone_analysis.parquet    → 50 linhas
```

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

### Fase 3: Transform
```
✅ 40+ features criadas
✅ 630M registros limpos (7% outliers removido)
✅ Valores nulos tratados (< 1% restante)
✅ Dados prontos para ML
```

### Fase 4: Análise
```
✅ Dashboard com 5 abas
✅ 6 agregações para visualização
✅ 12+ gráficos criados
✅ KPIs executivos definidos
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