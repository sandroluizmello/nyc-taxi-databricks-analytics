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

✅ Ingestão de dados em larga escala (7.5GB → 1.5GB)
✅ Processamento distribuído com PySpark
✅ Armazenamento otimizado com Delta Lake
✅ Engenharia de features de qualidade
✅ Análise exploratória e visualizações
✅ Machine Learning com versionamento (MLflow)
✅ Validação temporal de modelos
✅ Automação e deployment em produção

**Público-alvo:** Data Engineers, Data Scientists, Iniciantes em Big Data

**Duração esperada:** 6-8 semanas (1-2h/dia)

---

## 🎓 Objetivos

### Objetivo Principal
Criar um sistema de **previsão de tarifas de taxi** (Fare Amount) em NYC com modelo de ML treinado em 680 milhões de registros reais.

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
| **Período** | Janeiro - Abril 2016 |
| **Volume** | ~680 milhões de registros |
| **Tamanho Original** | 7.5GB (4 arquivos CSV) |
| **Tamanho Processado** | ~1.5GB (Delta Lake comprimido) |
| **Compressão** | 80% de redução |

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

### No GitHub

```
nyc-taxi-databricks-analytics/
├── .gitignore                          # Arquivos ignorados no Git
├── README.md                           # Este arquivo
├── LICENSE                             # Licença MIT
├── requirements.txt                    # Dependências Python
│
├── docs/                               # Documentação do projeto
│   ├── ARCHITECTURE.md                 # Diagrama e explicação
│   ├── SETUP_GUIDE.md                  # Guia de setup
│   ├── DATA_DICTIONARY.md              # Dicionário de features
│   ├── PHASES_OVERVIEW.md              # Visão geral das 7 fases
│   ├── PHASE_1_INGESTAO.md             # Fase 1: Detalhes
│   ├── PHASE_2_EDA.md                  # Fase 2: Detalhes
│   ├── PHASE_3_TRANSFORM.md            # Fase 3: Detalhes
│   ├── PHASE_4_INSIGHTS.md             # Fase 4: Detalhes
│   ├── PHASE_5_ML_MODELS.md            # Fase 5: Detalhes
│   ├── GITHUB_WORKFLOW.md              # Como usar Git
│   └── TROUBLESHOOTING.md              # Problemas comuns
│
├── notebooks/                          # Notebooks Databricks
│   ├── 01_setup.py                     # Validação ambiente
│   ├── 02_ingestao.py                  # CSV → Delta Lake
│   ├── 03_eda.py                       # Exploração de dados
│   ├── 04_transform.py                 # Feature engineering
│   ├── 05_analise.py                   # Análises e vizs
│   ├── 06_ml.py                        # Models + MLflow
│   └── 07_deploy.py                    # Automação
│
├── src/                                # Código modular
│   ├── __init__.py
│   ├── config.py                       # Configurações
│   ├── data_ingestion.py               # Funções I/O
│   ├── transformations.py              # Feature eng.
│   ├── ml_utils.py                     # Utilitários ML
│   └── monitoring.py                   # Monitoramento
│
├── config/                             # Arquivos config
│   ├── paths.yaml                      # Paths DBFS
│   ├── features.yaml                   # Lista de features
│   └── ml_config.yaml                  # Params ML
│
├── tests/                              # Testes (opcional)
│   ├── test_transformations.py
│   └── test_ml.py
│
└── data/                               # Data local (gitignored)
    └── sample_data.csv                 # Amostra para testes
```

### No Databricks (DBFS)

```
/mnt/data/
├── raw/
│   ├── csv/                            # CSVs temporários
│   │   ├── yellow_tripdata_2016-01.csv (deletado após fase 1)
│   │   ├── yellow_tripdata_2016-02.csv (deletado após fase 1)
│   │   ├── yellow_tripdata_2016-03.csv (deletado após fase 1)
│   │   └── yellow_tripdata_2016-04.csv (deletado após fase 1)
│   └── delta/                          # Delta Lake raw
│       ├── taxi_2016-01/
│       ├── taxi_2016-02/
│       ├── taxi_2016-03/
│       └── taxi_2016-04/
│
├── processed/
│   ├── cleaned/                        # Dados limpos
│   ├── featured/                       # Com features
│   │   └── taxi_featured/
│   └── combined/                       # 4 meses combinados
│       └── taxi_combined_4months/
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

---

## 📊 Fases do Projeto

### ⏱️ Timeline Total: 6-8 semanas

| # | Fase | Duração | Tarefas | Status |
|---|------|---------|--------|--------|
| 0 | **Setup & Preparação** | 2-3h | 7 | ⬜ Não iniciado |
| 1 | **Ingestão & Delta Lake** | 2-4h | 9 | ⬜ Não iniciado |
| 2 | **Análise Exploratória (EDA)** | 6-8h | 10 | ⬜ Não iniciado |
| 3 | **Feature Engineering** | 10-14h | 12 | ⬜ Não iniciado |
| 4 | **Análise & Visualização** | 8-10h | 10 | ⬜ Não iniciado |
| 5 | **Machine Learning (MLflow)** | 14-18h | 12 | ⬜ Não iniciado |
| 6 | **Validação & Avaliação** | 2-3h | 3 | ⬜ Não iniciado |
| 7 | **Deployment & Automação** | 4-6h | 5 | ⬜ Não iniciado |

---

### 📍 Fase 0: Setup & Preparação (2-3 horas)

**Objetivo:** Preparar ambiente Databricks, GitHub e estrutura inicial

**Tarefas:**
- [x] Criar repositório GitHub
- [x] Estruturar pastas localmente
- [x] Conectar GitHub ao Databricks Repos
- [x] Criar estrutura DBFS
- [x] Avaliar ambiente Databricks
- [x] Setup MLflow experiment
- [x] Documentação inicial

**Output:** Ambiente pronto, GitHub sincronizado, MLflow funcionando

**Arquivo:** `notebooks/01_setup.py`

---

### 📍 Fase 1: Ingestão & Conversão para Delta Lake (2-4 horas)

**Objetivo:** Upload de 4 CSVs e transformação em Delta Lake comprimido

**Tarefas:**
- [ ] Upload dos 4 CSVs para DBFS (~7.5GB)
- [ ] Exploração rápida do schema
- [ ] Transformação CSV → Delta (1 arquivo por vez)
- [ ] Validação dos dados convertidos
- [ ] Deleção dos CSVs originais (libera 7.5GB)
- [ ] Relatório de ingestão

**Output:** 4 tabelas Delta (~1.5GB), 680M registros preservados

**Arquivo:** `notebooks/02_ingestao.py`

**Métricas Esperadas:**
```
Linhas originais:   680.000.000
Linhas processadas: 680.000.000 (100%)
Tamanho CSV:        7.5GB
Tamanho Delta:      1.5GB
Compressão:         80%
Tempo total:        20-40 min
```

---

### 📍 Fase 2: Análise Exploratória (EDA) (6-8 horas)

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
- [ ] Combinar 4 meses em tabela única
- [ ] Criar Delta Lake (opcional, versioning)
- [ ] Criar módulo src/transformations.py
- [ ] Documentar dicionário de features

**Output:** 40+ features, 630M registros limpos, tabela única 4 meses

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
- [ ] Análises por período (jan vs abr)
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

### Conta Databricks
- ✅ **Free Tier** (comunidade)
- ✅ Sem custos adicionais
- ⚠️ Limitações:
  - ~5-10GB storage
  - Cluster compartilhado
  - SQL Warehouse limitado
  - Sem suporte prioritário

**Como criar:** https://databricks.com/product/pricing

### GitHub
- ✅ Conta pública (gratuita)
- ✅ Repositório público (seu portfólio)

**Como criar:** https://github.com/signup

### Ambiente Local
- ✅ Python 3.8+ instalado
- ✅ Git instalado
- ✅ ~8GB espaço em disco (para download dos dados)
- ✅ Conexão internet estável

### Conhecimentos Prévios
- ⚠️ **Python básico** (loops, funções, classes)
- ⚠️ **SQL básico** (SELECT, WHERE, GROUP BY)
- ⚠️ **Noções de Git** (clone, commit, push)
- ⚠️ **Conceitos de ML** (train/test, RMSE, R²)

---

## 🚀 Setup Inicial

### Passo 1: Preparar Repositório GitHub

```bash
# 1.1 Clone este repo (ou crie um novo)
git clone https://github.com/seu-usuario/nyc-taxi-databricks-analytics.git
cd nyc-taxi-databricks-analytics

# 1.2 Crie estrutura de pastas
mkdir -p notebooks src config docs tests data
touch notebooks/.gitkeep src/__init__.py config/.gitkeep

# 1.3 Copie este README
# (já deve estar aqui)

# 1.4 Commit inicial
git add .
git commit -m "Initial project structure"
git push origin main
```

### Passo 2: Preparar Databricks

```
2.1 Acesse Databricks Community Edition
    → https://community.cloud.databricks.com

2.2 Crie um novo Workspace (ou use existente)

2.3 Workspace → Repos → Create Repo
    Repository URL: https://github.com/seu-usuario/nyc-taxi-databricks-analytics
    Branch: main
    Repo name: nyc-taxi-databricks-analytics

2.4 Crie um Cluster (se não tiver)
    → Workspace → Create → Cluster
    → Databricks Runtime: 12.2 LTS ou superior
    → Worker Type: i3.xlarge (1 worker)
    → Auto-terminate: 30 min

2.5 Abra Workspace → Repos → seu-repo
    Abrirá a estrutura GitHub sincronizada
```

### Passo 3: Download dos Dados

```bash
# 3.1 Baixe do Kaggle (manual)
# https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data

# 3.2 Ou use Kaggle CLI
pip install kaggle
kaggle datasets download -d elemento/nyc-yellow-taxi-trip-data

# 3.3 Extraia
unzip nyc-yellow-taxi-trip-data.zip

# 3.4 Você terá 4 arquivos CSV (~7.5GB total)
ls -lh yellow_tripdata_2016-*.csv
```

### Passo 4: Validar Ambiente

No Databricks, crie um notebook de teste:

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

### Fase 1: Ingestão
```
✅ 4 tabelas Delta criadas
✅ 680M registros preservados (100%)
✅ Compressão: 7.5GB → 1.5GB (80%)
✅ Espaço liberado: 7.5GB
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

### Dia a Dia

```
1. Puxe mudanças do GitHub
   git pull origin main

2. Abra notebook no Databricks
   Workspace → Repos → notebook.py

3. Faça alterações
   (Databricks auto-save a cada linha)

4. Teste e valide outputs

5. Commit no GitHub
   (Via Databricks Repos ou terminal)
   
6. Push para GitHub
   git push origin main
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
- Fork este repositório (clone)
- Crie branch (`git checkout -b feature/melhoria`)
- Commit mudanças (`git commit -m 'feat: add improvement'`)
- Push para branch (`git push origin feature/melhoria`)
- Open Pull Request (ou simplesmente em seu repo)

### Para Melhorar Este Template
Sugestões e issues são bem-vindas! (em versões públicas)

---

## ⚠️ Troubleshooting Rápido

### Problema: "Storage limit exceeded"
```
Solução:
1. Verifique espaço usado: dbutils.fs.du("/mnt/data")
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

### Problema: "Git merge conflicts"
```
Solução:
1. git pull origin main
2. Resolva conflitos manualmente
3. git add .
4. git commit -m "merge: resolve conflicts"
5. git push origin main
```

Mais soluções em: [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)

---

## 📊 Métricas de Sucesso

| Métrica | Meta | Status |
|---------|------|--------|
| Código versionado | 100% no GitHub | ⬜ |
| Features criadas | 40+ | ⬜ |
| Modelo RMSE | < $2.50 | ⬜ |
| Dados processados | 680M registros | ⬜ |
| Compressão | 80% (7.5GB→1.5GB) | ⬜ |
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

- [Databricks Community Edition Slack](https://databricks.com/slack)
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
- 🔗 [Databricks](https://community.cloud.databricks.com)
- 📊 [Dataset](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- 📚 [Docs](./docs)

---

**Aproveite o projeto! 🚕✨**

Dúvidas? Abra uma issue no GitHub ou consulte a documentação.
