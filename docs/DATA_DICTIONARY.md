# 📖 Dicionário de Dados — NYC Yellow Taxi Analytics

Este documento descreve todas as colunas do dataset, desde as originais (fornecidas pela NYC TLC) até as features criadas no projeto (Fase 3).

---

## 1️⃣ Colunas Originais (Fonte: NYC TLC)

| Coluna | Tipo | Descrição |
|---|---|---|
| `VendorID` | int | Fornecedor do TPEP: 1=Creative Mobile Technologies, 2=VeriFone Inc. |
| `tpep_pickup_datetime` | timestamp | Data/hora em que o taxímetro foi ativado |
| `tpep_dropoff_datetime` | timestamp | Data/hora em que o taxímetro foi desativado |
| `passenger_count` | int | Número de passageiros (valor inserido pelo motorista) |
| `trip_distance` | float | Distância percorrida em **MILHAS** (não km — ver nota abaixo) |
| `pickup_longitude` | float | Longitude de onde o taxímetro foi ativado |
| `pickup_latitude` | float | Latitude de onde o taxímetro foi ativado |
| `dropoff_longitude` | float | Longitude de onde o taxímetro foi desativado |
| `dropoff_latitude` | float | Latitude de onde o taxímetro foi desativado |
| `RatecodeID` | int | Tarifa final: 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated fare. **Nos dados reais também aparecem os códigos 6 e 99, não documentados oficialmente (volume irrelevante: 607 registros)** |
| `store_and_fwd_flag` | string | Y=corrida guardada no veículo por falta de conexão antes de enviar; N=enviada em tempo real |
| `payment_type` | int | 1=Cartão de crédito, 2=Dinheiro, 3=Sem cobrança, 4=Disputa, 5=Desconhecido, 6=Corrida anulada |
| `fare_amount` | float | Tarifa calculada pelo taxímetro (tempo + distância) |
| `extra` | float | Taxas extras (rush hour $0.50, noturna $1.00) |
| `mta_tax` | float | Taxa fixa de $0.50 (MTA) |
| `improvement_surcharge` | float | Taxa fixa de $0.30 (vigente desde 2015) |
| `tip_amount` | float | Gorjeta — **só registra gorjetas em cartão**; gorjetas em dinheiro NÃO aparecem aqui |
| `tolls_amount` | float | Total de pedágios pagos na corrida |
| `total_amount` | float | Total cobrado do passageiro — **NÃO inclui gorjetas em dinheiro** |

> ⚠️ **Correção de unidade:** `trip_distance` é medido em **milhas**, não km, conforme confirmado pelo dicionário oficial da NYC TLC. Isso foi corrigido em toda a documentação do projeto (inicialmente presumimos "km" por engano na Fase 2).

> ⚠️ **Gorjetas em dinheiro:** `tip_amount` e `total_amount` refletem apenas pagamentos em cartão. Toda análise ou modelo de ML sobre gorjeta deve filtrar `payment_type = 1` para não interpretar "gorjeta em dinheiro" como "sem gorjeta".

---

## 2️⃣ Features Criadas — Fase 3 (Feature Engineering)

### Conversão de Unidade

| Feature | Tipo | Descrição |
|---|---|---|
| `trip_distance_km` | float | `trip_distance` (milhas) convertido para km — fator 1,60934. Coluna original em milhas mantida intacta. |

### Temporais

| Feature | Tipo | Descrição |
|---|---|---|
| `pickup_hour` | int | Hora do dia da retirada (0-23) |
| `pickup_day_of_week` | int | Dia da semana (1=domingo ... 7=sábado) |
| `pickup_month` | int | Mês da retirada |
| `pickup_year` | int | Ano da retirada (2015 ou 2016) |
| `is_weekend` | int | 1 se sábado/domingo, 0 caso contrário |
| `is_rush_hour` | int | 1 se horário de pico (6h-9h ou 16h-19h), 0 caso contrário |
| `time_of_day` | string | `manha` (6-11h), `tarde` (12-16h), `noite` (17-21h), `madrugada` (demais horários) |

> Nota: `pickup_date` já havia sido criada na Fase 1 (ingestão), usada para particionamento do Delta Lake.

### Aeroporto

| Feature | Tipo | Descrição |
|---|---|---|
| `is_airport_trip` | int | 1 se `RatecodeID` in (2, 3) — JFK ou Newark — 0 caso contrário |
| `airport_type` | string | `"JFK"`, `"Newark"` ou `"N/A"`. Calculado via `RatecodeID`, sem necessidade de cálculo geoespacial por coordenadas |

### Velocidade e Duração

| Feature | Tipo | Descrição |
|---|---|---|
| `trip_duration_minutes` | float | Diferença entre `tpep_dropoff_datetime` e `tpep_pickup_datetime`, em minutos |
| `duracao_valida` | int | 1 se duração entre 1 e 300 minutos, 0 caso contrário (sinaliza qualidade, não remove a linha) |
| `speed_kmh` | float ou null | Velocidade média em km/h. `NULL` quando `duracao_valida = 0` (velocidade não confiável) |
| `speed_category` | string | `"lenta"` (<10km/h), `"normal"` (10-30km/h), `"rapida"` (≥30km/h), ou `"duracao_invalida"` quando `speed_kmh` é nulo |

### Tarifa e Gorjeta

| Feature | Tipo | Descrição |
|---|---|---|
| `fare_per_km` | float | `fare_amount` dividido por `trip_distance_km` |
| `tip_percentage` | float ou null | `(tip_amount / fare_amount) * 100`. Calculado **apenas** para `payment_type = 1` (cartão); `NULL` para outros tipos de pagamento |
| `tip_category` | string | `"sem_gorjeta"` (0%), `"baixa"` (<10%), `"media"` (10-20%), `"alta"` (≥20%), ou `"nao_aplicavel"` quando `payment_type != 1` (dinheiro e outros) |

---

## 3️⃣ Regras de Limpeza Aplicadas (Fase 3)

Definidas com base nos percentis reais observados na Fase 2 (EDA):

| Regra | Limite | Registros Removidos |
|---|---|---|
| `trip_distance` | entre 0,1 e 200 milhas | 53.888 (abaixo de 0,1mi) + 184 (acima de 200mi) |
| `fare_amount` | entre $2,5 e $500 | 2.600 (abaixo) + 105 (acima) |
| `passenger_count` | entre 1 e 8 | 6.628 |
| `tip_amount` | ≥ 0 | 3 |
| `total_amount` | ≥ 0 | 5 |
| **Total único removido** | — | **63.182 (0,13% do dataset)** |

> As regras não são mutuamente exclusivas — um mesmo registro pode violar mais de uma regra simultaneamente, por isso a soma individual das contagens é maior que o total único removido.

---

## 4️⃣ Achados de Qualidade de Dados (Referência)

- **Nulos:** praticamente inexistentes em todas as colunas originais
- **Duplicatas:** 4 registros em 46,9M (irrelevante)
- **Outliers extremos:** menos de 300 registros no total antes da limpeza (0,0006%)
- **RatecodeID:** códigos 6 e 99 aparecem nos dados mas não são documentados oficialmente pela TLC (607 registros)
- **Validação de confiabilidade:** a tarifa média de viagens com `RatecodeID = 2` (JFK) é $52,54, batendo com a tarifa fixa histórica real (~$52) — forte evidência da qualidade dos dados

---

*Última atualização: Fase 3 (Feature Engineering) concluída.*