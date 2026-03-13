# 💳 Guirra Solution — Plataforma de Conciliação Financeira de Meios de Pagamento

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.0+-150458?style=flat-square&logo=pandas&logoColor=white)
![Excel](https://img.shields.io/badge/Excel-openpyxl-217346?style=flat-square&logo=microsoftexcel&logoColor=white)
![HTML](https://img.shields.io/badge/Dashboard-HTML%2FJS%2FChart.js-E34F26?style=flat-square&logo=html5&logoColor=white)
![Status](https://img.shields.io/badge/Status-Concluído-00A878?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)

**Projeto de portfólio profissional** simulando uma plataforma completa de conciliação financeira para uma adquirente de meios de pagamento (Adquirente XZ), cobrindo 11 módulos de conciliação, 83.853 registros e R$ 1,49M em volume processado.

[📊 Ver Dashboard](#-dashboard) · [📁 Estrutura do Projeto](#-estrutura-do-projeto) · [🚀 Como Executar](#-como-executar) · [📋 Resultados](#-resultados-por-módulo)

</div>

---

## 🎯 Objetivo

Construir do zero uma plataforma de conciliação financeira de meios de pagamento que seja capaz de:

- **Cruzar** transações entre sistema interno (ERP) e adquirente usando múltiplas chaves de conciliação
- **Calcular** MDR contratado vs cobrado, custos operacionais e recebíveis líquidos
- **Monitorar** chargebacks, disputas e aging de prazo de contestação
- **Visualizar** resultados em dashboard interativo com 8 abas temáticas
- **Documentar** todo o processo em relatório executivo com fluxo de controle e boas práticas

---

## 📦 Bases de Dados

> Dados fictícios gerados para estudo. Nenhum dado real foi utilizado.

| Base | Registros | Colunas | Descrição |
|------|-----------|---------|-----------|
| `BASE_INTERNA.csv` | 19.692 | 31 | Transações do sistema interno (ERP/gateway) |
| `BASE_INTERNA_DEBITOS.csv` | 3.108 | 33 | Reembolsos e estornos internos |
| `THIRD_PARTY.csv` | 51.803 | 37 | Extrato da adquirente XZ (expandido por parcela) |
| `DEBITOS_THIRD_PARTY.csv` | 9.250 | 38 | Débitos de reembolso da adquirente |

**Total:** 83.853 registros · **Período:** Jan–Mar 2025 · **Bandeiras:** Visa, Mastercard, Elo, Amex

---

## 🗂 Estrutura do Projeto

```
guirra-solution/
│
├── src/conciliacao/
│   ├── utils.py                  # Normalizadores, carregadores, helpers comuns
│   ├── m00_dicionario_custos.py  # MDR contratado, tarifas, impostos, CB reason codes
│   ├── m01_pagamentos_anulacoes.py # Módulo 1: Pagamentos e Anulações
│   ├── m02_m11_modulos.py        # Módulos 2 a 11 (conciliações especializadas)
│   └── pipeline.py               # Orquestrador: executa tudo e gera o Excel
│
├── docs/
│   ├── BPMN_FluxoConciliacao.md  # Fluxograma BPMN detalhado do processo
│   ├── CHAVES_CONCILIACAO.md     # Tabela de chaves por módulo
│   └── DICIONARIO_DADOS.md       # Dicionário de campos das 4 bases
│
├── outputs/                      # Gerados pelo pipeline (não versionados)
│   ├── GuirraSolution_Fase2_Conciliacao.xlsx
│   └── GuirraSolution_Fase4_RelatorioExecutivo.docx
│
├── dashboard/
│   └── GuirraSolution_Dashboard.html  # Dashboard interativo (Chart.js)
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🔑 Chaves de Conciliação por Módulo

| Módulo | Par de Bases | Chave Primária | Chave Secundária | Tolerância |
|--------|-------------|----------------|-----------------|------------|
| M01 · Pagamentos | BI × TP | `NSU` (int64) | `PAYMENT_ID` × `ORDER_ID` | ±R$0,02 · D+2 |
| M02 · Reembolsos | BID × DTP | `NSU_KEY` (str norm.) | `PAYMENT_ID` × `ORDER_ID` | ±R$0,02 |
| M03 · CB Notificados | BI(cb) × TP(cb) | `NSU_KEY` left join | `CB_REASON_CODE` | Valor abs. ±R$0,02 |
| M04 · Disputas | BI (intra) | `NSU_KEY` + `TIPO_MOVIMENTO` | `DATA` (aging) | 45 dias corridos |
| M05 · CB Debitados | BI(cb) × TP(cb) | `NSU_KEY` left join | `STATUS` (capturada) | ±R$0,02 |
| M06 · Taxas/MDR | TP × Dicionário | `BANDEIRA`+`TIPO`+`PARCELAS` | `MCC` | Dif. > R$0,05 |
| M07 · Fluxo de Caixa | BI + BID | `DATA_ARQUIVO` (diário) | `TIPO_MOVIMENTO` | Granularidade D |
| M08 · Parcelamento | BI × TP | `NSU_KEY` outer join | `QTDE_PARCELAS` (contagem) | ±R$0,02 |
| M09 · Antecipações | TP (parceladas) | `NSU_KEY` + `PARCELA_NUM` | `DATA_CAPTURA` (agenda) | 1,99% a.m. |
| M10 · Recebíveis | TP × MDR × BI | `NSU_KEY` (TP vendas) | `BANDEIRA`+`TIPO` (MDR) | Margem ≥ 0% |
| M11 · Remessas | TP (liquidadas) | `MERCHANT_NUM_ADQ` + `DATA` | `STATUS` (liquidada) | Direto |

---

## 📊 Dashboard

Dashboard interativo em HTML/CSS/JS puro com **8 abas temáticas**, sidebar retrátil, dark theme e Chart.js:

| Aba | Conteúdo Principal |
|-----|--------------------|
| ⬡ Visão Geral | KPIs globais, TPV mensal, mix por bandeira e canal |
| ⇄ Resumo Conciliação | Funil, matriz bandeira × status, distribuição |
| ◈ Pagamentos / Anulações | M01 — status, causas raiz, análise de exceções |
| ↩ Reembolsos | M02 — BID vs DTP, anomalias de valor negativo |
| ⊞ Parcelamentos | M08 — distribuição por parcelas, MDR crescente |
| ⚑ Chargebacks | M03/M04/M05 — aging, pipeline, debitados |
| ◎ Taxas & Custos | M06 — MDR contratado vs cobrado, waterfall |
| ◬ Recebíveis | M10 — waterfall bruto→líquido, agenda, fluxo de caixa |

> Abrir `dashboard/GuirraSolution_Dashboard.html` diretamente no navegador — sem servidor necessário.

---

## 📋 Resultados por Módulo

| Módulo | Total | Conciliados | Taxa | KPI Principal |
|--------|-------|-------------|------|---------------|
| M01 · Pagamentos e Anulações | 17.924 | 15.641 | **91,6%** | R$ 492.962 conciliados |
| M02 · Reembolsos | 3.108 | 0* | —* | 100% NSU coberto |
| M03 · CB Notificados | 3.971 | 2.881 | 72,5% | R$ 93.999 em CBs |
| M04 · Disputas | 1.719 | 634 | 36,9% | R$ 55.171 em risco |
| M05 · CB Debitados | 1.973 | 1.348 | 68,4% | R$ 42.170 debitados |
| M06 · Taxas e Custos | 17.972 | 16.809 | 93,5% | Dif. MDR: R$ -0,46 |
| M07 · Fluxo de Caixa | 90 dias | 87 dias | 96,7% | Saldo: R$ 122.437 |
| M08 · Parcelamento | 10.246 | 8.575 | 83,7% | 0 parcelas incompletas |
| M09 · Antecipações | Calculado | 100% | 100% | R$ 249,74 líquido |
| M10 · Recebíveis Líquidos | 17.972 | 17.972 | 100% | R$ 500.765 líquido |
| M11 · Remessas Comércios | 7.564 | 7.564 | 100% | R$ 611.075 remessas |

> *M02: divergência estrutural de granularidade BID×DTP — após agrupamento, cobertura NSU é 100%.

### KPIs Globais

```
TPV Total          → R$ 1.494.481,18
TPT Total          → 47.281 transações
Taxa Conciliação   → 91,6% (meta: ≥ 98%)
MDR Contratado     → R$ 14.841,28 (2,86% médio)
Custo Operacional  → R$ 17.961,75 (3,46% do TPV)
Recebível Líquido  → R$ 500.764,72 (96,5% de margem)
Saldo Operacional  → R$ 122.436,83
```

---

## 🚀 Como Executar

### Pré-requisitos

```bash
python >= 3.11
pip install pandas numpy openpyxl
```

### Instalação

```bash
git clone https://github.com/SEU_USUARIO/guirra-solution.git
cd guirra-solution
pip install -r requirements.txt
```

### Execução do Pipeline Completo

```bash
# Coloque os 4 CSVs na pasta data/
mkdir -p data
# BASE_INTERNA.csv, BASE_INTERNA_DEBITOS.csv, THIRD_PARTY.csv, DEBITOS_THIRD_PARTY.csv

# Executar pipeline completo (todos os 11 módulos)
cd src/conciliacao
python pipeline.py

# Output gerado em: outputs/GuirraSolution_Fase2_Conciliacao.xlsx
```

### Executar Módulo Individual

```python
from src.conciliacao.utils import carregar_base_interna, carregar_third_party
from src.conciliacao.m01_pagamentos_anulacoes import executar as m01

df_bi = carregar_base_interna(deduplicar_nsu=True)
df_tp = carregar_third_party(agrupar_por_nsu=True)

resultado = m01(df_bi, df_tp)
print(resultado['kpis'])
```

### Dashboard

Abrir diretamente no navegador:
```bash
open dashboard/GuirraSolution_Dashboard.html
# ou simplesmente clique duas vezes no arquivo
```

---

## ⚙️ Arquitetura do Pipeline

```
CSV Input (4 bases)
        │
        ▼
┌───────────────────────────────────────┐
│  utils.py — Normalização & Ingestão   │
│  • Parser multi-formato de datas       │
│  • Deduplicação de NSU                 │
│  • Normalização de valores             │
└───────────────┬───────────────────────┘
                │
        ┌───────┴────────┐
        ▼                ▼
┌──────────────┐  ┌──────────────────────┐
│ M00 MDR Dict │  │  M01–M11 Módulos     │
│ • Tabela MDR │  │  Conciliação por par │
│ • Tarifas    │  │  de bases            │
│ • Impostos   │  │  Status por NSU      │
│ • CB Reasons │  └──────────┬───────────┘
└──────────────┘             │
                             ▼
                    ┌────────────────────┐
                    │   pipeline.py      │
                    │ Orquestra M01–M11  │
                    │ Gera Excel 20 abas │
                    └────────┬───────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        Excel Report    Dashboard      Relatório
        (20 abas)       HTML/JS        Executivo
                                        DOCX
```

---

## 🔍 Pontos de Atenção Identificados

| Prioridade | Achado | Módulo | Ação Recomendada |
|------------|--------|--------|-----------------|
| 🔴 CRÍTICO | 2.971 CBs com prazo vencido (>45 dias) | M03 | Acionar adquirente imediatamente |
| 🔴 CRÍTICO | 49 NSUs duplicados na BASE_INTERNA | M01 | Corrigir na origem (ERP/gateway) |
| 🟡 ALTA | 269 refunds com valor negativo | M02 | Verificar sinal contábil no sistema |
| 🟡 ALTA | Gap Mastercard: +16pp na TP vs BI | M01 | Investigar base complementar |
| 🟡 MÉDIA | 4 formatos de data coexistentes | Todos | Padronizar para ISO-8601 UTC |
| 🟡 MÉDIA | Taxa conciliação em 91,6% (meta 98%) | M01 | Tratar autorizadas sem captura |

---

## 🛠 Stack Técnica

| Camada | Tecnologia |
|--------|-----------|
| Linguagem | Python 3.11 |
| Manipulação de dados | Pandas 2.0, NumPy |
| Exportação Excel | openpyxl (formatação profissional) |
| Dashboard | HTML5 + CSS3 + Vanilla JS + Chart.js 4.4 |
| Relatório Executivo | docx-js (Node.js) |
| Controle de versão | Git / GitHub |

---

## 📁 Entregáveis

| Fase | Entregável | Descrição |
|------|-----------|-----------|
| Fase 1 | `GuirraSolution_Fase1_Diagnostico.docx` | EDA completa, diagnóstico de qualidade de dados |
| Fase 2 | `GuirraSolution_Fase2_Conciliacao.xlsx` | Excel com 20 abas — todos os módulos M01–M11 |
| Fase 3 | `GuirraSolution_Dashboard.html` | Dashboard interativo 8 abas (sem servidor) |
| Fase 4 | `GuirraSolution_Fase4_RelatorioExecutivo.docx` | Relatório executivo com chaves, alertas e fluxo de controle |
| Fase 5 | `README.md` + `BPMN` + `DICIONARIO_DADOS.md` | Documentação final para portfólio |

---

## 📄 Licença

Este projeto é de uso educacional e para portfólio profissional. Dados são inteiramente fictícios.

MIT License — veja [LICENSE](LICENSE) para detalhes.

---

<div align="center">

**Guirra Solution** · Plataforma de Conciliação Financeira de Meios de Pagamento  
Projeto de Portfólio · Dados Fictícios · Março 2025

</div>

