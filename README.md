# RaptorSoft RevOps — Pipeline Health Intelligence & Churn Risk Signal

Case tecnico de Revenue Operations: pipeline de dados integrando Salesforce, HubSpot, Catalyst CS e Telemetria de Produto para uma empresa SaaS B2B ficticia.

---

## Dashboard Interativo

**Acesse o dashboard ao vivo:** [raptorsoft-revops.streamlit.app](https://raptorsoft-revops.streamlit.app)

> Se o link acima estiver indisponivel, execute localmente:
> ```bash
> pip install -r requirements.txt
> streamlit run app.py
> ```

---

## Entregaveis

| # | Entregavel | Arquivo | Descricao |
|---|-----------|---------|-----------|
| 1 | **Data Treatment Report** | [`data_treatment_report.md`](data_treatment_report.md) | Documentacao completa: entity resolution, joins, anomalias, limitacoes |
| 2 | **Dashboard Interativo** | [`app.py`](app.py) | Streamlit com 4 secoes: Pipeline Health, Churn Risk, Simulador, Bowtie View |
| 3 | **Proposta de Governanca** | [`governance_proposal.md`](governance_proposal.md) | Pipeline automatizado, IA vs. humano, implementacao por horizonte |

---

## Arquitetura do Pipeline

```
pipeline_sf.csv (1.200 opps)          leads_hs.csv (3.400 leads)
         |                                      |
         +------ Entity Resolution (81 nomes -> 20 empresas) ------+
         |                                      |
health_catalyst.csv (680 contratos)    telemetry_product.csv (12.000 registros)
         |                                      |
         v                                      v
  +------+------+        +----------+     +-----------+
  | Churn Model |        | Forecast |     | Metricas  |
  | 2 Camadas   |        | Slippage |     | WBD       |
  | + Backtest  |        | Progress.|     | Bowtie    |
  +------+------+        +----+-----+     +-----+-----+
         |                     |                 |
         +---------------------+-----------------+
                               |
                     Dashboard Streamlit
                    (4 secoes interativas)
```

## Secoes do Dashboard

- **A — Pipeline Health:** Volume de pipeline, slippage heatmap, sinalizacao de oportunidades em risco, monitor de estornos
- **B — Churn Risk Signal:** GRR (72,2%), distribuicao de risco, timeline de renovacoes, lista de acao para CS, backtesting do modelo
- **C — Simulador de Cenarios:** Forecast com slippage ajustavel, threshold de churn interativo, simulador de intervencao CS
- **D — Bowtie View:** CR4 Win Rate Trendline, Dt6 Time to First Impact, Closed Loop ICP, CR5 ACV Trendline

## Numeros-chave

| Metrica | Valor |
|---------|-------|
| Empresas canonicas | 20 (de 81 variantes de nome) |
| GRR (taxa de sobrevivencia) | 72,2% (491 Active / 680 total) |
| Pipeline aberto | R$ 74,8M — 100% em slippage |
| Forecast ajustado Q2/25 | R$ 6,7M (desconto medio ~88%) |
| Churn recall Alto | 40,2% (target >= 40%) |
| Churn recall Alto+Medio | 73,0% (target >= 70%) |
| Dt6 mediana | 4 dias (company-level) |

## Estrutura do Repositorio

```
.
├── app.py                        # Dashboard Streamlit (959 linhas)
├── requirements.txt              # Dependencias
├── data_treatment_report.md      # Entregavel 1: Data Treatment Report
├── governance_proposal.md        # Entregavel 3: Proposta de Governanca
├── src/
│   ├── utils.py                  # Constantes, mapeamentos, funcoes puras
│   ├── data_treatment.py         # Fase 1: normalizacao + entity resolution + joins
│   ├── churn_model.py            # Fase 2: Contract_Risk x Engagement_Modifier
│   ├── forecast_model.py         # Fase 3: win rates + slippage progressivo
│   └── wbd_metrics.py            # Fase 4: GRR, CR4, CR5, Dt6, ICP
├── data/                         # 14 CSVs gerados pelo pipeline
├── pipeline_sf.csv               # Dados brutos: Salesforce (1.200 opps)
├── leads_hs.csv                  # Dados brutos: HubSpot (3.400 leads)
├── health_catalyst.csv           # Dados brutos: Catalyst (680 contratos)
└── telemetry_product.csv         # Dados brutos: Telemetria (12.000 registros)
```

## Stack

Python 3.11 | Streamlit | Pandas | NumPy | Plotly

## Como executar o pipeline completo

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Executar o pipeline de dados (gera os 14 CSVs em data/)
python src/data_treatment.py
python src/churn_model.py
python src/forecast_model.py
python src/wbd_metrics.py

# 3. Iniciar o dashboard
streamlit run app.py
```

---

*Case tecnico de RevOps | Framework WBD (Winning by Design) como narrativa unificadora*
