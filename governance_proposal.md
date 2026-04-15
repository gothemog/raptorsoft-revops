# Proposta de Governança Contínua — RaptorSoft RevOps
## Pipeline Health Intelligence & Churn Risk Signal
**Versão 4.0 | Referência: Plano Mestre v4, Seção 12**

---

## 1. Contexto e Motivação

O pipeline de RevOps construído neste projeto integrou pela primeira vez dados de Salesforce, HubSpot, Catalyst e Telemetria de Produto em uma visão unificada. A análise revelou dois sintomas com causa-raiz comum:

- **Forecast overestimated em 23%:** sem slippage tracking e win rates endógenos, o forecast dependia da declaração do rep
- **Churn pós-renovação:** sem fechamento de loop entre performance pós-venda e qualificação de pipeline, clientes renovavam por inércia sem sinal de Impact real

A causa-raiz compartilhada — **desconexão entre Value (aquisição) e Impact (retenção)** — só se resolve com um pipeline de dados contínuo, não com uma análise pontual. Esta proposta define como operacionalizar esse pipeline.

---

## 2. Arquitetura de Dados — Estado Alvo

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Salesforce  │    │   HubSpot    │    │   Catalyst   │    │  Telemetria  │
│  (pipeline)  │    │   (leads)    │    │  (contratos) │    │   (produto)  │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │                   │
       └───────────────────┴───────────────────┴───────────────────┘
                                     │
                         ┌───────────┴──────────┐
                         │  Pipeline ETL diário  │
                         │  (normalização +      │
                         │   entity resolution + │
                         │   joins validados)    │
                         └───────────┬──────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                │                │
             ┌──────┴─────┐  ┌──────┴─────┐  ┌──────┴─────┐
             │  Dashboard  │  │   Alertas  │  │  Modelos   │
             │ (Streamlit) │  │   (Slack/  │  │  (Churn +  │
             │             │  │    Email)  │  │  Forecast) │
             └────────────┘  └────────────┘  └────────────┘
```

---

## 3. Pipeline de Dados Automatizado

### 3.1 Frequência de atualização

| Fonte | Frequência | Justificativa |
|-------|-----------|---------------|
| Catalyst CS | Diária | Health score e sinais de churn mudam rapidamente |
| Telemetria de Produto | Diária | Sessões e feature_depth são leading indicators |
| Salesforce | Semanal | Stage e forecast_category mudam com menor urgência |
| HubSpot | Semanal | Leads e MQLs têm ciclo semanal tipicamente |

### 3.2 Gatilhos de re-processamento imediato

Os eventos abaixo devem acionar re-run do pipeline fora do ciclo regular:

| Evento | Gatilho | Ação |
|--------|---------|------|
| Mudança de stage no SF | `stage != stage_anterior` | Recalcular forecast contribution |
| Mudança de forecast_category | `forecast_category != anterior` | Recalcular forecast; alertar se Commit mudou |
| Mudança de amount_brl | `|delta| > 10%` | Recalcular forecast; alertar se Commit |
| Queda de health_score > 10 pts em 7 dias | Catalyst webhook | Alerta CS imediato + recalcular churn_risk_score |
| `active_users = 0` por 14 dias consecutivos | Telemetria | Alerta de churn risk + recalcular engagement_modifier |
| Novo registro com `amount_brl < 0` | SF | Alerta para validação manual (possível estorno) |
| Novo `account_name` desconhecido | Qualquer fonte | Alerta para atualização da tabela de entity resolution |

### 3.3 Sequência de execução

```
1. Ingestão dos 4 datasets (extração via API)
2. Aplicar normalize_canonical() em todos os account_names
3. Detectar novos nomes → alertar para resolução humana se confiança < 90%
4. Executar data_treatment.py (normalização + joins + flags)
5. Executar churn_model.py (Layer 2 → Layer 1 → score final)
6. Executar forecast_model.py (win rates + slippage)
7. Executar wbd_metrics.py (GRR + CR4 + CR5 + Δt6 + ICP)
8. Publicar dashboard atualizado
9. Disparar alertas configurados
10. Registrar log de execução com contagem de registros e anomalias
```

---

## 4. Monitoramento de Qualidade de Dados

### 4.1 Alertas de degradação

| Métrica | Threshold de alerta | Ação |
|---------|--------------------|----|
| % NULLs em `health_score` | > threshold_histórico + 10% | Investigar Catalyst API |
| % NULLs em `mrr_usd` | > 5% dos Active | Revisar processo de sync do Catalyst |
| Taxa de join failure (canonical_name não resolvido) | > 2% de registros | Executar entity resolution review |
| Duplicatas em `opportunity_id` | qualquer | Bloquear pipeline + alerta crítico |
| Duplicatas em `hs_contact_id` | qualquer | Alerta para revisão de dedup |
| Novas variantes de account_name | qualquer | Fila de resolução para RevOps |
| Conta sem canonical_segment | qualquer | Alerta + bloquear conta de métricas segmentadas |
| GRR mensal cai > 5 p.p. | MoM | Alerta para liderança de CS |

### 4.2 Dashboard de qualidade de dados

Recomenda-se um painel interno (pode ser aba adicional no dashboard atual) com:
- Volume de registros por fonte (trend semanal)
- % de NULLs por campo crítico (trend)
- Taxa de match de entity resolution (trend)
- Contagem de alertas disparados na última semana
- Lista de account_names em fila de resolução

### 4.3 Contrato semântico entre sistemas

Para garantir estabilidade da integração, cada produtor de dados deve publicar e manter:

| Sistema | Campo contratado | Tipo esperado | SLA de atualização |
|---------|-----------------|--------------|-------------------|
| Salesforce | `opportunity_id` | string não-NULL | imediato (trigger) |
| Salesforce | `amount`, `stage`, `forecast_category` | numérico/string | imediato (trigger) |
| HubSpot | `sf_opportunity_id` | string (quando aplicável) | semanal |
| Catalyst | `customer_id`, `health_score`, `mrr_usd`, `status` | tipos definidos | diário |
| Telemetria | `account_domain`, `week_start`, `feature_depth_score` | string/date/float | diário |

---

## 5. Divisão de Responsabilidades — IA vs. Humano

| Ação | IA resolve autonomamente | Humano valida |
|------|--------------------------|---------------|
| Normalização de campos (stage, lead_source, forecast_category) | ✅ | — |
| Cálculo de Churn_Risk_Score (2 camadas) | ✅ | — |
| Recalcular forecast com slippage progressivo | ✅ | — |
| Cálculo de GRR, CR4, CR5, Δt6 | ✅ | — |
| Alertas automáticos de renovação | ✅ | — |
| Entity resolution para nomes com confiança > 90% | ✅ | — |
| Entity resolution para nomes com confiança 70–90% | ✅ sugere match | ✅ confirma |
| Entity resolution para nomes com confiança < 70% | — | ✅ resolve |
| Imputação de MRR faltante | — | ✅ sempre — valor de negócio |
| Reclassificação Active → Churned | ✅ aplica critério automático | ✅ valida edge cases |
| Atualização de pesos do churn model | — | ✅ trimestral com CRO/CS |
| Atualização de tiers de slippage discount | — | ✅ trimestral (baseado em delta real) |
| Atribuição de canonical_segment para novas empresas | — | ✅ sempre — decisão estratégica |
| Calibração de thresholds de churn (backtesting) | ✅ sugere threshold ótimo | ✅ aprova mudança |
| Detecção de champion change (Resell risk) | ✅ detecta via email/nome | ✅ valida ação |
| Investigação de anomalias > 2σ | ✅ sinaliza | ✅ investiga |

**Princípio orientador:** IA deve resolver o que tem regras claras e verificáveis. Humano deve validar o que tem implicação de negócio não capturável por regra (valor de MRR, estratégia de segmento, peso de modelo).

---

## 6. Implementação por Horizonte

### Horizonte 1 — Primeiros 15 dias (fundação analítica)

O que já está entregue como base:

- [x] Pipeline de normalização dos 4 datasets (1.200 + 3.400 + 680 + 12.000 registros)
- [x] Tabela mestre de 20 empresas com canonical_segment (employees-first)
- [x] Dashboard v1 funcional com dados estáticos (Streamlit, 4 seções)
- [x] Modelo de churn risk v1 (2 camadas) + backtesting inicial
- [x] Cálculo de GRR, CR4, CR5, Δt6, ICP
- [x] Forecast com slippage progressivo + simulador interativo
- [ ] Alertas manuais de renovação (lista semanal via email — pode ser gerada pelo pipeline)

**Meta:** RevOps tem visibilidade unificada pela primeira vez. Modelo de churn e forecast operacionais manualmente.

### Horizonte 2 — 30 a 90 dias (automação e integração)

| Tarefa | Responsável | Prioridade |
|--------|-------------|-----------|
| Configurar extração via API de SF, HubSpot, Catalyst e Telemetria | Engenharia de Dados | Alta |
| Agendar pipeline ETL diário (Airflow, Prefect ou similar) | Engenharia de Dados | Alta |
| Integrar alertas de churn e renovação via Slack/email | RevOps + Engenharia | Alta |
| Calibrar tiers de slippage com delta real do Q2/25 | RevOps + CRO | Alta |
| Validar recall do churn model com dados prospectivos (Q2/25) | RevOps + CS | Média |
| Publicar dashboard em Streamlit Community Cloud | RevOps | Média |
| Construir painel de qualidade de dados (aba interna) | Engenharia de Dados | Média |
| Documentar contrato semântico com cada sistema produtor | RevOps + TI | Média |

**Meta:** Pipeline rodando automaticamente. Alertas operacionais. Primeiro ciclo de validação do modelo com dados reais.

### Horizonte 3 — 90+ dias (maturidade e feedback loop)

| Tarefa | Responsável | Valor esperado |
|--------|-------------|---------------|
| Recalibrar pesos do churn model trimestralmente com feedback de CS | RevOps + CS | Modelo mais preciso ao longo do tempo |
| Implementar Resell risk detection (champion change) | Engenharia + RevOps | Reduzir churn pós-renovação |
| Atualizar Closed Loop ICP mensalmente | RevOps | Marketing alinhado com perfil real de cliente saudável |
| Capturar MRR pré-churn no Catalyst (série temporal) | Engenharia | GRR real em vez de estimado |
| Construir agente de IA para queries em linguagem natural | Engenharia de IA | Democratizar acesso aos dados |
| Expandir canonical_segment para novos clientes automaticamente | RevOps + TI | Escalabilidade sem intervenção manual |
| Fechar loop: ICP → critérios de qualificação de lead no HubSpot | RevOps + Marketing | Reduzir CAC, aumentar win rate |

**Meta:** RevOps como função estratégica com feedback loop completo Value↔Impact.

---

## 7. KPIs de Saúde do Pipeline de Dados

Para medir a efetividade da governança em si:

| KPI | Meta | Frequência de medição |
|-----|------|----------------------|
| % de registros com canonical_name resolvido | 100% | A cada run |
| % de registros com canonical_segment atribuído | 100% | A cada run |
| Taxa de join failure (qualquer fonte) | < 1% | A cada run |
| Latência do pipeline (ingestão → dashboard) | < 4 horas | Diária |
| % de alertas de churn respondidos em < 24h | > 80% | Semanal |
| Recall do churn model Alto (prospectivo) | ≥ 40% | Trimestral |
| Delta forecast × realizado (accuracy) | < 15% | Trimestral |
| % de campos críticos com NULL < threshold | 100% dos campos monitorados | A cada run |

---

## 8. Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|--------------|---------|----------|
| Mudança de schema em um dos sistemas de origem | Alta | Alto | Testes de schema no início de cada run; alertas se campos críticos desaparecerem |
| Degradação silenciosa de qualidade (NULLs crescentes) | Média | Alto | Dashboard de qualidade com trend histórico |
| Entity resolution falhando para novas empresas | Média | Médio | Fila de resolução + alerta automático para RevOps |
| Modelo de churn desatualizado (drift) | Alta | Alto | Backtesting trimestral obrigatório; recalibração automática de threshold |
| Dependência de taxa de câmbio fixa (R$/USD 5,70) | Média | Médio | Parametrizar taxa; atualizar mensalmente via API de câmbio |
| Dashboard inacessível (downtime) | Baixa | Médio | Deploy em cloud; versão de emergência com CSVs estáticos |

---

*Proposta de Governança Contínua — RaptorSoft RevOps v4*
*Gerado com Claude Code (Anthropic) | claude-opus-4-6 | Data de referência: 2025-04-15*
