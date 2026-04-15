# Data Treatment Report — RaptorSoft RevOps Case
## Pipeline Health Intelligence & Churn Risk Signal
**Versão 4.0 | Data de referência: 2025-04-15**

---

## 1. Resumo Executivo

Este relatório documenta todas as decisões de tratamento de dados aplicadas no pipeline de preparação analítica da RaptorSoft. O pipeline integrou quatro datasets de sistemas distintos (Salesforce, HubSpot, Catalyst CS, Telemetria de Produto) em uma tabela analítica unificada, resolvendo problemas de fragmentação de identidade, granularidades incompatíveis, NULLs estruturais e anomalias de qualidade de dados.

**Descobertas críticas documentadas:**
- O campo `segment` do Salesforce é aleatório a nível de oportunidade — `canonical_segment` foi reconstruído via estratégia employees-first
- O Catalyst é um snapshot point-in-time (não série temporal), impactando o cálculo de GRR
- Nenhuma empresa atinge health_score ≥ 75 simultaneamente com feature_depth ≥ 0.60 (ICP absoluto)
- 100% do pipeline aberto está em slippage (close_date no passado)
- Os ciclos de venda declarados no case (45d/90d/150d) não são observáveis nos dados reais

**Datasets processados:**

| Dataset | Registros brutos | Registros limpos | Grain |
|---------|-----------------|-----------------|-------|
| `pipeline_sf.csv` | 1.200 | 1.200 | 1 linha = 1 oportunidade |
| `leads_hs.csv` | 3.400 | 3.400 | 1 linha = 1 contato/lead |
| `health_catalyst.csv` | 680 | 680 | 1 linha = 1 contrato ativo ou churned |
| `telemetry_product.csv` | 12.000 | 12.000 | 1 linha = 1 empresa × 1 semana |

---

## 2. Entity Resolution — As 20 Empresas Canônicas

### 2.1 Problema

Os quatro datasets utilizam nomes de empresas em formato livre, gerando 81 variantes brutas para apenas 20 entidades reais. A normalização superficial (apenas `lower() + strip()`) colapsa apenas parcialmente — pares como "BluePath"/"Blue Path", "Acme"/"Acme Corp."/"Acme Corp" e "Amplia"/"Amplia Soluções"/"Amplia Soluções Ltda" permanecem não resolvidos.

### 2.2 Algoritmo de normalização

```python
import re, unicodedata

def normalize_canonical(name):
    n = str(name).lower().strip()
    # 1. Remover sufixos empresariais
    for suffix in ['ltda', 'ltd', 'corp', 'corp.', 'tech', 'soluções',
                   'solucoes', 'sistemas', 'digital', 'group', 'grupo']:
        n = re.sub(r'\b' + suffix + r'\b', '', n).strip()
    # 2. Remover acentos (NFD decomposition)
    n = unicodedata.normalize('NFD', n)
    n = ''.join(c for c in n if unicodedata.category(c) != 'Mn')
    # 3. Colapsar espaços, pontos e hifens (resolve "Blue Path" vs "BluePath")
    n = re.sub(r'[\s.\-]+', '', n).strip()
    return n
```

**Resultado validado:** 81 variantes brutas → 20 nomes canônicos (tabela completa em `src/utils.py`).

### 2.3 Mapeamento de domínios

Para os registros de telemetria (identificados por `account_domain`), um dicionário estático de 20 domínios foi construído e validado manualmente:

```
acmecorp.com.br → acme | amplia.com.br → amplia | ...
```

Cobertura: 20/20 domínios com match (100% da telemetria coberta após consolidação para 20 empresas).

### 2.4 Distribuição de contratos no Catalyst

Após aplicação do algoritmo de normalização: **20 empresas × 34 contratos = 680 registros** (distribuição perfeitamente uniforme).

> **Nota:** A distribuição desigual reportada em versões anteriores do plano (ex.: "Beta Solutions com 28 contratos") era artefato da contagem por `account_name` bruto. Com normalização aplicada, a distribuição é uniforme — **artefato sintético do dataset do case**.

---

## 3. Diagnóstico de Qualidade de Dados

### 3.1 Campos com NULLs estruturais

| Dataset | Campo | NULLs | Tratamento aplicado |
|---------|-------|-------|---------------------|
| SF | `lead_source` | ~15% | Padronizado para "Unknown" via `LEAD_SOURCE_MAP` |
| SF | `forecast_category` | ~5% | Mapeado para "pipeline" (categoria padrão) |
| Catalyst | `health_score` | ~8% | W1 excluído do denominador (denominador dinâmico) |
| Catalyst | `health_trend` | ~12% | health_trend_score = 0.5 (neutro) |
| Catalyst | `nps_score` | ~20% | nps_bonus = 0.0 (neutro) |
| Catalyst | `last_qbr_date` | variável | no_qbr_signal derivado de último QBR > 90d |
| HubSpot | `number_of_employees` | ~10% | Fallback para mode do SF na atribuição de segment |

### 3.2 Anomalias identificadas e tratadas

#### P-C1 — Registros Stale (21 oportunidades)
**Definição:** Oportunidades abertas com `created_date` anterior a 365 dias da data de referência (2025-04-15) sem fechamento.

**Encontrado:** 21 registros (plano v4 estimava 20; diferença de 1 registro após recalcular `implied_days` com a data de referência real).

**Tratamento:** Flag `is_stale_opportunity = True`. Mantidos no dataset — exclusão ou reclassificação requer validação humana.

#### P-C2 — Amounts negativos (30 registros, R$ -2,98M)
**Definição:** `amount_brl < 0` — estornos, cancelamentos parciais ou ajustes negativos.

**Encontrado:** 30 registros em 15+ empresas distintas, totalizando R$ -2,98M.

**Tratamento:** Flag `is_negative_amount = True`. Excluídos de forecast, win rate e métricas de pipeline. Visualizados separadamente no Monitor de Estornos (Seção A do dashboard).

#### P-C3 — Slippage (857 oportunidades, 100% do pipeline aberto)
**Definição:** Oportunidade com stage aberto (`is_open = True`) e `close_date < REFERENCE_DATE`.

**Encontrado:** 857/857 oportunidades abertas (100%).

**Implicação crítica:** Todo o pipeline aberto está com close_date no passado. Isso invalida o forecast baseado apenas em stage/categoria declarada pelo rep — evidência direta do Sintoma 1 (forecast overestimated).

**Tratamento:** `slippage_days = (REFERENCE_DATE - close_date).days`. Desconto progressivo aplicado no forecast model.

#### P-C4 — MRR = 0 em contratos Active (20 registros → 19 imputados)
**Encontrado:** 20 contratos com status="Active" e `mrr_usd = 0`.

**Investigação:** 1 dos 20 tem `churn_date` preenchida → reclassificado para Churned via CA-06 *antes* da imputação.

**Tratamento:** 19 contratos Active recebem `mrr_usd` imputado pela mediana global calculada *antes* da reclassificação CA-06 (USD 8.319,83). Flag `mrr_imputed = True` nos 19 registros.

> **Decisão crítica documentada:** A mediana foi calculada no conjunto Active original (pré-reclassificação) para evitar viés. Calcular a mediana pós-reclassificação mudaria a pool de referência.

#### P-C5 — Registros Churned com MRR = 0 (189 registros)
**Encontrado:** Todos os 189 contratos com `status = "Churned"` têm `mrr_usd = 0`.

**Explicação:** O Catalyst registra MRR como snapshot do estado atual. Contratos churned perdem o MRR ao sair — o valor histórico não é preservado.

**Implicação para GRR:** MRR pré-churn estimado pela mediana global (USD 8.319,83 × n_churned). Documentado como limitação — ver Seção 7.

#### P-C6 — Reclassificação Active → Churned
**Critério:** Active com `churn_date` preenchida e `churn_date ≤ REFERENCE_DATE`.

**Encontrado:** 1 contrato reclassificado.

**Ordem de operação:** Imputação de MRR (P-C4) executada *antes* da reclassificação para garantir que a mediana de referência não seja contaminada.

---

## 4. Estratégia de Canonical Segment (employees-first)

### 4.1 Problema com o campo segment original

O campo `segment` no Salesforce é atribuído a nível de **oportunidade** (não de conta). Investigação dos dados revela que todas as 20 empresas têm oportunidades nos 3 segmentos (SMB, Mid-Market, Enterprise), frequentemente variando por rep ou trimestre. Trata-se de ruído estrutural, não de dado utilizável diretamente.

**Evidência confirmada:** Ciclos de venda por segment original (Closed Won):
- SMB: mediana de 104 dias
- Mid-Market: mediana de 128 dias
- Enterprise: mediana de 94 dias

Enterprise mais rápido que SMB contradiz toda lógica de negócio — confirma que o segment original é aleatório.

### 4.2 Estratégia employees-first (v4)

**Fonte primária:** `number_of_employees` do HubSpot, calculado como mediana por empresa canônica.

**Regras de segmentação:**
- ≤ 100 funcionários → SMB
- 101–500 funcionários → Mid-Market
- > 500 funcionários → Enterprise

**Fallback:** Mode do campo `segment` do SF (quando HubSpot não tem dados suficientes).

**Resultado:** 20/20 empresas classificadas com `segment_confidence = "HIGH"` (mediana de employees disponível para todas).

**Finding:** Todas as 20 empresas têm mediana de employees entre 194 e 495 → **todas classificadas como Mid-Market**. Isso é consistente com o perfil do produto declarado (plataforma de automação para times de operações — mercado Mid-Market) e é documentado como finding de dados, não como erro.

---

## 5. Mapa de Joins e Níveis de Confiança

### 5.1 Estratégia de integração

| Join | Chave primária | Cobertura | Nível de confiança |
|------|---------------|-----------|-------------------|
| HubSpot → SF | `sf_opportunity_id` ↔ `opportunity_id` | 749/749 opps únicas (100%) | **HIGH** |
| SF → Catalyst | `account_id` ↔ `sf_account_id` | 364/680 records (53,5%) | **HIGH** |
| SF → Catalyst | `canonical_name` (entity resolution) | 316/680 records (46,5%) | **MEDIUM** |
| Catalyst → Telemetria | `customer_id` ↔ `customer_id_fk` | 20/20 IDs únicos (100%) | **HIGH** |
| Telemetria → Todas | `account_domain` → `canonical_name` | 20/20 domínios (100%) | **HIGH** |

### 5.2 Esclarecimento sobre HubSpot↔SF

Não existem IDs fantasma na integração HS↔SF. Os 1.170 records do HubSpot apontam para 749 `opportunity_id` únicos — todos existem no SF. A diferença de 421 (1.170 - 749) são múltiplos contatos/leads associados à mesma oportunidade (um lead pode ter múltiplos contatos em papéis distintos: decisor, influenciador, técnico).

### 5.3 Validação de integridade

Após joins:
- SF: 1.200/1.200 oportunidades com `canonical_name` atribuído (100%)
- SF: 1.200/1.200 com `canonical_segment` atribuído (100%)
- Catalyst: 680/680 com `canonical_name` atribuído (100%)
- Telemetria: 12.000/12.000 com `canonical_name` via `account_domain` (100%)

---

## 6. Campos Derivados

### 6.1 Salesforce

| Campo | Lógica | Notas |
|-------|--------|-------|
| `stage_canonical` | Mapeamento de stage normalizado via `STAGE_MAP` | 6 stages canônicos |
| `forecast_category_canonical` | Normalizado via `FORECAST_CATEGORY_MAP` | pipeline/best_case/commit/omit |
| `is_open` | `stage_canonical in OPEN_STAGES` | não inclui Closed Won/Lost |
| `is_negative_amount` | `amount_brl < 0` | flag para estornos |
| `is_slippage` | `is_open AND close_date < REFERENCE_DATE` | 857/857 abertos |
| `slippage_days` | `(REFERENCE_DATE - close_date).days` quando is_slippage | base para desconto |
| `amount_brl` | `amount_usd × BRL_USD_RATE (5.70)` quando amount_brl NULL | taxa de câmbio fixada |
| `close_quarter` | `{year}Q{quarter}` derivado de `close_date` | para trendlines |
| `implied_days` | `(close_date - created_date).days` | para stale detection |
| `is_stale_opportunity` | `is_open AND implied_days > 365` | 21 registros |

### 6.2 Catalyst

| Campo | Lógica | Notas |
|-------|--------|-------|
| `days_to_renewal` | `(renewal_date - REFERENCE_DATE).days` | apenas Active |
| `renewal_proximity_score` | Tier-based: ≤30→1.0, ≤60→0.60, ≤90→0.30, >90→0.0 | componente W5 |
| `health_trend_score` | Declining→1.0, Stable→0.30, Improving→0.0, NULL→0.50 | componente W2 |
| `support_pressure` | normalizada 0–1 no pipeline | componente W4 |
| `nps_bonus` | ≤6→+0.10, 7-8→0.0, ≥9→−0.08, NULL→0.0 | ajuste ao contract_risk |
| `no_qbr_signal` | `last_qbr_date` NULL ou > 90 dias atrás | detrator +0.08 |
| `mrr_imputed` | True para os 19 contratos com MRR=0 imputado | flag de qualidade |

---

## 7. Limitações e Ressalvas

### 7.1 GRR — Catalyst como snapshot

O Catalyst não contém série temporal de MRR. O GRR calculado (72,2%) usa a taxa de sobrevivência de contratos como proxy:

```
GRR_contratos = n_active_current / (n_active_current + n_churned)
             = 491 / (491 + 189) = 72.2%
```

O MRR pré-churn dos 189 contratos churned é estimado pela mediana global (USD 8.319,83), gerando uma estimativa de MRR perdido de USD 1,57M. Este valor deve ser interpretado como **estimativa de ordem de magnitude**, não como MRR exato.

**Implicação:** Um GRR real calculado sobre série temporal tende a ser diferente. A recomendação é instrumentar o Catalyst para capturar MRR_start e MRR_end por contrato.

### 7.2 Backtesting do modelo de churn — análise pós-fato

O backtesting sobre os 189 contratos Churned é **indicativo**, não definitivo. O motivo: os dados do Catalyst são um snapshot do estado atual. Os campos `health_score`, `health_trend` e `active_users` dos contratos Churned podem refletir o estado pós-churn (já degradado), não o estado pré-churn que levou ao cancelamento.

**Implicação:** O recall alto (40,2% Alto, 73,0% Alto+Médio) pode estar inflado por data leakage temporal. O poder discriminativo real do modelo só poderá ser validado com dados prospectivos (acompanhamento de contratos Active ao longo do tempo).

### 7.3 Δt6 — Telemetria company-level, não contract-level

A telemetria disponível está agregada por empresa (não por contrato individual). Portanto, o `dt6` mede quando a **empresa** atingiu `feature_depth ≥ 0.40` após um Closed Won, não quando aquele contrato específico ativou a feature.

**Implicação:** Para empresas com múltiplos contratos, o dt6 baixo (mediana de 4 dias) pode refletir adoção pré-existente de contratos anteriores, não onboarding do novo contrato. O finding permanece válido como sinal de engajamento de empresa, mas não de onboarding de contrato individual.

### 7.4 ICP Health — sem threshold absoluto satisfeito

Nenhuma das 20 empresas atinge simultaneamente `health_score ≥ 75` e `avg_feature_depth ≥ 0.60` (threshold absoluto do plano).

**Valores máximos observados:**
- `health_score` máximo (nível empresa): 65,5
- `avg_feature_depth` máximo (nível empresa): 0,82

As duas metas não são atingidas simultaneamente por nenhuma empresa. Fallback para threshold relativo (p70 de cada métrica) identificou 2 empresas como comparativamente mais saudáveis: **bluepath** e **mindbox**.

**Interpretação:** Este finding confirma a hipótese central do case — a RaptorSoft entrega Value (fecha deals) mas não verifica Impact (saúde + adoção). O perfil de clientes "saudáveis" existe apenas em termos relativos dentro de uma base com saúde baixa.

### 7.5 Canonical Segment — todas Mid-Market

Todas as 20 empresas foram classificadas como Mid-Market pela estratégia employees-first. Isso não é um erro — reflete o real perfil da base de clientes (medianas de employees entre 194 e 495). A ausência de SMB e Enterprise na base ativa limita a utilidade de métricas segmentadas (win rate por segmento, ACV por segmento retornam uma única linha).

### 7.6 Pipeline 100% em Slippage

Todas as 857 oportunidades abertas têm `close_date` no passado. Nenhuma oportunidade tem close_date futuro. Isso pode indicar que o dataset foi extraído com data de referência defasada ou que o processo de atualização de close_date no SF é inconsistente.

---

## 8. Decisões de Normalização — Tabelas de Mapeamento

### Stage (SF)

| Valor bruto | Stage canônico |
|-------------|---------------|
| prospecting | Prospecting |
| qualification, discovery | Discovery/Qualification |
| proposal, demo | Proposal |
| negotiation, contract sent | Negotiation |
| closed won | Closed Won |
| closed lost | Closed Lost |

### Forecast Category (SF)

| Valor bruto | Categoria canônica |
|-------------|-------------------|
| pipeline, in pipeline | pipeline |
| best case, best_case | best_case |
| commit, committed | commit |
| omit, omitted | omit |

### Lead Source (HubSpot)

| Variantes brutas | Fonte canônica |
|-----------------|---------------|
| organic search, organic | Organic Search |
| paid search, ppc, google ads | Paid Search |
| partner, referral, partner/referral | Partner/Referral |
| email, email marketing, nurture | Email Marketing |
| social, social media, linkedin | Social Media |
| content, content download | Content Download |
| webinar, event | Webinar |
| *demais* | Other |

---

## 9. Outputs do Pipeline

| Arquivo | Conteúdo | Linhas | Grão |
|---------|----------|--------|------|
| `sf_clean.csv` | Oportunidades normalizadas + flags + segment | 1.200 | oportunidade |
| `hs_clean.csv` | Leads normalizados + canonical_name | 3.400 | contato |
| `catalyst_clean.csv` | Contratos normalizados + campos derivados | 680 | contrato |
| `telemetry_clean.csv` | Telemetria normalizada + canonical_name | 12.000 | empresa × semana |
| `master_accounts.csv` | 20 empresas + canonical_segment + segment_source | 20 | empresa |
| `catalyst_scored.csv` | Catalyst + churn_risk_score + risk_level | 680 | contrato |
| `company_engagement.csv` | Engagement modifier por empresa | 20 | empresa |
| `sf_forecast.csv` | Oportunidades elegíveis + forecast_contribution | 642 | oportunidade |
| `cr4_trendline.csv` | Win rate por quarter × segmento | 7 | quarter × segmento |
| `cr5_trendline.csv` | ACV médio por quarter × segmento | 7 | quarter × segmento |
| `dt6_analysis.csv` | Δt6 por oportunidade Closed Won | 173 | oportunidade |
| `icp_company_profile.csv` | Perfil de saúde por empresa | 20 | empresa |
| `icp_lead_source_comparison.csv` | Comparativo lead source saudáveis vs. todos | 8 | canal |
| `grr_by_company.csv` | GRR por empresa | 20 | empresa |

---

## 10. Rastreabilidade das Correções v1 → v4

| # | Correção | Versão | Impacto |
|---|----------|--------|---------|
| 1 | Empresas canônicas: 46 → 20 | v2 | Entity resolution, cobertura de telemetria |
| 2 | IDs fantasma HS↔SF: 421 → 0 | v2 | Remove narrativa de degradação de integração |
| 3 | Cobertura Catalyst↔SF: 3% → 53,5% | v2 | Integridade de join muito superior |
| 4 | Contagem de slippage: 874 → 857 | v2 | Cálculo corrigido (1200 - 173 CW - 170 CL = 857) |
| 5 | Amounts negativos: escopo ampliado a R$ -2,98M | v2 | Justifica visualização dedicada |
| 6 | Cobertura de telemetria: 43% → 100% | v2 | 20 domínios = 20 empresas = 100% |
| 7 | Segment: opportunity-level → account-level via employees | v3/v4 | Canonical_segment confiável |
| 8 | Churn model: aditivo → 2 camadas multiplicativas | v3 | Contract_Risk × Engagement_Modifier |
| 9 | Slippage discount: fixo 40% → progressivo 5 tiers | v3 | Forecast mais conservador e justificado |
| 10 | MRR imputado: flag visual obrigatório | v3 | Transparência no dashboard |
| 11 | Seção D: todas as métricas → 4 focadas (CR4, Δt6, ICP, CR5) | v3/v4 | Foco em alto impacto |
| 12 | WBD como narrativa unificadora, não seção isolada | v3 | Framework permeia todos os entregáveis |
| 13 | Catalyst: distribuição desigual → 34/empresa (uniforme) | v4 | Artefato sintético documentado |
| 14 | Canonical segment: mode-first → employees-first | v4 | Remove viés estrutural do SF |
| 15 | GRR (CR7) adicionado como métrica explícita | v4 | Visibilidade do lado direito do Bowtie |
| 16 | Backtesting de churn: recall sobre 189 churned | v4 | Evidência de poder discriminativo |
| 17 | CR5 (ACV trendline) adicionado à Seção D | v4 | 4ª métrica WBD de alto impacto |

---

*Gerado com Claude Code (Anthropic) | claude-opus-4-6 | Data de referência: 2025-04-15*
