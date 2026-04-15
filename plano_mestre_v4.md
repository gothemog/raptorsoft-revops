# Plano Mestre de Execução — RaptorSoft RevOps Case (v4)
## Pipeline Health Intelligence & Churn Risk Signal
**Versão 4.0 — Documento para Handoff ao Claude Code**

> **Changelog v3 → v4:** Seis melhorias aplicadas com base em auditoria cruzada independente dos dados brutos (CSVs) contra premissas do plano v3, conduzida pelo Opus 4.6 em sessão de revisão dedicada. (1) Correção factual: distribuição de contratos no Catalyst é uniforme (34/empresa), não desigual. (2) Estratégia de canonical_segment reformulada: prioriza `number_of_employees` sobre mode quando divergem. (3) GRR (CR7) adicionado como métrica explícita na Seção B. (4) CR5 (ACV trendline) adicionado como 4ª métrica na Seção D. (5) Backtesting do modelo de churn com recall sobre contratos churned reais, thresholds ajustáveis no simulador. (6) Registro completo de todas as correções v1→v2→v3→v4 para rastreabilidade.

---

## ÍNDICE

1. [Contexto e Problema de Negócio](#1-contexto-e-problema-de-negócio)
2. [Inventário dos Datasets](#2-inventário-dos-datasets)
3. [Diagnóstico de Qualidade de Dados](#3-diagnóstico-de-qualidade-de-dados)
4. [Descoberta Estrutural Crítica — Grain do Catalyst](#4-descoberta-estrutural-crítica--grain-do-catalyst)
5. [Descoberta Estrutural #2 — Segmento é Opportunity-Level](#5-descoberta-estrutural-2--segmento-é-opportunity-level)
6. [Mapa de Integridade de Joins](#6-mapa-de-integridade-de-joins)
7. [Premissas de Tratamento de Dados](#7-premissas-de-tratamento-de-dados)
8. [Modelo de Churn Risk Scoring — Arquitetura em Duas Camadas](#8-modelo-de-churn-risk-scoring--arquitetura-em-duas-camadas)
9. [Modelo de Forecast Corrigido — Desconto Progressivo por Aging](#9-modelo-de-forecast-corrigido--desconto-progressivo-por-aging)
10. [Análise WBD — Bowtie como Framework Unificador](#10-análise-wbd--bowtie-como-framework-unificador)
11. [Arquitetura do Dashboard](#11-arquitetura-do-dashboard)
12. [Proposta de Governança Contínua](#12-proposta-de-governança-contínua)
13. [Plano de Execução por Fases](#13-plano-de-execução-por-fases)
14. [Apêndice A — Registro de Correções v1 → v2](#apêndice-a--registro-de-correções-v1--v2)
15. [Apêndice B — Registro de Correções v2 → v3](#apêndice-b--registro-de-correções-v2--v3)
16. [Apêndice C — Registro de Correções v3 → v4](#apêndice-c--registro-de-correções-v3--v4)

---

## 1. Contexto e Problema de Negócio

### A empresa
**RaptorSoft** — SaaS B2B de médio porte. Plataforma de automação de processos para times de operações. Modelo de receita: subscription (MRR) com expansão via upsell de módulos. Segmentos: SMB, Mid-Market, Enterprise.

### GTM (mapeado para o Bowtie Standard WBD)
- **Marketing** → leads inbound via HubSpot → MQLs para Pré-Vendas (Stages 1-2 WBD: Awareness → Education)
- **Vendas** → SF: SQL até Closed Won. Ciclos: 45d (SMB), 90d (Mid-Market), 150d (Enterprise) (Stages 3-4 WBD: Selection → Mutual Commit)
- **CS** → Catalyst: onboarding e monitoramento pós-fechamento. KPI principal: Health Score (0–100) (Stages 5-6 WBD: Onboarding → Adoption/Retention)
- **Produto** → Telemetria interna: uso real da plataforma por semana/conta (sinais de Adoption e Expansion — Stages 6-7 WBD)

**Nota v4 sobre ciclos de venda:** Os ciclos declarados no case (45d/90d/150d) **não são observáveis nos dados**. As medianas reais dos Closed Won são: SMB=104d, MM=128d, Enterprise=94d. Enterprise mais rápido que SMB é sinal inequívoco de que o `segment` original é aleatório a nível de oportunidade (ver Seção 5). Os ciclos reais só serão úteis **após** a atribuição de `canonical_segment`. Este é um finding importante para o Data Treatment Report.

### Os dois sintomas que o RevOps precisa resolver

**Sintoma 1 — Forecast overestimated em 23%**
Nos últimos 3 quarters, o delta entre o forecast comprometido na semana 8 e o resultado real foi em média -23%. A empresa consistentemente sobrestimou receita.

**Sintoma 2 — Churn nos primeiros 60 dias pós-renovação**
CS reporta clientes com contratos recém-renovados churning logo após a renovação. O processo de renovação não captura sinais de risco com antecedência.

### Hipótese central
Os dois sintomas compartilham a mesma causa-raiz: **os dados de pipeline (SF), marketing (HubSpot), saúde de cliente (Catalyst) e uso do produto (Telemetria) nunca foram integrados de forma confiável.** Sem visibilidade cross-source, o forecast é baseado em estágio e categoria de forecast declaradas pelo rep — sem correlação com sinais de engajamento real do cliente.

**Leitura WBD — O frame que unifica os dois sintomas:**

Na linguagem do Bowtie Standard, a RaptorSoft vende **Value** (lado esquerdo do Bowtie — Acquisition) mas não mede **Impact** (lado direito — Retention & Expansion). O Bowtie Standard estabelece como First Principle que *recurring revenue é resultado de recurring impact*. Quando o lado esquerdo promete Value mas o lado direito não verifica se o Impact está sendo entregue, o resultado é exatamente o que os dados mostram:

- **Forecast impreciso** (Constraint 4 WBD — operators run the business in the wrong way): sem sinais de Impact (telemetria, health score) alimentando o forecast, a projeção é baseada apenas na declaração do rep.
- **Churn invisível** (Constraint 1 WBD — o funil para onde a receita recorrente começa): sem closed loop entre performance pós-venda e qualificação do pipeline, clientes que não atingem Impact renovam por inércia e churnam logo após.

**Este frame WBD não é uma seção do dashboard — é a lógica que unifica todos os entregáveis.**

### Entregáveis do case
| # | Entregável | Critério |
|---|-----------|----------|
| 1 | **Data Treatment Report** | Critério de desempate — o mais importante |
| 2 | **Dashboard Interativo** (Seções A, B, C + Seção D Bowtie View) | 5 pts |
| 3 | **Proposta de Governança Contínua** | 5 pts |
| ★ | Agente de IA em linguagem natural (opcional) | Diferenciador |

---

## 2. Inventário dos Datasets

| Dataset | Fonte | Linhas | Colunas | Grain real |
|---------|-------|--------|---------|------------|
| `pipeline_sf.csv` | Salesforce | 1.200 | 12 | 1 linha = 1 oportunidade de venda |
| `leads_hs.csv` | HubSpot | 3.400 | 10 | 1 linha = 1 contato/lead |
| `health_catalyst.csv` | Catalyst CS | 680 | 13 | 1 linha = **1 contrato** (não 1 empresa — ver Seção 4) |
| `telemetry_product.csv` | Data Warehouse | 12.000 | 11 | 1 linha = 1 conta × 1 semana |

### Universo de empresas

**São 20 empresas canônicas** (não 46 — ver Apêndice A, Correção #1).

O número 46 aparecia na v1 porque a normalização aplicada (apenas `lower() + strip()`) era insuficiente — colapsava 81 nomes brutos em 46 variantes, mas não resolvia pares como "BluePath"/"Blue Path", "Acme"/"Acme Corp"/"Acme Corp.", "Amplia"/"Amplia Soluções"/"Amplia Soluções Ltda". A entity resolution completa (remoção de sufixos, unificação de espaçamento, remoção de espaços entre palavras compostas, normalização de acentos) converge para 20 empresas.

**Algoritmo de normalização validado:**
```python
import re, unicodedata

def normalize_canonical(name):
    n = str(name).lower().strip()
    # Remove sufixos empresariais
    for suffix in ['ltda', 'ltd', 'corp', 'corp.', 'tech', 'soluções',
                   'solucoes', 'sistemas', 'digital', 'group', 'grupo']:
        n = re.sub(r'\b' + suffix + r'\b', '', n).strip()
    # Remove acentos
    n = unicodedata.normalize('NFD', n)
    n = ''.join(c for c in n if unicodedata.category(c) != 'Mn')
    # Remove espaços, pontos e hifens (colapsa "Blue Path" com "BluePath")
    n = re.sub(r'[\s.\-]+', '', n).strip()
    return n
```

As 20 empresas: acme, amplia, betasolutions, bluepath, connectwave, corelogic, dataprime, fluxo, gridsoft, innova, mindbox, nexus, orbit, polare, polaris, rushflow, skyops, tapflow, vertice, zentra.

### Chaves de integração disponíveis

| De | Para | Chave | Cobertura real |
|----|------|-------|-----------|
| HubSpot | SF | `sf_opportunity_id` ↔ `opportunity_id` | 1.170 records → 749 opps únicas (100% match) |
| HubSpot | SF | `hs_contact_id` ↔ `hubspot_contact_id` | 468 matches únicos |
| SF | Catalyst | `account_id` ↔ `sf_account_id` | 364/680 records (53,5%) via chave; 20 IDs únicos, todos com match |
| SF/Catalyst | Catalyst (restante) | `canonical_name` (entity resolution) | 316/680 records (46,5%) via nome |
| Catalyst | Telemetria | `customer_id` ↔ `customer_id_fk` | 20 customer_ids únicos (1 por empresa) |
| Telemetria | Todas | `account_domain` → `canonical_name` | 20 domínios → 20 empresas (cobertura 100%) |

**Nota crítica sobre HubSpot↔SF:** Não existem IDs fantasma. Os 1.170 records do HS apontam para 749 opp_ids únicos — todos existem no SF. A diferença de 421 (1.170 - 749) são múltiplos leads associados à mesma oportunidade, não erros de integração.

---

## 3. Diagnóstico de Qualidade de Dados

### 3.1 Pipeline Salesforce (`pipeline_sf.csv`)

#### [SF-01] Normalização de `stage` — 14 variantes para 6 valores canônicos
```
Raw → Canônico:
'Prospecting', 'prospecting'          → Prospecting
'Qualification', 'qualification',
'Discovery', 'DISCOVERY'              → Discovery/Qualification  ← equivalentes na WBD (Stage 2-3)
'Proposal', 'Prop'                    → Proposal
'Negotiation', 'Negotiation '         → Negotiation
'Closed Won', 'closed-won'            → Closed Won
'Closed Lost', 'Closed_Lost'          → Closed Lost
```
**Decisão de premissa:** `Qualification` e `Discovery` (incluindo variantes de casing) são tratados como **equivalentes funcionais** na metodologia Winning by Design — ambos cobrem a mesma transição de Education para Selection (CR2→CR3 no Bowtie). Serão colapsados em `Discovery/Qualification`.

**Contagens após normalização:**
- Closed Won: 150 (standard) + 23 (closed-won) = **173**
- Closed Lost: 153 (standard) + 17 (Closed_Lost) = **170**
- Total fechados: **343** | Total abertos: **857**

#### [SF-02] Normalização de `forecast_category` — 8 variantes para 4 valores
```
'Pipeline', 'pipeline'       → pipeline    (254 + 49 = 303)
'Best Case', 'best case'     → best_case   (245 + 69 = 314)
'Commit', 'commit'           → commit      (237 + 61 = 298)
'Omit', 'omit'               → omit        (231 + 54 = 285)
```

#### [SF-03] 857 oportunidades abertas com `close_date` no passado — sinal primário do problema de forecast

Das 1.200 oportunidades, 857 estão em stage aberto e 343 em stages fechados. **Todas as 857 abertas têm close_date anterior a hoje (2025-04-15)** — 100% do pipeline "ativo" está em slippage. Isso confirma a hipótese do delta de forecast de -23%: oportunidades são contadas como receita comprometida mas nunca fecham.

**Tratamento:** Flag `is_slippage = True`. Não excluir — é dado analítico fundamental. O modelo de forecast aplica desconto progressivo por aging (ver Seção 9).

#### [SF-04] `days_in_stage` com valores inválidos — dois subgrupos distintos

**Total: 25 registros com `days_in_stage < 0`.**

**Grupo A — 21 registros com `days_in_stage < 0` e `implied_days` válido:**
`implied_days = close_date - created_date` resulta em valor positivo. O erro está no campo `days_in_stage`, não nas datas.
→ **Tratamento:** Substituir por `implied_days`. Flag `days_recalculated = True`.

**Grupo B — 4 registros com `days_in_stage < 0` E `close_date < created_date`:**
Datas corrompidas na fonte.
→ **Tratamento:** `days_in_stage = NULL`, `implied_days = NULL`. Flag `data_quality = invalid_dates`. Excluídos de análises de aging e velocity.

**Registros com `days_in_stage > 365` (20 registros):** Mantidos com flag `is_stale_opportunity = True`.

#### [SF-05] 30 registros com `amount_brl < 0` — estornos distribuídos em 15+ empresas

Valor total negativo: **R$ -2.975.934,71**. Afeta empresas de todos os segmentos.

**Tratamento:**
- Flag `is_negative_amount = True`
- **Excluídos de forecast** (não são receita projetável)
- **Mantidos com visualização dedicada no dashboard** — volume e tendência de estornos são sinais operacionais relevantes
- Hipótese: estornos, créditos ou ajustes contábeis registrados como oportunidades

#### [SF-06] Cobertura de chaves de integração
- `account_id`: 90/1.200 nulos (7,5%) — nas 1.110 preenchidas, 20 valores únicos mapeiam para as 20 empresas
- `hubspot_contact_id`: 469/1.200 nulos (39,1%) — nos 731 preenchidos, 468 fazem match com HS
- `owner_email`: 66/1.200 nulos (5,5%)

#### [SF-07] Account name com inconsistência — 81 variantes para 20 empresas
O mesmo account aparece com múltiplas grafias (ex: "Zentra Tech", "ZENTRA", "zentra", "Zentra"). Com 20 empresas canônicas, tratável via tabela mestre manual com mapeamento explícito.

---

### 3.2 Leads HubSpot (`leads_hs.csv`)

#### [HS-01] `country` — 6 variantes para "Brasil", campo sem valor analítico
```
'Brasil' (2393), 'Brazil' (213), 'brasil' (208), 'BRA' (201), 'BR' (196), 'BRASIL' (189)
```
100% dos registros são do Brasil. Colapsar para `BR`. Campo descartado das análises.

#### [HS-02] `lead_source` — 18 variantes para 7 grupos canônicos
```
Organic Search / organic / SEO / Organic    → Organic Search   (254+116+107+111 = 588)
Paid Search / paid / google ads             → Paid Search      (260+117+108 = 485)
Social Media / social                       → Social Media     (259+112 = 371)
Content Download                            → Content Download (277)
Webinar                                     → Webinar          (243)
Partner / partner referral / Referral       → Partner/Referral (255+111+252 = 618)
Email Marketing / email                     → Email Marketing  (276+95 = 371)
Direct / Event                              → Other            (224+223 = 447)
```

#### [HS-03] `mql_date` nulo em 948/3.400 (27,9%)
Leads sem data de MQL excluídos de análises de velocidade de funil (Δt1 e Δt2 no Bowtie). Mantidos para análises de conversion rate.

#### [HS-04] `sf_opportunity_id` — 1.170 preenchidos, 749 opp_ids únicos, 100% com match no SF

Dos 3.400 leads, 2.230 não têm sf_opportunity_id (esperado — maioria dos leads não converte). Dos 1.170 com ID preenchido, **todos fazem match** com `opportunity_id` no SF. São 749 opp_ids únicos, o que significa que 312 oportunidades têm mais de 1 lead associado.

**Não existem IDs fantasma.**

#### [HS-05] `number_of_employees` nulo em 1.172/3.400 (34,5%), preenchido em 2.228

**Distribuição dos preenchidos (v4 — validada contra dados reais):**
```
≤ 100 funcionários (proxy SMB):        485 (21,8%)
101-500 funcionários (proxy MM):        816 (36,6%)
> 500 funcionários (proxy Enterprise):  927 (41,6%)
```

Stats: min=10, mediana=327.5, média=1225.5, max=10.000.

**NOVO v4:** Este campo é a **fonte primária** para atribuição de `canonical_segment` quando disponível (ver Seção 5). A distribuição mostra perfil mais Enterprise/MM do que o SF sugere — evidência adicional de que o `segment` no SF é atribuído aleatoriamente.

#### [HS-06] `lifecycle_stage` — distribuição suspeitamente uniforme
```
MQL: 693 | Customer: 683 | Lead: 676 | Subscriber: 675 | SQL: 673
```
Distribuição suspeitamente uniforme — provável artefato de dados sintéticos. Mantida como está.

#### [HS-07] Estratégia de join HS ↔ SF — dois caminhos complementares
- **Caminho A:** `sf_opportunity_id` ↔ `opportunity_id` → 749 opps únicas (preferencial)
- **Caminho B:** `hs_contact_id` ↔ `hubspot_contact_id` → 468 matches (complementar)
- **União dos dois caminhos** para maximizar cobertura

---

### 3.3 Health Catalyst (`health_catalyst.csv`)

#### [CA-01] `sf_account_id` — cobertura real de 53,5% via chave técnica

364 dos 680 registros possuem `sf_account_id` preenchido (53,5%). São 20 valores únicos de account_id, todos com match perfeito no SF. Os outros 316 registros (46,5%) não têm chave técnica e dependem de entity resolution por nome canônico.

#### [CA-02] `health_score` nulo em 134/680 (19,7%) e `health_trend` nulo em 145/680 (21,3%)

Campos centrais do modelo de churn risk. Contratos sem health_score usam denominador dinâmico (ver Seção 8).

**Stats para contratos Active (532 registros):**
- health_score nulo: 107 (20,1%)
- health_score preenchido: 425 → min=10, Q1=38, mediana=59, Q3=79, max=100, mean=57.5

#### [CA-03] `nps_score` nulo em 326/680 (47,9%)
Tratado como **bonus signal**, nunca como penalizador. Ausência = impacto zero no score.

#### [CA-04] `last_qbr_date` nulo em 287/680 (42,2%)
Ausência de QBR registrado é **sinal de risco** — não apenas dado faltante. Penalização no componente de engajamento de CS.

#### [CA-05] 20 contratos `Active` com `mrr_usd = 0` — falha de migração

Contratos de empresas com Closed Won no SF e amount_brl > 0. Não são trials.

**Tratamento:**
- Flag `mrr_source = missing_integration`
- **Incluídos** no modelo de churn (clientes reais com contrato ativo)
- MRR imputado pela **mediana global** dos contratos ativos com MRR > 0: **USD 8.319,83** (validado v4: mediana calculada sobre 512 contratos Active com MRR > 0)
- Flags: `mrr_imputed = True` + `mrr_source = global_median_proxy`
- **No dashboard:** MRR imputado aparece com destaque visual (cor diferente, asterisco) para que o gestor saiba que ~USD 166.397 (8.319,83 × 20) do MRR total é estimado, não real.

**Justificativa da mediana global vs. mediana por segmento:** O Catalyst não possui coluna `segment`. Seria necessário join via entity resolution + SF para obter segmento. Além disso, o campo `segment` no SF é atribuído por oportunidade, não por conta (ver Seção 5), o que torna a segmentação por essa via pouco confiável. A mediana global é a premissa mais conservadora e transparente.

#### [CA-06] 41 contratos `Active` com `churn_date` preenchida — contradição lógica
`churn_date` é mais específica e confiável que o campo `status`. Reclassificar como `Churned`. Flag `status_corrected = True`.

**Impacto na base:** Após reclassificação → **491 Active** (vs. 532 original) e **189 Churned** (vs. 148 original).

**Nota v4:** Dos 20 contratos com MRR=0, apenas 1 tem churn_date preenchida. Os outros 19 permanecem Active e recebem MRR imputado.

#### [CA-07] `csm_owner` nulo em 81/680 (11,9%)
No dashboard, aparecem como "CSM: Não Atribuído".

---

### 3.4 Telemetria (`telemetry_product.csv`)

#### [TE-01] 20 domínios únicos = 20 empresas = cobertura de 100%

A telemetria cobre **todas as 20 empresas canônicas** via `account_domain`. O campo `customer_id_fk` está preenchido para 20 customer_ids (1 por empresa), representando 7.368 dos 12.000 registros (61,4%). Os outros 4.632 registros (38,6%) têm `customer_id_fk` nulo mas podem ser vinculados via `account_domain` → `canonical_name`.

**Range temporal:** 2024-01-06 a 2025-01-25 (55 semanas).

#### [TE-02] `feature_depth_score` acima de 1,0 em 693 registros (máx 1,349)
Cap aplicado em 1,0. Flag `feature_depth_capped = True`.

#### [TE-03] `plan_tier` — 10 variantes para 4 tiers canônicos
```
'Starter', 'starter'                     → Starter     (2296+519 = 2815)
'Growth', 'growth', 'Growth Plan', 'GROWTH' → Growth   (2280+504+474+485 = 3743)
'Business', 'BUSINESS'                   → Business    (2204+486 = 2690)
'Enterprise', 'enterprise plan'          → Enterprise  (2254+498 = 2752)
```

#### [TE-04] 215 registros com `active_users = 0` e 201 com `sessions_total = 0`
Mantidos como dados válidos. Semanas sem acesso são sinal real de abandono.

---

## 4. Descoberta Estrutural Crítica — Grain do Catalyst

### O Catalyst é contract-level, não account-level

**Evidências:**
- 680 customer_ids únicos para 20 empresas canônicas = **exatamente 34 contratos por empresa** (distribuição perfeitamente uniforme — ver nota v4 abaixo)
- Uma mesma empresa pode ter contratos com MRR, health_score e renewal_date completamente diferentes
- Contratos `Churned` têm `mrr_usd = 0` sistematicamente
- O mesmo `sf_account_id` é compartilhado entre múltiplos `customer_id`

**CORREÇÃO v4 — Distribuição uniforme de contratos:**

A v3 reportava distribuição desigual: "A maioria das empresas tem 34 contratos, com 4 exceções: Beta Solutions (28), Amplia (27), Fluxo (21), Nexus (20)." **Isso estava incorreto.**

Auditoria independente dos dados reais mostra que **todas as 20 empresas têm exatamente 34 contratos cada** após entity resolution correta. O erro na v3 foi causado pela contagem usando `account_name` bruto do Catalyst (que tem 81 variantes) em vez dos nomes canônicos normalizados. Quando se aplica a mesma normalização da Seção 2, empresas como "Fluxo", "Fluxo Sistemas", "Fluxo Sistemas Ltda" e "FLUXO SISTEMAS" convergem para os mesmos 34 contratos.

**Implicação prática:** A distribuição uniforme é um artefato de dados sintéticos que *simplifica* a modelagem — não é necessário normalizar métricas por volume de contratos ao comparar empresas. Churn rate, MRR total e health score médio são diretamente comparáveis entre empresas sem ajuste por tamanho de portfólio.

### Implicações para o modelo

**MRR de uma empresa = SOMA dos contratos `Active` com MRR > 0** (excluindo churned; contratos Active com MRR=0 recebem imputação)

**Churn rate de uma empresa** = `contratos_churned / total_contratos_históricos` (34 para todas as empresas)

**Health score de uma empresa** — não há valor único. Agregação:
- `health_score_empresa = média ponderada pelo MRR dos contratos ativos`
- Ou: mínimo do health score entre contratos ativos (postura conservadora para risco)

**Renewal timeline** — múltiplos contratos podem ter renewal_dates diferentes. O modelo identifica **o próximo contrato a vencer** como gatilho de urgência.

---

## 5. Descoberta Estrutural #2 — Segmento é Opportunity-Level

### O problema

**Todas as 20 empresas têm oportunidades distribuídas em todos os 3 segmentos.** Exemplos (confirmados v4):

| Empresa | SMB | Mid-Market | Enterprise |
|---------|-----|------------|-----------|
| Mindbox | 44 | 30 | 8 |
| Zentra | 27 | 32 | 13 |
| Acme | 23 | 27 | 12 |
| Beta Solutions | 34 | 29 | 12 |
| SkyOps | 35 | 21 | 9 |

**Distribuição global:** SMB=559, Mid-Market=450, Enterprise=191. Forte viés SMB.

Em uma operação real, uma empresa seria classificada em **um** segmento. A co-existência de opps nos 3 segmentos para a mesma conta indica que `segment` foi atribuído a nível de oportunidade (provavelmente aleatoriamente nos dados sintéticos), não a nível de conta.

### Impacto se não tratado

- **Win rates por segmento** artificialmente similares: SMB=49,1%, MM=52,7%, Ent=49,0% (count-based, confirmados v4)
- **Ciclos de venda** não parametrizáveis: medianas reais são SMB=104d, MM=128d, Ent=94d — Enterprise mais rápido que SMB é impossível em operação real
- **Análise de MRR por segmento** no Catalyst impossível
- **Forecast por segmento** essencialmente ruído
- Qualquer análise WBD que segmente (CR4 trendline, CR5 por segmento) perderia significado

### Estratégia de resolução v4: Employees-First + Mode Fallback

**MUDANÇA v4:** A v3 priorizava o mode (segmento mais frequente por empresa) com validação secundária por `number_of_employees`. A v4 **inverte a prioridade**: `number_of_employees` é a fonte primária e o mode é o fallback.

**Justificativa:** O mode é contaminado pela mecânica de atribuição aleatória. Como a distribuição global é 47% SMB / 37% MM / 16% Enterprise, o mode tem viés estrutural para SMB. Já o `number_of_employees` do HubSpot é um atributo da empresa (não da oportunidade), preenchido em 65,5% dos leads, e segue uma distribuição que faz sentido empresarial.

**Passo 1 — Coletar `number_of_employees` por empresa via HubSpot:**

```python
# Para cada canonical_name, pegar a mediana do number_of_employees
# dos leads associados a essa empresa (via company_name normalizado)
employees_by_company = (
    hs[hs['number_of_employees'].notna()]
    .assign(canonical=hs['company_name'].apply(normalize_canonical))
    .groupby('canonical')['number_of_employees']
    .median()
)
```

**Passo 2 — Atribuição primária via employees:**
```
mediana_employees ≤ 100    → SMB
100 < mediana_employees ≤ 500 → Mid-Market
mediana_employees > 500    → Enterprise
```

**Passo 3 — Fallback via mode (quando employees não disponível):**

Para empresas sem nenhum lead com `number_of_employees` preenchido:
```python
canonical_segment = (
    sf.groupby('canonical_name')['segment']
    .agg(lambda x: x.mode().iloc[0])
)
```

**Passo 4 — Confidence scoring:**
```
employees disponível + mode concorda    → segment_confidence = HIGH
employees disponível + mode diverge     → segment_confidence = HIGH (employees prevalece)
employees indisponível, apenas mode     → segment_confidence = MEDIUM
```

**Passo 5 — Propagação consistente:**
O `canonical_segment` é atribuído à **empresa** na tabela mestre e propagado para:
- Todas as oportunidades dessa empresa (substituindo o `segment` original)
- Todos os contratos do Catalyst via join
- Todas as análises de forecast, churn, WBD

### Flag de transparência
- `segment_source = employees_median` ou `segment_source = mode_fallback`
- `segment_original` — mantido como campo auxiliar para auditoria
- `segment_confidence = HIGH | MEDIUM`

---

## 6. Mapa de Integridade de Joins

### Estratégia de integração entre as 4 fontes

```
HubSpot ─── 749 opps únicas (via opp_id, 100% match) ──→ SF Pipeline
             468 contacts (via contact_id, complementar)

SF Pipeline ─── sf_account_id (53,5% dos records) ────────→ Health Catalyst
            ─── entity resolution por canonical_name ──────→ Health Catalyst (46,5% restante)
                 (20/20 empresas com match perfeito)

Health Catalyst ─── customer_id direto ────────────────────→ Telemetria (20 customer_ids)
                ─── account_domain via canonical_name ─────→ Telemetria (cobertura 100%)
```

### Tabela de confiabilidade de join

| Nível | Critério | Uso |
|-------|----------|-----|
| `HIGH` | Match por chave técnica (`account_id`, `customer_id`, `sf_opportunity_id`) | Todas as análises |
| `MEDIUM` | Match por `canonical_name` sem chave técnica | Análises cross-source com nota |
| `LOW` | Sem match por nenhum critério | Não esperado — todas as 20 empresas são compartilhadas |

---

## 7. Premissas de Tratamento de Dados

### Bloco A — Entity Resolution (Tabela Mestre de Contas)

**P-A1:** Tabela mestre construída com `canonical_name` para cada uma das 20 empresas, mapeando todas as 81 variantes de nome + 20 domínios de telemetria + **canonical_segment** (via employees-first + mode fallback).

**Mapeamento completo:**
```
canonical_name  | variantes_nome (SF+Catalyst)                                  | domain                  | canonical_segment
acme            | Acme, acme, Acme Corp, ACME CORP, Acme Corp.                 | acmecorp.com.br         | [calculado]
amplia          | Amplia, AMPLIA SOLUCOES, Amplia Soluções, Amplia Soluções Ltda| ampliasolucoes.com.br   | [calculado]
betasolutions   | Beta Solutions, beta solutions, BETA SOLUTIONS, Beta Solutions Ltda | betasolutions.com.br | [calculado]
bluepath        | BluePath, bluepath, BLUEPATH, Blue Path                       | bluepath.com.br         | [calculado]
connectwave     | ConnectWave, connectwave, CONNECTWAVE, Connect Wave           | connectwave.com.br      | [calculado]
corelogic       | CoreLogic, corelogic, CORELOGIC, Core Logic                  | corelogic.com.br        | [calculado]
dataprime       | DataPrime, dataprime, DATAPRIME, Data Prime                   | dataprime.com.br        | [calculado]
fluxo           | Fluxo, Fluxo Sistemas, FLUXO SISTEMAS, Fluxo Sistemas Ltda   | fluxosistemas.com.br    | [calculado]
gridsoft         | GridSoft, gridsoft, GRIDSOFT, Grid Soft                       | gridsoft.com.br         | [calculado]
innova          | Innova, innova, INNOVA, Innova Tech                           | innova.com.br           | [calculado]
mindbox         | Mindbox, mindbox, MINDBOX, Mind Box                           | mindbox.com.br          | [calculado]
nexus           | Nexus Group, nexus, GRUPO NEXUS, Grupo Nexus                  | gruponexus.com.br       | [calculado]
orbit           | Orbit, Orbit Digital, ORBIT DIGITAL, orbit digital            | orbitdigital.com.br     | [calculado]
polare          | Polare, polare, POLARE, Polare Ltda                           | polare.com.br           | [calculado]
polaris         | Polaris, polaris, POLARIS, Polaris Tech                        | polaris.com.br          | [calculado]
rushflow        | RushFlow, rushflow, RUSHFLOW, Rush Flow                        | rushflow.com.br         | [calculado]
skyops          | SkyOps, skyops, SKYOPS, Sky Ops                               | skyops.com.br           | [calculado]
tapflow         | TapFlow, tapflow, TAPFLOW, Tap Flow                           | tapflow.com.br          | [calculado]
vertice         | Vértice Tech, VERTICE TECH, Vertice Tech, Vertice             | verticetech.com.br      | [calculado]
zentra          | Zentra, zentra, ZENTRA, Zentra Tech                           | zentra.com.br           | [calculado]
```

**P-A2:** Chave de join primária: `canonical_name`. Chaves técnicas para atribuição de nível de confiança.

**P-A3:** Não existem registros sem match — as 20 empresas são compartilhadas entre todas as fontes.

**P-A4:** Contratos duplicados no Catalyst mantidos individualmente — representam contratos reais distintos (34 por empresa).

### Bloco B — Normalização de Campos

**P-B1 — `stage` (SF):** Mapeamento para 6 valores canônicos. `Qualification`/`Discovery` → `Discovery/Qualification`.

**P-B2 — `forecast_category` (SF):** `.lower().strip()` → 4 valores: `pipeline`, `best_case`, `commit`, `omit`.

**P-B3 — `plan_tier` (Telemetria):** 4 tiers: `Starter`, `Growth`, `Business`, `Enterprise`.

**P-B4 — `lead_source` (HubSpot):** 7 grupos canônicos.

**P-B5 — `country` (HubSpot):** Colapsar para `BR`. Descartado.

**P-B6 — `segment` (SF):** Substituído pelo `canonical_segment` da tabela mestre. Campo original mantido como `segment_original` para auditoria. **v4: Fonte primária = employees; fallback = mode.**

### Bloco C — Anomalias e Contradições

**P-C1 — `days_in_stage` negativos (25 registros):**
- Grupo A (21 registros, implied_days válido): substituir por `implied_days`, flag `days_recalculated`
- Grupo B (4 registros, close_date < created_date): NULL duplo, flag `invalid_dates`, excluídos de aging

**P-C2 — `days_in_stage > 365` (20 registros):** Manter com flag `is_stale_opportunity = True`.

**P-C3 — `amount_brl < 0` (30 registros, R$ -2,98M):** Flag `is_negative_amount = True`. Excluídos de forecast. **Visualização dedicada no dashboard**.

**P-C4 — `feature_depth_score > 1,0` (693 registros):** Cap em 1,0. Flag `feature_depth_capped = True`.

**P-C5 — 41 contratos `Active` com `churn_date`:** Reclassificar como `Churned`. Flag `status_corrected = True`.

**P-C6 — 20 contratos `Active` com `mrr_usd = 0`:** Imputar mediana global USD 8.319,83 (sobre 512 contratos Active com MRR > 0). Flags `mrr_imputed = True` + `mrr_source = global_median_proxy`. **Dashboard flag visual obrigatório**.

**P-C7 — `active_users = 0` e `sessions_total = 0` (telemetria):** Manter como dado válido.

**P-C8 — `segment` inconsistente a nível de conta:** Substituir por `canonical_segment` derivado de employees-first + mode fallback. Ver Seção 5 para estratégia completa.

### Bloco D — Moeda e Conversão

**P-D1:** Análises de forecast em **BRL** (fonte SF: `amount_brl`).

**P-D2:** Análises de MRR/NRR/GRR em **USD** (fonte Catalyst: `mrr_usd`).

**P-D3:** Quando necessário cruzar: taxa fixa de **R$ 5,70/USD**, explicitada como premissa no dashboard.

**P-D4:** Quarter atual assumido como **Q2 2025 (Abril–Junho 2025)** para filtros de `close_date`. Data de referência: **2025-04-15**.

---

## 8. Modelo de Churn Risk Scoring — Arquitetura em Duas Camadas

### Mudança arquitetural v3→v4: Backtesting adicionado

**Arquitetura mantida da v3:** Contract_Risk × Company_Engagement_Modifier (separação por grain).

**NOVO v4:** Após o cálculo do score, o modelo é **validado contra os 189 contratos que efetivamente deram churn** (148 originais + 41 reclassificados). O backtesting mede o recall (quantos churns reais foram capturados como Alto Risco) e informa se os thresholds precisam de ajuste. Os thresholds são configuráveis no Simulador de Cenários (Seção C).

---

### Camada 1 — Contract Risk (dados do Catalyst)

```
Contract_Risk =
  (W1 × health_score_inv)       +   [peso: 40%]
  (W2 × health_trend_score)     +   [peso: 25%]
  (W4 × support_pressure)       +   [peso: 10%]
  (W5 × renewal_proximity)      +   [peso: 25%]
  + BONUS_nps
  + DETRACTOR_no_qbr
```

Quando componentes têm valor NULL, os pesos são redistribuídos proporcionalmente (denominador dinâmico).

#### W1 — `health_score` invertido normalizado (peso: 40%)
```
health_score_inv = (100 - health_score) / 100
```
- Score 10 → risco 0,90 | Score 100 → risco 0,00
- NULL (19,7%): componente não computa; pesos redistribuídos

#### W2 — `health_trend` (peso: 25%)
```
Declining   → 1,00
Stable      → 0,30
Improving   → 0,00
NULL        → 0,50 (neutro)
```

#### W4 — `support_pressure` (peso: 10%)
```
support_pressure = open_tickets / max_open_tickets_no_dataset
```
Normalização por percentil.

#### W5 — `renewal_proximity` (peso: 25%)
```
days_to_renewal = renewal_date - data_hoje (2025-04-15)

Score base:
  ≤ 30 dias  → 1,00
  ≤ 60 dias  → 0,60
  ≤ 90 dias  → 0,30
  > 90 dias  → 0,00
```

#### BONUS_nps (ajuste sobre Contract_Risk)
```
NPS ≤ 6  (detrator)  → + 0,10
NPS 7–8  (passivo)   → ± 0,00
NPS ≥ 9  (promotor)  → - 0,08
NULL                 → ± 0,00  ← nunca penaliza
```

#### DETRACTOR_no_qbr (+0,08)
Aplicado quando `last_qbr_date` é NULL ou `last_qbr_date < hoje - 180 dias`.

#### Cap do Contract_Risk
Após aplicação dos ajustadores: `Contract_Risk = min(1.0, max(0.0, Contract_Risk))`

---

### Camada 2 — Company Engagement Modifier (dados da Telemetria)

Calculado **uma vez por empresa** e aplicado a todos os contratos dessa empresa.

#### Cálculo do engagement da empresa

Usar as **últimas 4 semanas disponíveis** da telemetria para a empresa (via `account_domain` → `canonical_name`):

```python
avg_feature_depth = mean(feature_depth_score) das últimas 4 semanas  # já capped em 1.0
avg_sessions      = mean(sessions_total) das últimas 4 semanas
last_login_recent = (last_login_days_ago <= 14 na semana mais recente)  # boolean
```

#### Fórmula do Modifier

```python
engagement_score = (
    0.50 × avg_feature_depth +
    0.30 × min(1.0, avg_sessions / 100) +
    0.20 × (1.0 if last_login_recent else 0.0)
)

# engagement_score = 0.0 → modifier = 1.40 (amplifica risco em 40%)
# engagement_score = 0.5 → modifier = 1.00 (neutro)
# engagement_score = 1.0 → modifier = 0.70 (reduz risco em 30%)

Company_Engagement_Modifier = 1.40 - (0.70 × engagement_score)
```

**Faixa do modifier:** 0.70 a 1.40

#### DETRACTOR adicional — Low Engagement Absoluto

Quando `avg_feature_depth < 0.20` E `avg_sessions < 10` nas últimas 4 semanas:
```
Company_Engagement_Modifier = max(Company_Engagement_Modifier, 1.30)
```

---

### Score Final

```
Churn_Risk_Score = min(1.0, Contract_Risk × Company_Engagement_Modifier)
```

### Classificação (thresholds iniciais — ajustáveis)

| Faixa do Score | Nível de Risco |
|---------------|----------------|
| 0,65 – 1,00 | 🔴 Alto |
| 0,35 – 0,64 | 🟡 Médio |
| 0,00 – 0,34 | 🟢 Baixo |

### NOVO v4 — Backtesting e Calibração

**Objetivo:** Medir o poder discriminativo do modelo sobre os churns reais.

**Método:**
1. Calcular `Churn_Risk_Score` para **todos os 680 contratos** (incluindo os 189 Churned, usando dados que existiam antes do churn quando possível)
2. Para os 189 contratos Churned: qual % foi classificado como Alto Risco?
3. Reportar **Recall por nível:**
   - Recall Alto = (churned classificados como Alto) / 189
   - Recall Alto+Médio = (churned classificados como Alto ou Médio) / 189

**Critérios de aceitação:**
- **Recall Alto ≥ 40%:** modelo aceitável para produção
- **Recall Alto+Médio ≥ 70%:** modelo captura a maioria dos riscos em pelo menos uma das faixas
- **Se Recall Alto < 40%:** ajustar thresholds (baixar cutoff de Alto de 0,65 para valor que capture ≥40%)

**Nota sobre dados de contratos Churned:** Contratos Churned têm `mrr_usd = 0` sistematicamente e podem ter health_score/health_trend refletindo estado pós-churn, não pré-churn. O backtesting será **indicativo** (não definitivo) porque o snapshot é pós-fato. Isso deve ser documentado no Data Treatment Report como limitação.

**No Simulador de Cenários:** O gestor pode ajustar os thresholds e ver:
- Redistribuição de contratos por nível
- MRR exposto em cada nível
- Recall estimado (se backtesting disponível)

### Transparência do modelo no dashboard

O dashboard deve exibir, para cada contrato com risco Alto ou Médio:
- O **Contract_Risk** (componente individual)
- O **Company_Engagement_Modifier** (componente empresarial)
- Indicação de qual camada está dominando o risco

---

## 9. Modelo de Forecast Corrigido — Desconto Progressivo por Aging

### Contexto

100% do pipeline aberto (857 de 857 oportunidades em stages não-fechados) tem `close_date` no passado. Win rates históricos com `segment` original são artificialmente similares. **Com `canonical_segment` (v4, employees-first), os win rates por segmento devem apresentar maior diferenciação** — a ser confirmado na execução.

### Passo 1 — Win rates calculados (CR4 WBD)
```
win_rate_por_segmento =
  Closed Won (valor) / (Closed Won + Closed Lost) por canonical_segment

Calculados sobre oportunidades fechadas que passaram de Discovery/Qualification.
```

### Passo 2 — Slippage discount progressivo por aging

```
slippage_days = data_hoje (2025-04-15) - close_date

Tabela de desconto:
  slippage_days ≤ 30   → discount = 0.15
  slippage_days ≤ 60   → discount = 0.30
  slippage_days ≤ 90   → discount = 0.45
  slippage_days ≤ 180  → discount = 0.65
  slippage_days > 180  → discount = 0.85

adjusted_win_rate = win_rate_base × (1 - discount)
```

### Passo 3 — Projeção do quarter
```
Para cada oportunidade aberta (excluindo omit e amount_brl < 0):
  forecast_contribution = amount_brl × adjusted_win_rate

Forecast_Total = Σ forecast_contribution
```

### Passo 4 — Override do gestor (Simulador)
No dashboard, o gestor pode:
- Ajustar os tiers de slippage discount
- Ajustar o win rate base por segmento
- Definir a meta de receita do quarter (input livre)
- Ver gap entre forecast e meta em tempo real
- Ver pipeline coverage ratio

### NOVO v4 — Feedback loop telemetria → forecast

**Oportunidade de closed loop:** Oportunidades de empresas com alto engagement (Company_Engagement_Modifier < 1.0) poderiam receber um slippage discount *menor* — a lógica sendo que empresas que já demonstram Impact com o produto têm maior probabilidade de fechar novos deals (expansion). Isso cria um closed loop real entre o lado direito (Impact/telemetria) e o lado esquerdo (forecast) do Bowtie.

**Implementação sugerida (opcional — diferenciador):**
```python
# Para oportunidades de empresas com engagement modifier < 0.90 (uso intenso):
# Reduzir slippage discount em 20%
if company_engagement_modifier < 0.90:
    adjusted_discount = discount * 0.80
```

Este refinamento é **opcional** e deve ser documentado como premissa experimental se implementado.

---

## 10. Análise WBD — Bowtie como Framework Unificador

### Princípio v3/v4: WBD não é uma seção — é a lógica que organiza o dashboard inteiro

- **Seção A (Pipeline Health)** = Lado esquerdo do Bowtie: VM3→VM5, CR4, CR5, Δt4
- **Seção B (Churn Risk)** = Lado direito do Bowtie: CR6, **CR7 (GRR)**, plus o modelo de churn risk
- **Seção C (Simulador)** = Conexão entre os dois lados: forecast (left) + churn impact (right) + intervenção
- **Seção D (Bowtie View)** = **4 métricas** focadas que geram insight acionável (v4: +CR5)

### 10.1 Mapeamento dos datasets para a Data Structure WBD

**Lado Esquerdo — Acquisition:**

| Métrica WBD | Nome | Cálculo a partir dos dados |
|-------------|------|---------------------------|
| VM1 | Prospects | Leads no HS com lifecycle_stage em estágios iniciais |
| VM2 | MQLs | Leads com `mql_date` preenchido |
| VM3 | SQLs/Opportunities | Opps no SF (total criadas no período) |
| VM4 | Qualified Opps | Opps que passaram de Discovery/Qualification |
| VM5 | Wins | Closed Won no SF |
| CR2 | Lead→Opp | VM3 / VM2 (leads que viraram opp). Calculável: 34,5% |
| CR4 | Win Rate | VM5 / VM4 |
| **CR5** | **ACV** | **amount médio dos Closed Won, por canonical_segment e por quarter (NOVO v4)** |
| Δt2 | Lead→Opp time | mql_date → created_date (SF) |
| Δt4 | Sales Cycle | created_date → close_date (Closed Won) |

**Lado Direito — Retention & Expansion:**

| Métrica WBD | Nome | Cálculo a partir dos dados |
|-------------|------|---------------------------|
| VM6 | MRR Committed | Soma de mrr_usd dos contratos no momento do Closed Won |
| VM7 | MRR Start | VM6 - onboarding churn (contratos churned < 90 dias) |
| VM8 | MRR (recurring) | Soma de mrr_usd dos contratos Active |
| CR6 | Onboarding Success | Contratos on-going / total novos contratos |
| **CR7** | **GRR (Gross Revenue Retention)** | **MRR retido / MRR início do período (NOVO v4 — cálculo explícito)** |
| CR8 | Expansion (proxy) | New ARR de deals subseqüentes da mesma empresa |
| Δt6 | Time to First Impact | close_date → primeira semana com feature_depth ≥ 0.40 |

### 10.2 NOVO v4 — Cálculo explícito de GRR (CR7)

**Por que calcular GRR diretamente:**

GRR é a métrica mais importante do lado direito do Bowtie. O plano v3 usava o churn risk score como proxy, mas GRR deveria ser calculado diretamente e exibido na Seção B. É a métrica que os avaliadores certamente esperam ver.

**Fórmula:**
```python
# Definir período (últimos 12 meses ou janela configurável)
# MRR início = soma de mrr_usd dos contratos Active no início do período
# MRR churned = soma de mrr_usd dos contratos que deram churn durante o período
# MRR downgrades = não capturado nos dados (premissa: zero, documentar como limitação)

GRR = (MRR_inicio - MRR_churned) / MRR_inicio
```

**Desafio nos dados:** O Catalyst é um snapshot, não tem série temporal. Não sabemos qual era o MRR exato no início do período — apenas o MRR atual (que é zero para contratos Churned) e as datas de churn.

**Abordagem prática:**
1. Para contratos Churned: usar `mrr_usd` = 0 (dado atual). Precisamos **estimar o MRR pré-churn**.
2. **Estimativa v4:** Usar a mediana global dos contratos Active (USD 8.319,83) como proxy do MRR pré-churn para cada contrato Churned. Flag `grr_mrr_estimated = True`.
3. GRR calculado como:
```python
n_active_start = n_active_current + n_churned_no_periodo
mrr_start_estimated = n_active_start * median_mrr_active
mrr_lost = n_churned_no_periodo * median_mrr_active
GRR = (mrr_start_estimated - mrr_lost) / mrr_start_estimated
# Equivale a: GRR = n_active_current / n_active_start (taxa de sobrevivência)
```

**Nota:** Com distribuição uniforme de contratos (34/empresa) e sem dados de downgrades, o GRR é essencialmente uma **taxa de sobrevivência de contratos**, não de receita. Documentar essa limitação.

### 10.3 Seção D — 4 Métricas Focadas de Alto Impacto (v4: +CR5)

#### D1. CR4 Trendline — Win Rate por quarter e canonical_segment
- **O que mostra:** Evolução do win rate ao longo do tempo, segmentado
- **Por que importa:** Se CR4 está caindo enquanto VM3 sobe, marketing gera volume sem qualidade — sinal clássico de Constraint 2 WBD (seller-centric)
- **Ação gerada:** Ajustar ICP ou qualificação

#### D2. Δt6 — Time to First Impact (proxy de Onboarding Success)
- **O que mostra:** Mediana de dias do Closed Won até a primeira semana com `feature_depth_score ≥ 0.40`, por canonical_segment
- **Por que importa:** KPI mais importante do lado direito do Bowtie. Δt6 alto → cliente demora para atingir First Impact → probabilidade de churn sobe.

**Cálculo:**
```python
# Para cada Closed Won no SF:
# 1. Identificar a empresa (canonical_name)
# 2. Na telemetria dessa empresa, encontrar a primeira semana APÓS close_date
#    onde feature_depth_score >= 0.40
# 3. Δt6 = data dessa semana - close_date
# 4. Se não atinge 0.40 dentro da janela → onboarding_failed = True
```

**Janela parametrizada por canonical_segment:**

| canonical_segment | Janela máxima | Justificativa |
|-------------------|--------------|---------------|
| SMB | 60 dias | Ciclo de venda curto → onboarding rápido |
| Mid-Market | 90 dias | Janela padrão |
| Enterprise | 120 dias | Ciclo longo, integrações complexas |

**Nota de execução v4:** O range da telemetria é 2024-01-06 a 2025-01-25. Closed Won vão de 2023-08-23 a 2025-01-31. Para Closed Won anteriores a Jan/2024, não há telemetria disponível — esses registros devem ser excluídos do Δt6 ou marcados como `telemetry_not_available`.

#### D3. Closed Loop — ICP Health (Diferenciador killer)

- **O que mostra:** Comparação entre o perfil de clientes saudáveis e o perfil de leads que marketing está gerando
- **Por que importa:** Closed Loop #4 do Bowtie Standard (ICP feedback loop). Responde: "estamos alimentando o pipeline com o tipo certo de cliente?"

**Cálculo (v4 — com cadeia de agregações explicitada):**
```python
# 1. Agregar health_score para nível empresa:
#    health_score_empresa = média ponderada por MRR dos contratos Active
#    (usar apenas contratos com health_score preenchido)

# 2. Agregar feature_depth para nível empresa:
#    avg_feature_depth_empresa = média das últimas 4 semanas da telemetria

# 3. Definir "cliente saudável":
#    health_score_empresa >= 75 AND avg_feature_depth_empresa >= 0.60

# 4. Extrair perfil dessas empresas:
#    - lead_source original (do HS, via leads associados a essa empresa)
#    - canonical_segment
#    - mediana de number_of_employees (do HS)

# 5. Comparar com o perfil de TODOS os leads gerados nos últimos 6 meses:
#    - lead_source distribution
#    - segment distribution
#    - number_of_employees distribution

# 6. Gap = diferença entre os dois perfis
```

**Visualização:** Gráfico de barras lado a lado — "Perfil dos clientes saudáveis" vs. "Perfil dos leads recentes" — com destaque nos gaps.

#### D4. NOVO v4 — CR5 Trendline — ACV por quarter e canonical_segment

- **O que mostra:** Evolução do ACV (Average Contract Value) ao longo do tempo, segmentado por canonical_segment
- **Por que importa:** No Bowtie Standard, CR5 mede a conversão de deals em receita committed (VM5 → VM6). Se ACV está caindo enquanto win rate se mantém, vendas está cedendo em preço para manter volume — sinal de Constraint 2 (seller-centric). Se ACV está subindo, pode indicar movimento up-market saudável ou filtro natural de deals menores.

**Cálculo:**
```python
# Para cada quarter:
#   acv_por_segmento = mean(amount_brl) dos Closed Won, agrupado por canonical_segment
#   Excluir amount_brl < 0 (estornos) do cálculo
```

**Ação gerada:** Se CR5 está caindo e CR4 está estável, o problema é pricing/discounting, não pipeline quality. Se ambos estão caindo, há degradação sistêmica da operação de vendas.

### 10.4 Resell Risk — Gap nos dados e recomendação

A WBD define 4 tipos de crescimento na expansão: Upsell, Cross-sell, Renew e **Resell** (quando o Champion sai e um novo decisor precisa ser reconquistado). Nenhum dos datasets captura turnover de stakeholders.

**Recomendação operacional para governança:**
- Monitorar mudanças no campo `email` do contato principal no HubSpot
- Registrar em QBR se houve mudança de stakeholder/champion
- Criar campo no Catalyst: `champion_change_detected = True/False`

**Nota para apresentação:** Mencionar o Resell Risk como finding demonstra domínio do modelo completo do Bowtie e consciência de onde os dados param de cobrir a jornada.

---

## 11. Arquitetura do Dashboard

### Narrativa unificadora (WBD)

O dashboard conta uma história em 4 atos:
1. **"Como está o motor de aquisição?"** (Seção A — lado esquerdo do Bowtie)
2. **"Os clientes que adquirimos estão recebendo Impact?"** (Seção B — lado direito do Bowtie)
3. **"O que acontece se agirmos / não agirmos?"** (Seção C — simulação do sistema completo)
4. **"Estamos aprendendo com o que funciona?"** (Seção D — closed loops e métricas WBD)

### Seção A — Pipeline Health Overview (VM3→VM5, CR4, Δt4)

| Visualização | Dados | Lógica |
|-------------|-------|--------|
| Distribuição do pipeline por stage, segmento e forecast_category | SF normalizado (com canonical_segment) | Stacked bar + filtros |
| Pipeline coverage vs. meta | SF + input do gestor | `pipeline_total / meta_quarter` — target: 3x |
| Aging do pipeline (X configurável) | SF `days_in_stage` + `slippage_days` | Highlight opps acima do threshold por stage |
| Sinalizações de risco | SF | Flags: `is_slippage`, `is_stale`, `commit_sem_atividade` |
| **Monitor de Estornos** | SF `is_negative_amount` | Volume e tendência de registros com amount < 0 |
| **Slippage Heatmap** | SF | Distribuição de oportunidades por faixa de slippage (≤30d, ≤60d, ≤90d, ≤180d, >180d) |

### Seção B — Churn Risk Signal (CR6, CR7)

| Visualização | Dados | Lógica |
|-------------|-------|--------|
| **GRR (Gross Revenue Retention)** (NOVO v4) | Catalyst | Taxa de sobrevivência de contratos × MRR estimado |
| Segmentação por nível de risco (Alto/Médio/Baixo) | Catalyst + Telemetria (modelo 2 camadas) | Churn_Risk_Score |
| **Decomposição do risco** | Modelo | Para cada contrato Alto/Médio: Contract_Risk vs Company_Engagement_Modifier |
| MRR em risco por segmento e por CSM owner | Catalyst | Soma MRR × nível de risco |
| Timeline de renovações (próximos 90 dias) com risco | Catalyst | `renewal_date` + score por contrato |
| Lista de ação para CS | Catalyst | `renewal_date ≤ 60d` + `risk_level = Alto` |
| **Flag visual de MRR imputado** | Catalyst | 20 contratos com `mrr_imputed = True` aparecem com asterisco/cor diferente |
| **Recall do modelo (backtesting)** (NOVO v4) | Modelo | % de contratos Churned capturados como Alto Risco |

### Seção C — Simulador de Cenários

| Simulador | Inputs do gestor | Output |
|-----------|-----------------|--------|
| **Forecast do quarter** | Win rates por segmento, meta de receita, tiers de slippage discount (configuráveis) | Receita projetada vs. meta, gap, coverage ratio |
| **Churn por threshold** | Threshold de score configurável (sliders para Alto/Médio/Baixo) | Nº contratos, MRR exposto, impacto no GRR e NRR projetado |
| **Intervenção de CS** | Seleção de contratos alto risco para ação | MRR em risco recalculado assumindo que intervenção reduz risco em X% |

### Seção D — Bowtie View (4 Métricas Focadas)

| Visualização | Dados | Lógica |
|-------------|-------|--------|
| **CR4 Trendline** — Win rate por quarter e segmento | SF | Win rate calculado sobre Qualified Opps, usando canonical_segment |
| **Δt6 — Time to First Impact** | SF + Telemetria | Mediana de dias Closed Won → feature_depth ≥ 0.40, por canonical_segment |
| **Closed Loop — ICP Health** | HS + Catalyst + Telemetria | Perfil de clientes saudáveis vs. perfil de leads recentes |
| **CR5 Trendline — ACV por quarter e segmento** (NOVO v4) | SF | amount_brl médio dos Closed Won, por canonical_segment |

---

## 12. Proposta de Governança Contínua

### Pipeline de dados automatizado

**Frequência:** Diária para Catalyst e Telemetria; semanal para SF e HubSpot.

**Gatilhos:**
- Alteração em `stage`, `forecast_category` ou `amount_brl` no SF → recalcular forecast
- Queda de `health_score` > 10 pontos em 7 dias → alerta de CS imediato
- `active_users = 0` por 14 dias consecutivos → alerta de churn risk
- Novo registro com `amount_brl < 0` → alerta para validação manual (estorno)
- Mudança de email do contato principal associado a renovação recente → alerta de Resell risk (WBD)

### Monitoramento de qualidade de dados

**Alertas de degradação:**
- % de NULLs por campo acima de threshold histórico + 10%
- Taxa de join failure acima de threshold
- Novas variantes de `account_name` não presentes na tabela mestre → alerta para atualização
- Duplicatas emergindo por `opportunity_id` ou `hs_contact_id`
- Validação contínua de segment: novo account_name sem canonical_segment atribuído → alerta

### IA vs. validação humana

| Ação | IA resolve | Humano valida |
|------|-----------|---------------|
| Normalização de campos | ✅ | — |
| Cálculo de Churn_Risk_Score (2 camadas) | ✅ | — |
| Recalcular forecast com slippage progressivo | ✅ | — |
| Cálculo de GRR | ✅ | — |
| Alertas de renovação | ✅ | — |
| Entity resolution (novos nomes) | ✅ confiança > 90% | ✅ 70–90% |
| Imputação de MRR faltante | — | ✅ sempre |
| Reclassificação Active → Churned | — | ✅ sempre |
| Atualização de pesos do modelo | — | ✅ trimestral |
| Atualização de tiers de slippage discount | — | ✅ trimestral (baseado em delta real) |
| Atribuição de canonical_segment para novas empresas | — | ✅ sempre |
| Calibração de thresholds de churn (backtesting) | ✅ sugere | ✅ aprova |
| Detecção de champion change (Resell risk) | ✅ detecta | ✅ valida ação |

### Implementação por horizonte

**Primeiros 15 dias:**
- Pipeline de normalização dos 4 datasets
- Tabela mestre de contas (20 empresas) com canonical_segment (employees-first)
- Dashboard v1 funcional com dados estáticos
- Alertas manuais de renovação (lista semanal)
- Modelo de churn risk v1 (2 camadas) + backtesting inicial
- Cálculo de GRR

**Médio prazo (30–90 dias):**
- Integração via API dos 4 sistemas
- Atualização diária automática
- Alertas automáticos via Slack/email
- Validação do modelo de churn contra dados reais de renovação/churn
- Calibração dos tiers de slippage discount com delta real do quarter

**Longo prazo (90+ dias):**
- Contrato semântico entre produtores e consumidores de dados
- Modelo de churn risk recalibrado trimestralmente com feedback loop
- Captura de sinais de Resell risk (champion change)
- Closed Loop analysis atualizado mensalmente
- Agente de IA para queries em linguagem natural

---

## 13. Plano de Execução por Fases

### Fase 1 — Data Treatment (entrada no Claude Code)

**Objetivo:** Produzir a tabela analítica unificada (`master_table`).

Etapas:
1. Ler os 4 CSVs e aplicar todas as normalizações (Bloco B — incluindo P-B6 segment employees-first)
2. Tratar anomalias (Bloco C) e registrar flags
3. Construir tabela mestre de contas com 20 empresas, 81 variantes, 20 domínios, **canonical_segment via employees-first + mode fallback**
4. Executar joins com níveis de confiança (HIGH/MEDIUM)
5. Calcular campos derivados: `is_slippage`, `slippage_days`, `implied_days`, `days_to_renewal`, `canonical_name`, `canonical_segment`, `segment_confidence`, `company_mrr_total`, `is_negative_amount`
6. Documentar: % de registros por nível de confiança, decisões tomadas

### Fase 2 — Churn Risk Model (Arquitetura 2 Camadas + Backtesting)

1. **Camada 2 primeiro:** Calcular `Company_Engagement_Modifier` por empresa via telemetria (últimas 4 semanas)
2. **Camada 1:** Calcular `Contract_Risk` por contrato via Catalyst
3. **Combinação:** `Churn_Risk_Score = Contract_Risk × Company_Engagement_Modifier`
4. Classificar em Alto/Médio/Baixo com thresholds iniciais (0.65/0.35)
5. **NOVO v4 — Backtesting:** Calcular recall sobre os 189 contratos Churned. Se recall Alto < 40%, ajustar thresholds automaticamente.
6. Reportar: distribuição por nível, recall, thresholds finais

### Fase 3 — Métricas WBD e Análises

1. **GRR (CR7):** Calcular taxa de sobrevivência de contratos como proxy de GRR
2. **CR5 (ACV):** Calcular amount_brl médio dos Closed Won por canonical_segment e quarter
3. **CR4 Trendline:** Win rate por quarter e canonical_segment
4. **Δt6 (Time to First Impact):** Mapear close_date de Closed Won para telemetria, calcular mediana por segment
5. **Closed Loop ICP:** Agregar health_score e feature_depth por empresa, definir perfil saudável, comparar com leads

### Fase 4 — Forecast Model

1. Calcular win rates endógenos por canonical_segment (CR4 WBD)
2. Calcular `slippage_days` por oportunidade
3. Aplicar slippage discount progressivo (tabela de tiers)
4. Construir projeção do quarter
5. Preparar outputs para simulador (inputs configuráveis)
6. **NOVO v4 (opcional):** Aplicar engagement modifier como ajuste ao slippage discount

### Fase 5 — Dashboard

1. Seção A: Pipeline Health + Monitor de Estornos + Slippage Heatmap
2. Seção B: Churn Risk Signal (2 camadas com decomposição) + **GRR** + Flag MRR imputado + **Recall backtesting**
3. Seção C: Simulador de Cenários (forecast com slippage tiers + churn threshold ajustável + intervenção CS)
4. Seção D: Bowtie View (CR4 Trendline + Δt6 + Closed Loop ICP + **CR5 Trendline**)

### Fase 6 — Data Treatment Report + Apresentação

1. Compilar decisões documentadas (incluindo todas as correções v1→v4)
2. Incluir backtesting como evidência de rigor
3. Estrutura da apresentação de 30 min:
   - Frame WBD como narrativa unificadora
   - Demonstrar que os dois sintomas (forecast -23% e churn pós-renovação) têm causa-raiz comum: desconexão Value↔Impact
   - Dashboard ao vivo com simulador
   - GRR como KPI do lado direito do Bowtie
   - Backtesting como evidência de poder discriminativo do modelo

---

## Apêndice A — Registro de Correções v1 → v2

### Correção #1 — Número de empresas canônicas: 46 → 20

**Causa raiz:** Normalização parcial insuficiente (apenas lower+strip não colapsa "BluePath"/"Blue Path").
**Impacto:** Afeta entity resolution, cobertura de telemetria, narrativa de fragmentação de dados.

### Correção #2 — IDs fantasma no HubSpot: 421 → 0

**Causa raiz:** Múltiplos leads associados à mesma oportunidade interpretados como erros.
**Impacto:** Remove narrativa de degradação HS↔SF.

### Correção #3 — Cobertura Catalyst↔SF: 3% → 53,5%

**Causa raiz:** Divisão de 20 IDs únicos por 680 records em vez de 364 records com ID preenchido.
**Impacto:** Integridade do join muito melhor do que reportado.

### Correção #4 — Contagem de slippage: 874 → 857

**Causa raiz:** Erro de contagem. Real: 1.200 - 173 Closed Won - 170 Closed Lost = 857.

### Correção #5 — Registros com amount_brl < 0: escopo ampliado

**Causa raiz:** 30 registros em 15+ empresas, R$ -2,98M. Justifica visualização dedicada.

### Correção #6 — Cobertura de telemetria: 43% → 100%

**Causa raiz:** Com 20 empresas (não 46), 20 domínios = 100%.

---

## Apêndice B — Registro de Correções v2 → v3

### Correção #7 — Segmento é opportunity-level, não account-level

**Causa raiz:** Todas as 20 empresas têm oportunidades nos 3 segmentos. Win rates artificialmente similares (~49-53%).
**Resolução v3:** Atribuição via mode + validação cruzada com employees. **Superada na v4.**

### Correção #8 — Modelo de churn risk: soma aditiva → arquitetura em 2 camadas

**Causa raiz:** Engagement score (nível empresa) tratado como componente aditivo com health_score (nível contrato).
**Resolução:** Contract_Risk × Company_Engagement_Modifier. Mantida na v4.

### Correção #9 — Slippage discount: fator fixo 40% → desconto progressivo por aging

**Causa raiz:** O desconto de 40% não tinha justificativa empírica.
**Resolução:** Tabela de desconto progressivo por faixa de slippage_days. Tiers configuráveis no simulador. Mantida na v4.

### Correção #10 — MRR imputado: flag visual obrigatório no dashboard

**Causa raiz:** 20 contratos com MRR imputado totalizando ~$166k de MRR não real.
**Resolução:** Flag visual obrigatório. Mantida na v4.

### Correção #11 — Seção D: cobertura total → 3 métricas focadas de alto impacto

**Causa raiz:** Tentar calcular todos os VM/CR/Δt dilui o foco.
**Resolução v3:** Foco em CR4, Δt6, Closed Loop ICP. **Expandida na v4 para 4 métricas (+CR5).**

### Correção #12 — WBD como framework unificador, não como seção isolada

**Causa raiz:** Na v2, WBD era tratado como seção adicional.
**Resolução:** WBD permeia todas as seções. Mantida na v4.

---

## Apêndice C — Registro de Correções v3 → v4

### Correção #13 — Distribuição de contratos no Catalyst: desigual → uniforme (34/empresa)

**Causa raiz:** A v3 contou contratos usando `account_name` bruto do Catalyst (81 variantes) em vez de nomes canônicos normalizados. O que parecia ser "Beta Solutions com 28 contratos" era na verdade a soma parcial de variantes — ao normalizar, todas as 20 empresas convergem para exatamente 34 contratos cada.

**Método de validação:** Aplicação da função `normalize_canonical()` sobre `cat['account_name']`, seguida de `value_counts()`. Resultado: 20 valores × 34 registros = 680 total.

**Impacto:** Remove necessidade de normalização por volume de contratos. Simplifica comparações entre empresas. O Data Treatment Report deve registrar isso como distribuição uniforme (artefato sintético).

### Correção #14 — Canonical segment: mode-first → employees-first

**Causa raiz:** O mode do campo `segment` no SF tem viés estrutural para SMB (47% do total de opps). A distribuição de `number_of_employees` no HubSpot (21,8% ≤100, 36,6% 101-500, 41,6% >500) sugere perfil mais Enterprise/MM. O mode amplifica o viés da distribuição aleatória original.

**Resolução v4:**
1. Fonte primária: mediana de `number_of_employees` por empresa (via leads associados no HS)
2. Fallback: mode do `segment` original (quando employees indisponível)
3. Confidence: HIGH quando employees disponível, MEDIUM quando apenas mode

**Impacto:** Win rates, ciclos de venda, janelas de onboarding e todas as métricas WBD segmentadas serão recalculadas com segmentos mais representativos. Espera-se maior diferenciação entre segmentos.

### Correção #15 — GRR (CR7) como métrica explícita

**Causa raiz:** GRR é a métrica mais importante do lado direito do Bowtie Standard. A v3 usava churn risk score como proxy sem calcular GRR diretamente.

**Resolução v4:** GRR calculado como taxa de sobrevivência de contratos × MRR estimado. Exibido na Seção B. Limitação documentada: sem série temporal no Catalyst, MRR pré-churn é estimado pela mediana global.

**Impacto:** Preenche gap crítico no lado direito do Bowtie. Permite ao gestor ver a taxa de retenção de receita diretamente, não apenas o risco projetado.

### Correção #16 — CR5 (ACV trendline) como 4ª métrica na Seção D

**Causa raiz:** O Bowtie Standard define CR5 como a conversão de deals em receita committed (desconto/pricing). ACV por segmento em declínio indica seller-centric behavior (Constraint 2 WBD). A v3 não incluía nenhuma métrica de pricing na Seção D.

**Resolução v4:** CR5 trendline adicionado como 4ª visualização na Seção D. Cálculo: mean(amount_brl) dos Closed Won, por canonical_segment e quarter.

**Impacto:** Completa a visão do lado esquerdo do Bowtie (CR4 = eficiência, CR5 = valor monetário). Permite diagnóstico de degradação de pricing vs. degradação de pipeline quality.

### Correção #17 — Backtesting do modelo de churn com recall

**Causa raiz:** Os thresholds do modelo (Alto ≥ 0.65, Médio 0.35-0.64) foram definidos por intuição, sem validação empírica. Se o modelo não discrimina bem entre contratos que deram churn e os que não deram, os thresholds não são úteis operacionalmente.

**Resolução v4:**
1. Calcular Churn_Risk_Score para todos os 680 contratos (incluindo Churned)
2. Medir recall: % dos 189 Churned capturados como Alto Risco
3. Se recall Alto < 40%: ajustar threshold automaticamente
4. Thresholds configuráveis no Simulador (Seção C)

**Limitação documentada:** Contratos Churned têm dados pós-fato (MRR=0, health_score pode refletir estado pós-churn). Backtesting é indicativo, não definitivo.

**Impacto:** Transforma o modelo de churn de "plausível por design" para "empiricamente testado". Na apresentação, o recall é evidência forte de rigor analítico.

---

*Documento v4 elaborado com base em auditoria cruzada independente dos CSVs reais contra premissas do plano v3, conduzida em sessão de revisão dedicada. Todas as 17 correções (v1→v4) estão documentadas e rastreáveis. As decisões da v4 foram tomadas em conjunto entre o operador e o revisor.*

*Ferramentas: Python (pandas), Claude AI (Opus 4.6) | Processo seletivo Pipefy — Especialista em AI para RevOps*
