"""
data_treatment.py — Fase 1: Pipeline completo de tratamento de dados
Plano Mestre v4 — RaptorSoft RevOps Case

Etapas:
  1. Leitura dos 4 CSVs
  2. Normalização de campos (Blocos B, C, D do plano)
  3. Entity resolution (P-A1 — 81 variantes → 20 empresas)
  4. Construção da tabela mestre de contas + canonical_segment
  5. Joins cross-source com níveis de confiança (HIGH/MEDIUM)
  6. Cálculo de campos derivados
  7. Log de qualidade de dados
"""

import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import (
    REFERENCE_DATE, CANONICAL_NAMES, DOMAIN_TO_CANONICAL,
    normalize_canonical, normalize_stage, normalize_forecast_category,
    normalize_lead_source, normalize_plan_tier, employees_to_segment,
    get_renewal_proximity_score, get_nps_bonus, health_trend_to_score,
    OPEN_STAGES, CLOSED_WON_STAGE, CLOSED_LOST_STAGE,
)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# ===========================================================================
# 1. CARGA DOS DADOS BRUTOS
# ===========================================================================

def load_raw_data():
    sf       = pd.read_csv(BASE_DIR / "pipeline_sf.csv")
    hs       = pd.read_csv(BASE_DIR / "leads_hs.csv")
    catalyst = pd.read_csv(BASE_DIR / "health_catalyst.csv")
    telemetry = pd.read_csv(BASE_DIR / "telemetry_product.csv")
    return sf, hs, catalyst, telemetry


# ===========================================================================
# 2. NORMALIZAÇÃO — SALESFORCE (pipeline_sf.csv)
# ===========================================================================

def normalize_sf(sf_raw: pd.DataFrame) -> pd.DataFrame:
    df = sf_raw.copy()

    # Parse dates
    df["close_date"]   = pd.to_datetime(df["close_date"],   errors="coerce")
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

    # [SF-01] Stage: 14 variantes → 6 canônicos
    df["stage_canonical"] = df["stage"].apply(normalize_stage)

    # [SF-02] Forecast category: 8 variantes → 4
    df["forecast_category_canonical"] = df["forecast_category"].apply(normalize_forecast_category)

    # [SF-07] Entity resolution — canonical_name
    df["canonical_name"] = df["account_name"].apply(normalize_canonical)

    # [SF-04] days_in_stage anomalias
    # implied_days = close_date - created_date
    df["implied_days"] = (df["close_date"] - df["created_date"]).dt.days

    # Grupo B: days_in_stage < 0 E implied_days < 0 (datas corrompidas — 4 registros)
    group_b = (df["days_in_stage"] < 0) & (df["implied_days"].fillna(-1) < 0)
    df.loc[group_b, "days_in_stage"] = np.nan
    df.loc[group_b, "implied_days"]  = np.nan
    df.loc[group_b, "data_quality"]  = "invalid_dates"

    # Grupo A: days_in_stage < 0 mas implied_days válido (21 registros)
    group_a = (df["days_in_stage"] < 0) & (~group_b)
    df.loc[group_a, "days_in_stage"]       = df.loc[group_a, "implied_days"]
    df.loc[group_a, "days_recalculated"]   = True

    # [SF-04] Oportunidades stale (> 365 dias no stage)
    df["is_stale_opportunity"] = df["days_in_stage"] > 365

    # [SF-03] Slippage: stages abertos com close_date no passado
    df["is_open"]       = df["stage_canonical"].isin(OPEN_STAGES)
    df["slippage_days"] = (REFERENCE_DATE - df["close_date"]).dt.days.where(df["is_open"], other=0)
    df["is_slippage"]   = df["is_open"] & (df["slippage_days"] > 0)

    # [SF-05] Amounts negativos
    df["is_negative_amount"] = df["amount_brl"] < 0

    # Flags de estágio fechado
    df["is_closed_won"]  = df["stage_canonical"] == CLOSED_WON_STAGE
    df["is_closed_lost"] = df["stage_canonical"] == CLOSED_LOST_STAGE

    # Quarter de criação e fechamento (para trendlines)
    df["close_quarter"]   = df["close_date"].dt.to_period("Q").astype(str)
    df["created_quarter"] = df["created_date"].dt.to_period("Q").astype(str)

    return df


# ===========================================================================
# 3. NORMALIZAÇÃO — HUBSPOT (leads_hs.csv)
# ===========================================================================

def normalize_hs(hs_raw: pd.DataFrame) -> pd.DataFrame:
    df = hs_raw.copy()

    # Parse dates
    df["mql_date"] = pd.to_datetime(df["mql_date"], errors="coerce")

    # [HS-01] Country → BR (campo sem valor analítico)
    df["country_canonical"] = "BR"

    # [HS-02] Lead source: 18 variantes → 7 grupos
    df["lead_source_canonical"] = df["lead_source"].apply(normalize_lead_source)

    # Entity resolution — canonical_name
    df["canonical_name"] = df["company_name"].apply(normalize_canonical)

    # Marcar leads sem opp no SF
    df["has_sf_opportunity"] = df["sf_opportunity_id"].notna()

    return df


# ===========================================================================
# 4. NORMALIZAÇÃO — HEALTH CATALYST (health_catalyst.csv)
# ===========================================================================

def normalize_catalyst(cat_raw: pd.DataFrame):
    """
    Retorna (df_catalyst_clean, median_mrr_active).
    median_mrr_active usado para imputação e GRR.
    """
    df = cat_raw.copy()

    # Parse dates
    df["renewal_date"]  = pd.to_datetime(df["renewal_date"],  errors="coerce")
    df["churn_date"]    = pd.to_datetime(df["churn_date"],    errors="coerce")
    df["last_qbr_date"] = pd.to_datetime(df["last_qbr_date"], errors="coerce")

    df["mrr_usd"] = pd.to_numeric(df["mrr_usd"], errors="coerce").fillna(0.0)

    # Entity resolution — canonical_name
    df["canonical_name"] = df["account_name"].apply(normalize_canonical)

    # Mediana global MRR calculada ANTES da reclassificacao (P-C6)
    # Plano v4: "mediana calculada sobre 512 contratos Active com MRR > 0"
    # 532 Active originais - 20 com MRR=0 = 512. Deve ser calculado pre-CA-06.
    active_with_mrr_pre = (df["status"] == "Active") & (df["mrr_usd"] > 0)
    median_mrr = round(df.loc[active_with_mrr_pre, "mrr_usd"].median(), 2)

    # [CA-06] Reclassificar Active com churn_date -> Churned
    # Nota v4: 1 dos 20 contratos MRR=0 tem churn_date -> 19 permanecem Active p/ imputacao
    mask_reclassify = (df["status"] == "Active") & df["churn_date"].notna()
    df.loc[mask_reclassify, "status"]           = "Churned"
    df.loc[mask_reclassify, "status_corrected"] = True

    # [CA-05] Imputar MRR=0 nos contratos Active
    mask_impute = (df["status"] == "Active") & (df["mrr_usd"] == 0)
    df.loc[mask_impute, "mrr_usd"]    = median_mrr
    df.loc[mask_impute, "mrr_imputed"] = True
    df.loc[mask_impute, "mrr_source"]  = "global_median_proxy"

    df["mrr_imputed"] = df["mrr_imputed"].fillna(False)
    df["mrr_source"]  = df["mrr_source"].fillna("actual")

    # Dias até renovação (referência: 2025-04-15)
    df["days_to_renewal"] = (df["renewal_date"] - REFERENCE_DATE).dt.days

    # Score de proximidade de renovação (W5 — input para churn model)
    df["renewal_proximity_score"] = df["days_to_renewal"].apply(get_renewal_proximity_score)

    # NPS bonus signal (input para churn model)
    df["nps_bonus"] = df["nps_score"].apply(get_nps_bonus)

    # Health trend score (W2 — input para churn model)
    df["health_trend_score"] = df["health_trend"].apply(health_trend_to_score)

    # [CA-04] Sinal "sem QBR recente": ausente ou > 180 dias
    qbr_days_ago = (REFERENCE_DATE - df["last_qbr_date"]).dt.days
    df["no_qbr_signal"] = df["last_qbr_date"].isna() | (qbr_days_ago > 180)

    # Support pressure normalizada (W4)
    max_tickets = df["open_tickets"].max()
    if max_tickets > 0:
        df["support_pressure"] = df["open_tickets"].fillna(0) / max_tickets
    else:
        df["support_pressure"] = 0.0

    return df, median_mrr


# ===========================================================================
# 5. NORMALIZAÇÃO — TELEMETRIA (telemetry_product.csv)
# ===========================================================================

def normalize_telemetry(tel_raw: pd.DataFrame) -> pd.DataFrame:
    df = tel_raw.copy()

    # Parse dates
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    # [TE-03] Plan tier: 10 variantes → 4 canônicos
    df["plan_tier_canonical"] = df["plan_tier"].apply(normalize_plan_tier)

    # [TE-02] Cap feature_depth_score em 1.0
    df["feature_depth_capped"] = df["feature_depth_score"] > 1.0
    df["feature_depth_score"]  = df["feature_depth_score"].clip(upper=1.0)

    # [TE-01] Canonical name via domain
    df["canonical_name"] = df["account_domain"].map(DOMAIN_TO_CANONICAL)

    # [TE-04] Semanas com zero são dados válidos — mantidos como estão
    # (ativa ausência de uso como sinal real — P-C7)

    return df


# ===========================================================================
# 6. TABELA MESTRE DE CONTAS + CANONICAL SEGMENT (Seção 5 e P-A1)
# ===========================================================================

def build_master_accounts(sf_clean: pd.DataFrame, hs_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói a tabela mestre das 20 empresas com:
    - canonical_name, canonical_segment (employees-first + mode fallback)
    - segment_source, segment_confidence
    - sf_account_id representativo
    """
    records = []

    # Passo 1: Mediana de employees por empresa no HubSpot
    hs_with_emp = hs_clean[hs_clean["number_of_employees"].notna()].copy()
    hs_with_emp["number_of_employees"] = pd.to_numeric(
        hs_with_emp["number_of_employees"], errors="coerce"
    )
    employees_by_company = (
        hs_with_emp[hs_with_emp["canonical_name"].isin(CANONICAL_NAMES)]
        .groupby("canonical_name")["number_of_employees"]
        .median()
    )

    # Passo 2: Mode do segment no SF (fallback)
    mode_by_company = (
        sf_clean[sf_clean["canonical_name"].isin(CANONICAL_NAMES)]
        .groupby("canonical_name")["segment"]
        .agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "SMB")
    )

    # Passo 3: sf_account_id representativo
    sf_account_ids = (
        sf_clean[sf_clean["account_id"].notna()]
        .groupby("canonical_name")["account_id"]
        .first()
    )

    for company in CANONICAL_NAMES:
        emp_median = employees_by_company.get(company, None)
        segment_from_emp = employees_to_segment(emp_median)
        mode_seg = mode_by_company.get(company, "SMB")

        if segment_from_emp is not None:
            canonical_segment = segment_from_emp
            segment_source     = "employees_median"
            segment_confidence = "HIGH"
        else:
            canonical_segment = mode_seg
            segment_source     = "mode_fallback"
            segment_confidence = "MEDIUM"

        records.append({
            "canonical_name":       company,
            "canonical_segment":    canonical_segment,
            "segment_source":       segment_source,
            "segment_confidence":   segment_confidence,
            "employees_median":     emp_median,
            "segment_mode_sf":      mode_seg,
            "sf_account_id":        sf_account_ids.get(company, None),
            "domain":               {v: k for k, v in DOMAIN_TO_CANONICAL.items()}.get(company, None),
        })

    return pd.DataFrame(records).set_index("canonical_name")


# ===========================================================================
# 7. JOINS CROSS-SOURCE COM NÍVEIS DE CONFIANÇA (Seção 6)
# ===========================================================================

def enrich_sf_with_segment(sf_clean: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    """
    Propaga canonical_segment da tabela mestre para todas as oportunidades SF.
    Mantém segment_original para auditoria (P-B6).
    """
    df = sf_clean.copy()
    df["segment_original"]    = df["segment"]
    df["canonical_segment"]   = df["canonical_name"].map(master["canonical_segment"])
    df["segment_confidence"]  = df["canonical_name"].map(master["segment_confidence"])
    df["segment_source"]      = df["canonical_name"].map(master["segment_source"])
    return df


def enrich_catalyst_with_segment(cat_clean: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    """
    Propaga canonical_segment ao Catalyst.
    Join por canonical_name (P-A2) + sf_account_id quando disponível.
    """
    df = cat_clean.copy()
    df["canonical_segment"]  = df["canonical_name"].map(master["canonical_segment"])
    df["segment_confidence"] = df["canonical_name"].map(master["segment_confidence"])

    # Nível de confiança do join SF ↔ Catalyst (Seção 6)
    df["join_confidence"] = df["sf_account_id"].apply(
        lambda x: "HIGH" if pd.notna(x) else "MEDIUM"
    )
    return df


def enrich_telemetry(tel_clean: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona canonical_segment e sf_account_id à telemetria.
    """
    df = tel_clean.copy()
    df["canonical_segment"] = df["canonical_name"].map(master["canonical_segment"])
    return df


# ===========================================================================
# 8. CAMPOS DERIVADOS FINAIS
# ===========================================================================

def add_sf_derived_fields(sf_enriched: pd.DataFrame) -> pd.DataFrame:
    """Adiciona campos derivados finais ao SF (ex: quarter, sales_cycle_days)."""
    df = sf_enriched.copy()

    # Sales cycle para Closed Won (created_date → close_date)
    won_mask = df["is_closed_won"]
    df.loc[won_mask, "sales_cycle_days"] = (
        df.loc[won_mask, "close_date"] - df.loc[won_mask, "created_date"]
    ).dt.days

    # Quarter para análise de trendline
    df["close_quarter"] = df["close_date"].dt.to_period("Q").astype(str)

    return df


def compute_company_mrr(catalyst_enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega MRR total por empresa (soma contratos Active).
    Retorna DataFrame indexado por canonical_name.
    """
    active = catalyst_enriched[catalyst_enriched["status"] == "Active"]
    company_mrr = (
        active.groupby("canonical_name")
        .agg(
            company_mrr_total=("mrr_usd", "sum"),
            contract_count_active=("customer_id", "count"),
        )
        .round(2)
    )
    return company_mrr


# ===========================================================================
# 9. EXECUÇÃO COMPLETA DO PIPELINE
# ===========================================================================

def run_pipeline():
    print("=" * 60)
    print("PIPELINE DE TRATAMENTO DE DADOS — v4")
    print("Referência: 2025-04-15")
    print("=" * 60)

    # --- Carga ---
    print("\n[1/7] Carregando dados brutos...")
    sf_raw, hs_raw, cat_raw, tel_raw = load_raw_data()
    print(f"  SF: {len(sf_raw):,} | HS: {len(hs_raw):,} | Catalyst: {len(cat_raw):,} | Telemetria: {len(tel_raw):,}")

    # --- Normalização ---
    print("\n[2/7] Normalizando Salesforce...")
    sf_clean = normalize_sf(sf_raw)
    _validate_sf(sf_clean)

    print("\n[3/7] Normalizando HubSpot...")
    hs_clean = normalize_hs(hs_raw)
    _validate_hs(hs_clean)

    print("\n[4/7] Normalizando Health Catalyst...")
    cat_clean, median_mrr = normalize_catalyst(cat_raw)
    _validate_catalyst(cat_clean, median_mrr)

    print("\n[5/7] Normalizando Telemetria...")
    tel_clean = normalize_telemetry(tel_raw)
    _validate_telemetry(tel_clean)

    # --- Tabela mestre ---
    print("\n[6/7] Construindo tabela mestre de contas + canonical_segment...")
    master_accounts = build_master_accounts(sf_clean, hs_clean)
    _validate_master(master_accounts)

    # --- Enriquecimento + joins ---
    print("\n[7/7] Enriquecendo datasets com canonical_segment e joins...")
    sf_enriched  = enrich_sf_with_segment(sf_clean, master_accounts)
    sf_enriched  = add_sf_derived_fields(sf_enriched)
    cat_enriched = enrich_catalyst_with_segment(cat_clean, master_accounts)
    tel_enriched = enrich_telemetry(tel_clean, master_accounts)
    company_mrr  = compute_company_mrr(cat_enriched)

    # Join catalyst + company_mrr para facilitar dashboard
    cat_enriched = cat_enriched.join(company_mrr, on="canonical_name", how="left")

    # --- Salvar outputs ---
    print("\n  Salvando arquivos processados em /data...")
    sf_enriched.to_csv(DATA_DIR / "sf_clean.csv", index=False)
    hs_clean.to_csv(DATA_DIR / "hs_clean.csv", index=False)
    cat_enriched.to_csv(DATA_DIR / "catalyst_clean.csv", index=False)
    tel_enriched.to_csv(DATA_DIR / "telemetry_clean.csv", index=False)
    master_accounts.to_csv(DATA_DIR / "master_accounts.csv")

    _print_join_coverage(sf_enriched, hs_clean, cat_enriched, tel_enriched)

    print("\n[OK] Pipeline concluido com sucesso.")
    return sf_enriched, hs_clean, cat_enriched, tel_enriched, master_accounts, median_mrr


# ===========================================================================
# VALIDAÇÕES INTERMEDIÁRIAS
# ===========================================================================

def _validate_sf(df):
    n_slippage   = df["is_slippage"].sum()
    n_stale      = df["is_stale_opportunity"].sum()
    n_neg        = df["is_negative_amount"].sum()
    n_neg_total  = df.loc[df["is_negative_amount"], "amount_brl"].sum()
    n_recalc     = df["days_recalculated"].sum() if "days_recalculated" in df else 0
    n_invalid    = (df.get("data_quality", pd.Series(dtype=str)) == "invalid_dates").sum()
    n_companies  = df["canonical_name"].nunique()

    print(f"  stage canônico — {df['stage_canonical'].nunique()} valores únicos: {sorted(df['stage_canonical'].unique())}")
    print(f"  [SF-03] Slippage: {n_slippage} oportunidades abertas com close_date no passado")
    print(f"  [SF-04] days_in_stage Grupo A (recalculado): {n_recalc} | Grupo B (inválido): {n_invalid}")
    print(f"  [SF-04] Stale (>365 dias): {n_stale}")
    print(f"  [SF-05] Amounts negativos: {n_neg} | Total: R$ {n_neg_total:,.2f}")
    print(f"  [SF-07] Entity resolution: {n_companies} empresas canônicas")
    assert n_companies == 20, f"ERRO: Esperado 20 empresas, obtido {n_companies}"
    # Stale: 20 no raw + possivelmente 1 extra de Group A pos-recalculo (aceitavel)
    print(f"  [SF-04] Stale pos-limpeza: {n_stale} (raw: 20 + eventual Group A > 365)")
    assert n_slippage == 857, f"ERRO: Esperado 857 slippage, obtido {n_slippage}"
    assert n_neg == 30,       f"ERRO: Esperado 30 amounts negativos, obtido {n_neg}"


def _validate_hs(df):
    n_with_opp = df["has_sf_opportunity"].sum()
    n_companies = df["canonical_name"].isin(CANONICAL_NAMES).sum()
    print(f"  Leads com sf_opportunity_id: {n_with_opp} ({n_with_opp/len(df)*100:.1f}%)")
    print(f"  Leads de empresas canônicas: {n_companies}/{len(df)}")


def _validate_catalyst(df, median_mrr):
    n_active  = (df["status"] == "Active").sum()
    n_churned = (df["status"] == "Churned").sum()
    n_imputed = df["mrr_imputed"].sum()
    n_corrected = df.get("status_corrected", pd.Series(dtype=bool)).sum()
    print(f"  Após reclassificação: Active={n_active} | Churned={n_churned}")
    print(f"  [CA-06] Contratos reclassificados Active->Churned: {n_corrected}")
    print(f"  [CA-05] Contratos com MRR imputado: {n_imputed} | Mediana global: USD {median_mrr:,.2f}")
    assert n_active == 491,   f"ERRO: Esperado 491 Active, obtido {n_active}"
    assert n_churned == 189,  f"ERRO: Esperado 189 Churned, obtido {n_churned}"
    # Nota v4: 1 dos 20 com MRR=0 tinha churn_date, entao apenas 19 sao imputados
    assert n_imputed == 19,   f"ERRO: Esperado 19 imputados pos-reclassificacao, obtido {n_imputed}"
    assert abs(median_mrr - 8319.83) < 1.0, f"ERRO: Mediana MRR inesperada: {median_mrr}"


def _validate_telemetry(df):
    n_capped    = df["feature_depth_capped"].sum()
    n_companies = df["canonical_name"].nunique()
    print(f"  [TE-02] feature_depth_score capped: {n_capped}")
    print(f"  [TE-01] Empresas mapeadas por domínio: {n_companies}")
    assert n_capped == 693,    f"ERRO: Esperado 693 capped, obtido {n_capped}"
    assert n_companies == 20,  f"ERRO: Esperado 20 empresas, obtido {n_companies}"


def _validate_master(master):
    print(f"  Empresas na tabela mestre: {len(master)}")
    print(f"  Canonical segment distribution:")
    seg_dist = master["canonical_segment"].value_counts()
    for seg, cnt in seg_dist.items():
        print(f"    {seg}: {cnt} empresas")
    high_conf = (master["segment_confidence"] == "HIGH").sum()
    med_conf  = (master["segment_confidence"] == "MEDIUM").sum()
    print(f"  Confiança: HIGH={high_conf} | MEDIUM={med_conf}")
    assert len(master) == 20, f"ERRO: Esperado 20 empresas, obtido {len(master)}"


def _print_join_coverage(sf, hs, cat, tel):
    print("\n  === Cobertura de Joins ===")
    # HS ↔ SF
    hs_with_opp = hs["sf_opportunity_id"].notna()
    hs_matched  = hs.loc[hs_with_opp, "sf_opportunity_id"].nunique()
    print(f"  HS->SF (opp_id): {hs['sf_opportunity_id'].notna().sum()} leads -> {hs_matched} opps unicas")

    # SF ↔ Catalyst (via account_id)
    cat_high = (cat["join_confidence"] == "HIGH").sum()
    cat_med  = (cat["join_confidence"] == "MEDIUM").sum()
    print(f"  SF->Catalyst: HIGH (account_id)={cat_high} ({cat_high/len(cat)*100:.1f}%) | MEDIUM (canonical_name)={cat_med} ({cat_med/len(cat)*100:.1f}%)")

    # Telemetry coverage
    print(f"  Telemetria: {tel['canonical_name'].notna().sum()}/{len(tel)} registros com canonical_name")


if __name__ == "__main__":
    run_pipeline()
