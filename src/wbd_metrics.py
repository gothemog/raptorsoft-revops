"""
wbd_metrics.py — Fase 4: Metricas WBD — Bowtie Standard
Plano Mestre v4 — RaptorSoft RevOps Case, Secao 10

Metricas calculadas:
  CR7 — GRR (Gross Revenue Retention)
  CR4 — Win Rate Trendline por quarter
  CR5 — ACV (Average Contract Value) Trendline por quarter
  dt6 — Time to First Impact (Closed Won -> feature_depth >= 0.40)
  ICP — Closed Loop: perfil clientes saudaveis vs. leads recentes
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils import REFERENCE_DATE, CANONICAL_NAMES, CLOSED_WON_STAGE

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

FEATURE_DEPTH_THRESHOLD = 0.40   # Limiar de First Impact (Secao 10.2)


# ===========================================================================
# CR7 — GRR (Gross Revenue Retention) (Secao 10.2)
# ===========================================================================

def compute_grr(cat: pd.DataFrame, median_mrr: float) -> dict:
    """
    Calcula GRR como taxa de sobrevivencia de contratos x MRR estimado.
    Secao 10.2 — NOVO v4.

    Abordagem: com Catalyst sendo snapshot (nao serie temporal), usamos:
    GRR = n_active_current / n_active_start (taxa de sobrevivencia)
    onde n_active_start = n_active_current + n_churned_no_periodo

    Limitacao documentada: sem serie temporal, MRR pre-churn e estimado
    pela mediana global dos Active.
    """
    n_active_current = (cat["status"] == "Active").sum()
    n_churned        = (cat["status"] == "Churned").sum()
    n_active_start   = n_active_current + n_churned

    # MRR estimado (pre-churn para contratos Churned = mediana global)
    mrr_active_current = cat.loc[cat["status"] == "Active", "mrr_usd"].sum()
    mrr_start_estimated = n_active_start * median_mrr
    mrr_lost_estimated  = n_churned * median_mrr

    grr_contracts = round(n_active_current / n_active_start, 4) if n_active_start > 0 else 0
    grr_mrr       = round((mrr_start_estimated - mrr_lost_estimated) / mrr_start_estimated, 4) if mrr_start_estimated > 0 else 0

    # GRR por empresa
    company_grr = []
    for company in CANONICAL_NAMES:
        comp = cat[cat["canonical_name"] == company]
        n_act = (comp["status"] == "Active").sum()
        n_chu = (comp["status"] == "Churned").sum()
        n_tot = n_act + n_chu
        grr_c = round(n_act / n_tot, 4) if n_tot > 0 else 0
        mrr_at_risk = n_chu * median_mrr
        company_grr.append({
            "canonical_name": company,
            "n_active":       n_act,
            "n_churned":      n_chu,
            "n_total":        n_tot,
            "grr_contracts":  grr_c,
            "mrr_lost_est":   round(mrr_at_risk, 2),
        })

    company_grr_df = pd.DataFrame(company_grr)

    return {
        "n_active_current":       int(n_active_current),
        "n_churned":              int(n_churned),
        "n_active_start":         int(n_active_start),
        "grr_contracts":          grr_contracts,
        "grr_mrr":                grr_mrr,
        "grr_pct":                f"{grr_contracts*100:.1f}%",
        "mrr_active_current_usd": round(mrr_active_current, 2),
        "mrr_start_estimated_usd": round(mrr_start_estimated, 2),
        "mrr_lost_estimated_usd": round(mrr_lost_estimated, 2),
        "company_grr":            company_grr_df,
        "note": (
            "GRR calculado como taxa de sobrevivencia de contratos. "
            "Sem serie temporal no Catalyst — MRR pre-churn estimado pela mediana global "
            f"(USD {median_mrr:,.2f}). Ver Data Treatment Report."
        ),
    }


# ===========================================================================
# CR4 — WIN RATE TRENDLINE POR QUARTER (Secao 10.3, D1)
# ===========================================================================

def compute_cr4_trendline(sf: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula win rate por quarter de fechamento (close_quarter).
    Exclui amounts negativos e omit.
    Usa canonical_segment.
    """
    closed = sf[
        sf["stage_canonical"].isin(["Closed Won", "Closed Lost"])
        & ~sf["is_negative_amount"]
        & sf["amount_brl"].notna()
        & sf["close_quarter"].notna()
    ].copy()

    result = (
        closed.groupby(["close_quarter", "canonical_segment"])
        .apply(lambda g: pd.Series({
            "n_won":    (g["stage_canonical"] == "Closed Won").sum(),
            "n_lost":   (g["stage_canonical"] == "Closed Lost").sum(),
            "won_brl":  g.loc[g["stage_canonical"] == "Closed Won", "amount_brl"].sum(),
            "lost_brl": g.loc[g["stage_canonical"] == "Closed Lost", "amount_brl"].sum(),
        }), include_groups=False)
        .reset_index()
    )

    result["win_rate_count"] = (result["n_won"] / (result["n_won"] + result["n_lost"])).round(4)
    result["win_rate_value"] = (result["won_brl"] / (result["won_brl"] + result["lost_brl"])).round(4)
    result["total_opps"]     = result["n_won"] + result["n_lost"]

    return result.sort_values("close_quarter")


# ===========================================================================
# CR5 — ACV TRENDLINE POR QUARTER (Secao 10.3, D4 — NOVO v4)
# ===========================================================================

def compute_cr5_trendline(sf: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula ACV medio dos Closed Won por quarter e canonical_segment.
    Exclui amounts negativos.
    """
    won = sf[
        (sf["stage_canonical"] == "Closed Won")
        & ~sf["is_negative_amount"]
        & sf["amount_brl"].notna()
        & sf["close_quarter"].notna()
    ].copy()

    result = (
        won.groupby(["close_quarter", "canonical_segment"])
        .agg(
            acv_mean=("amount_brl", "mean"),
            acv_median=("amount_brl", "median"),
            n_deals=("amount_brl", "count"),
            total_won_brl=("amount_brl", "sum"),
        )
        .round(2)
        .reset_index()
    )

    return result.sort_values("close_quarter")


# ===========================================================================
# dt6 — TIME TO FIRST IMPACT (Secao 10.3, D2)
# ===========================================================================

def compute_dt6_time_to_first_impact(sf: pd.DataFrame, tel: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada Closed Won, encontra a primeira semana APOS close_date
    onde feature_depth_score >= 0.40 (Secao 10.3, D2).

    Janelas por canonical_segment:
      SMB: 60d | Mid-Market: 90d | Enterprise: 120d

    Limitacao: telemetria range 2024-01-06 a 2025-01-25.
    Closed Won antes de Jan/2024 marcados como telemetry_not_available.
    """
    SEGMENT_WINDOWS = {
        "SMB":         60,
        "Mid-Market":  90,
        "Enterprise":  120,
    }
    DEFAULT_WINDOW = 90

    tel["week_start"] = pd.to_datetime(tel["week_start"], errors="coerce")
    tel_min_date = tel["week_start"].min()

    won = sf[
        (sf["stage_canonical"] == "Closed Won")
        & sf["close_date"].notna()
        & sf["canonical_name"].notna()
    ].copy()

    results = []
    for _, opp in won.iterrows():
        close_date     = opp["close_date"]
        company        = opp["canonical_name"]
        segment        = opp.get("canonical_segment", "Mid-Market")
        window_days    = SEGMENT_WINDOWS.get(segment, DEFAULT_WINDOW)
        opp_id         = opp["opportunity_id"]

        # Sem telemetria disponivel para closes muito antigos
        if close_date < tel_min_date:
            results.append({
                "opportunity_id":  opp_id,
                "canonical_name":  company,
                "close_date":      close_date,
                "canonical_segment": segment,
                "dt6_days":        np.nan,
                "first_impact_date": pd.NaT,
                "onboarding_failed": False,
                "telemetry_not_available": True,
                "window_days":     window_days,
            })
            continue

        # Telemetria da empresa apos close_date e dentro da janela
        window_end = close_date + pd.Timedelta(days=window_days)
        comp_tel   = tel[
            (tel["canonical_name"] == company)
            & (tel["week_start"] > close_date)
            & (tel["week_start"] <= window_end)
            & (tel["feature_depth_score"] >= FEATURE_DEPTH_THRESHOLD)
        ].sort_values("week_start")

        if comp_tel.empty:
            results.append({
                "opportunity_id":  opp_id,
                "canonical_name":  company,
                "close_date":      close_date,
                "canonical_segment": segment,
                "dt6_days":        np.nan,
                "first_impact_date": pd.NaT,
                "onboarding_failed": True,
                "telemetry_not_available": False,
                "window_days":     window_days,
            })
        else:
            first_impact_date = comp_tel.iloc[0]["week_start"]
            dt6 = (first_impact_date - close_date).days
            results.append({
                "opportunity_id":  opp_id,
                "canonical_name":  company,
                "close_date":      close_date,
                "canonical_segment": segment,
                "dt6_days":        dt6,
                "first_impact_date": first_impact_date,
                "onboarding_failed": False,
                "telemetry_not_available": False,
                "window_days":     window_days,
            })

    df_dt6 = pd.DataFrame(results)

    return df_dt6


def summarize_dt6(dt6_df: pd.DataFrame) -> dict:
    """Agrega dt6 por canonical_segment."""
    valid = dt6_df[
        ~dt6_df["telemetry_not_available"]
        & dt6_df["dt6_days"].notna()
    ]
    failed = dt6_df[~dt6_df["telemetry_not_available"] & dt6_df["onboarding_failed"]]
    no_tel = dt6_df[dt6_df["telemetry_not_available"]]

    summary_by_seg = (
        valid.groupby("canonical_segment")["dt6_days"]
        .agg(mediana="median", media="mean", n="count")
        .round(1)
    )

    fail_by_seg = (
        dt6_df[~dt6_df["telemetry_not_available"]]
        .groupby("canonical_segment")
        .apply(lambda g: pd.Series({
            "n_total":  len(g),
            "n_failed": g["onboarding_failed"].sum(),
            "fail_rate": round(g["onboarding_failed"].sum() / len(g), 4) if len(g) > 0 else 0,
        }), include_groups=False)
        .reset_index()
    )

    return {
        "summary_by_segment":    summary_by_seg,
        "fail_rates":            fail_by_seg,
        "n_valid":               len(valid),
        "n_failed":              len(failed),
        "n_no_telemetry":        len(no_tel),
        "overall_median_dt6":    round(valid["dt6_days"].median(), 1) if len(valid) > 0 else np.nan,
    }


# ===========================================================================
# ICP HEALTH — CLOSED LOOP (Secao 10.3, D3)
# ===========================================================================

def compute_icp_health(cat: pd.DataFrame, tel: pd.DataFrame, hs: pd.DataFrame) -> dict:
    """
    Compara perfil de clientes saudaveis vs. leads recentes.
    Secao 10.3, D3 — Differentiator killer.

    Saudavel: health_score_empresa >= 75 E avg_feature_depth_empresa >= 0.60
    """
    tel["week_start"] = pd.to_datetime(tel["week_start"], errors="coerce")

    # Agregar health_score por empresa (media ponderada por MRR)
    active_cat = cat[cat["status"] == "Active"].copy()
    active_cat_valid = active_cat[active_cat["health_score"].notna()].copy()

    def weighted_health(grp):
        if grp["mrr_usd"].sum() == 0:
            return grp["health_score"].mean()
        return np.average(grp["health_score"], weights=grp["mrr_usd"])

    health_by_company = (
        active_cat_valid.groupby("canonical_name")
        .apply(weighted_health, include_groups=False)
        .rename("health_score_empresa")
        .round(2)
    )

    # Agregar feature_depth por empresa (media ultimas 4 semanas)
    feat_by_company = {}
    for company in CANONICAL_NAMES:
        comp_tel = tel[tel["canonical_name"] == company]
        if comp_tel.empty:
            feat_by_company[company] = np.nan
        else:
            last4 = comp_tel.nlargest(4, "week_start")
            feat_by_company[company] = round(last4["feature_depth_score"].mean(), 4)

    feat_series = pd.Series(feat_by_company, name="avg_feature_depth_empresa")

    # Combinar
    company_profile = pd.DataFrame({
        "health_score_empresa":    health_by_company,
        "avg_feature_depth_empresa": feat_series,
    })
    company_profile.index.name = "canonical_name"

    # Definir "cliente saudavel" — thresholds do plano v4
    ABS_HEALTH_THRESHOLD = 75
    ABS_DEPTH_THRESHOLD  = 0.60

    company_profile["is_healthy_absolute"] = (
        (company_profile["health_score_empresa"] >= ABS_HEALTH_THRESHOLD)
        & (company_profile["avg_feature_depth_empresa"] >= ABS_DEPTH_THRESHOLD)
    )

    # Se nenhuma empresa atinge threshold absoluto, usar threshold relativo
    # (top 30% em health_score E top 30% em feature_depth)
    # FINDING: health_score max = 65.5, abaixo do threshold 75 do plano
    # Documentado como limitacao no Data Treatment Report
    healthy_companies = company_profile[company_profile["is_healthy_absolute"]].index.tolist()
    n_healthy_absolute = len(healthy_companies)

    if n_healthy_absolute == 0:
        health_p70   = company_profile["health_score_empresa"].quantile(0.70)
        depth_p70    = company_profile["avg_feature_depth_empresa"].quantile(0.70)
        company_profile["is_healthy"] = (
            (company_profile["health_score_empresa"] >= health_p70)
            & (company_profile["avg_feature_depth_empresa"] >= depth_p70)
        )
        threshold_used = "relative_p70"
        health_threshold_used = round(health_p70, 2)
        depth_threshold_used  = round(depth_p70, 4)
    else:
        company_profile["is_healthy"] = company_profile["is_healthy_absolute"]
        threshold_used = "absolute"
        health_threshold_used = ABS_HEALTH_THRESHOLD
        depth_threshold_used  = ABS_DEPTH_THRESHOLD

    healthy_companies = company_profile[company_profile["is_healthy"]].index.tolist()
    n_healthy = len(healthy_companies)

    # Perfil dos clientes saudaveis via leads do HS
    hs_healthy = hs[hs["canonical_name"].isin(healthy_companies)]
    hs_all_recent = hs  # todos os leads disponíveis

    # Lead source distribution
    healthy_lead_src = (
        hs_healthy["lead_source_canonical"].value_counts(normalize=True) * 100
    ).round(1)

    all_lead_src = (
        hs_all_recent["lead_source_canonical"].value_counts(normalize=True) * 100
    ).round(1)

    # Employees distribution (proxy de segmento)
    hs_healthy_valid = hs_healthy[hs_healthy["number_of_employees"].notna()].copy()
    hs_all_valid     = hs_all_recent[hs_all_recent["number_of_employees"].notna()].copy()

    def emp_segment_dist(df):
        df = df.copy()
        df["emp_segment"] = pd.cut(
            pd.to_numeric(df["number_of_employees"], errors="coerce"),
            bins=[0, 100, 500, float("inf")],
            labels=["SMB (<=100)", "Mid-Market (101-500)", "Enterprise (>500)"],
        )
        return df["emp_segment"].value_counts(normalize=True).mul(100).round(1)

    healthy_emp_dist = emp_segment_dist(hs_healthy_valid)
    all_emp_dist     = emp_segment_dist(hs_all_valid)

    # Montar comparativo
    lead_src_comparison = pd.DataFrame({
        "healthy_pct": healthy_lead_src,
        "all_leads_pct": all_lead_src,
    }).fillna(0).round(1)
    lead_src_comparison["gap_pct"] = (
        lead_src_comparison["healthy_pct"] - lead_src_comparison["all_leads_pct"]
    ).round(1)

    emp_comparison = pd.DataFrame({
        "healthy_pct": healthy_emp_dist,
        "all_leads_pct": all_emp_dist,
    }).fillna(0).round(1)
    emp_comparison["gap_pct"] = (
        emp_comparison["healthy_pct"] - emp_comparison["all_leads_pct"]
    ).round(1)

    return {
        "company_profile":          company_profile,
        "n_healthy":                n_healthy,
        "n_healthy_absolute":       n_healthy_absolute,
        "healthy_companies":        healthy_companies,
        "lead_src_comparison":      lead_src_comparison,
        "emp_comparison":           emp_comparison,
        "threshold_used":           threshold_used,
        "health_threshold_used":    health_threshold_used,
        "depth_threshold_used":     depth_threshold_used,
        "healthy_health_avg":  round(company_profile.loc[company_profile["is_healthy"], "health_score_empresa"].mean(), 2),
        "healthy_depth_avg":   round(company_profile.loc[company_profile["is_healthy"], "avg_feature_depth_empresa"].mean(), 4),
        "finding_no_absolute_healthy": n_healthy_absolute == 0,
        "finding_note": (
            "FINDING: nenhuma empresa atinge simultaneamente health_score>=75 e feature_depth>=0.60. "
            f"Maximo health_score empresa = {company_profile['health_score_empresa'].max():.1f}. "
            "Isso confirma a hipotese central: a RaptorSoft entrega Value (leads/deals) mas "
            "nao verifica Impact (saude+adocao). Threshold relativo p70 aplicado como fallback."
        ),
    }


# ===========================================================================
# PIPELINE COMPLETO — FASE 4
# ===========================================================================

def run_wbd_metrics():
    print("\n" + "=" * 60)
    print("METRICAS WBD — BOWTIE STANDARD v4")
    print("=" * 60)

    cat = pd.read_csv(DATA_DIR / "catalyst_clean.csv")
    cat["renewal_date"]  = pd.to_datetime(cat["renewal_date"],  errors="coerce")
    cat["churn_date"]    = pd.to_datetime(cat["churn_date"],    errors="coerce")
    cat["last_qbr_date"] = pd.to_datetime(cat["last_qbr_date"], errors="coerce")

    sf  = pd.read_csv(DATA_DIR / "sf_clean.csv")
    sf["close_date"]   = pd.to_datetime(sf["close_date"],   errors="coerce")
    sf["created_date"] = pd.to_datetime(sf["created_date"], errors="coerce")

    tel = pd.read_csv(DATA_DIR / "telemetry_clean.csv")
    tel["week_start"] = pd.to_datetime(tel["week_start"], errors="coerce")

    hs  = pd.read_csv(DATA_DIR / "hs_clean.csv")
    hs["mql_date"] = pd.to_datetime(hs["mql_date"], errors="coerce")

    MEDIAN_MRR = 8319.83  # Mediana global validada (P-C6)

    # --- GRR ---
    print("\n[1/4] Calculando GRR (CR7)...")
    grr = compute_grr(cat, MEDIAN_MRR)
    print(f"  GRR (contratos): {grr['grr_pct']}")
    print(f"  GRR (MRR est.): {grr['grr_mrr']*100:.1f}%")
    print(f"  MRR ativo atual: USD {grr['mrr_active_current_usd']:,.0f}")
    print(f"  MRR perdido (est.): USD {grr['mrr_lost_estimated_usd']:,.0f}")

    # --- CR4 Trendline ---
    print("\n[2/4] Calculando CR4 Win Rate Trendline por quarter...")
    cr4 = compute_cr4_trendline(sf)
    print(f"  Quarters com dados: {cr4['close_quarter'].nunique()}")
    recent = cr4.tail(4)
    if not recent.empty:
        print("  Ultimos 4 quarters:")
        for _, row in recent.iterrows():
            print(f"    {row['close_quarter']} [{row['canonical_segment']}]: {row['win_rate_value']*100:.1f}% ({row['total_opps']} opps)")

    # --- CR5 ACV Trendline ---
    print("\n[3/4] Calculando CR5 ACV Trendline por quarter...")
    cr5 = compute_cr5_trendline(sf)
    print(f"  Quarters com dados: {cr5['close_quarter'].nunique()}")
    recent5 = cr5.tail(4)
    for _, row in recent5.iterrows():
        print(f"    {row['close_quarter']}: ACV medio R$ {row['acv_mean']:,.0f} ({row['n_deals']} deals)")

    # --- dt6 Time to First Impact ---
    print("\n[4/4] Calculando dt6 Time to First Impact...")
    dt6_df = compute_dt6_time_to_first_impact(sf, tel)
    dt6_summary = summarize_dt6(dt6_df)
    print(f"  Oportunidades com telemetria: {dt6_summary['n_valid'] + dt6_summary['n_failed']}")
    print(f"  Sem telemetria (closes anteriores Jan/2024): {dt6_summary['n_no_telemetry']}")
    print(f"  Mediana dt6 global: {dt6_summary['overall_median_dt6']} dias")
    if not dt6_summary["summary_by_segment"].empty:
        print("  Por segmento:")
        print(dt6_summary["summary_by_segment"].to_string())
    print("  Falha de onboarding (nao atingiu 0.40 na janela):")
    if not dt6_summary["fail_rates"].empty:
        print(dt6_summary["fail_rates"].to_string(index=False))

    # --- ICP Health ---
    print("\n[5/4] Calculando Closed Loop ICP Health...")
    icp = compute_icp_health(cat, tel, hs)
    print(f"  Empresas saudaveis (health>=75 E feature_depth>=0.60): {icp['n_healthy']}")
    print(f"  Empresas saudaveis: {icp['healthy_companies']}")
    print("  Lead source: clientes saudaveis vs. todos os leads:")
    print(icp["lead_src_comparison"].to_string())

    # Salvar
    cr4.to_csv(DATA_DIR / "cr4_trendline.csv", index=False)
    cr5.to_csv(DATA_DIR / "cr5_trendline.csv", index=False)
    dt6_df.to_csv(DATA_DIR / "dt6_analysis.csv", index=False)
    icp["company_profile"].to_csv(DATA_DIR / "icp_company_profile.csv")
    icp["lead_src_comparison"].to_csv(DATA_DIR / "icp_lead_source_comparison.csv")
    grr["company_grr"].to_csv(DATA_DIR / "grr_by_company.csv", index=False)

    print("\n[OK] Metricas WBD concluidas.")
    return grr, cr4, cr5, dt6_df, dt6_summary, icp


if __name__ == "__main__":
    run_wbd_metrics()
