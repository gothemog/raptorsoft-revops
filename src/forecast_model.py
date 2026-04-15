"""
forecast_model.py — Fase 3: Modelo de Forecast com Slippage Progressivo
Plano Mestre v4 — RaptorSoft RevOps Case, Secao 9

Etapas:
  1. Win rates por canonical_segment (CR4 WBD)
  2. Slippage discount progressivo por aging
  3. Projecao do quarter (excluindo omit e amounts negativos)
  4. Feedback loop telemetria -> forecast (opcional v4)
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils import (
    REFERENCE_DATE, OPEN_STAGES,
    CLOSED_WON_STAGE, CLOSED_LOST_STAGE,
    get_slippage_discount,
)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

# Quarter atual (P-D4)
CURRENT_QUARTER = "2025Q2"  # Abril–Junho 2025


# ===========================================================================
# WIN RATES (CR4 WBD)
# ===========================================================================

def compute_win_rates(sf: pd.DataFrame) -> dict:
    """
    Calcula win rates por canonical_segment sobre oportunidades fechadas
    que passaram de Discovery/Qualification (Secao 9, Passo 1).

    Win rate = Closed Won / (Closed Won + Closed Lost)
    Calculado por valor (amount_brl) e por contagem.
    """
    # Filtrar fechadas com amount valido
    closed = sf[
        sf["stage_canonical"].isin([CLOSED_WON_STAGE, CLOSED_LOST_STAGE])
        & sf["amount_brl"].notna()
        & ~sf["is_negative_amount"]
    ].copy()

    win_rates = {}
    segments = closed["canonical_segment"].dropna().unique()

    for seg in segments:
        seg_data = closed[closed["canonical_segment"] == seg]
        won  = seg_data[seg_data["stage_canonical"] == CLOSED_WON_STAGE]
        lost = seg_data[seg_data["stage_canonical"] == CLOSED_LOST_STAGE]

        total_won_val  = won["amount_brl"].sum()
        total_lost_val = lost["amount_brl"].sum()
        total_val      = total_won_val + total_lost_val

        total_won_count  = len(won)
        total_lost_count = len(lost)
        total_count      = total_won_count + total_lost_count

        win_rates[seg] = {
            "win_rate_value": round(total_won_val / total_val, 4) if total_val > 0 else 0.0,
            "win_rate_count": round(total_won_count / total_count, 4) if total_count > 0 else 0.0,
            "n_won":   total_won_count,
            "n_lost":  total_lost_count,
            "total_won_brl":  round(total_won_val, 2),
        }

    # Fallback global (para segmentos com poucos dados)
    all_won  = closed[closed["stage_canonical"] == CLOSED_WON_STAGE]
    all_lost = closed[closed["stage_canonical"] == CLOSED_LOST_STAGE]
    total_all = all_won["amount_brl"].sum() + all_lost["amount_brl"].sum()
    win_rates["global"] = {
        "win_rate_value": round(all_won["amount_brl"].sum() / total_all, 4) if total_all > 0 else 0.0,
        "win_rate_count": round(len(all_won) / (len(all_won) + len(all_lost)), 4) if (len(all_won) + len(all_lost)) > 0 else 0.0,
        "n_won":  len(all_won),
        "n_lost": len(all_lost),
    }

    return win_rates


# ===========================================================================
# FORECAST PROGRESSIVO (Secao 9, Passos 2-3)
# ===========================================================================

def compute_forecast(
    sf: pd.DataFrame,
    win_rates: dict,
    company_engagement: pd.DataFrame = None,
    apply_engagement_feedback: bool = False,
) -> pd.DataFrame:
    """
    Calcula forecast contribution para cada oportunidade aberta.

    Exclusoes:
    - forecast_category = 'omit'
    - amount_brl < 0 (estornos)
    - Stages fechados (Closed Won / Closed Lost)

    Retorna df com:
    - slippage_days, slippage_discount, adjusted_win_rate, forecast_contribution
    """
    # Filtrar oportunidades elegiveis para forecast
    eligible = sf[
        sf["is_open"]
        & (sf["forecast_category_canonical"] != "omit")
        & ~sf["is_negative_amount"]
        & sf["amount_brl"].notna()
        & (sf["amount_brl"] > 0)
    ].copy()

    # Slippage discount progressivo
    eligible["slippage_discount"] = eligible["slippage_days"].apply(
        lambda d: get_slippage_discount(d) if pd.notna(d) else 0.0
    )

    # Engagement feedback (NOVO v4 — opcional/experimental)
    if apply_engagement_feedback and company_engagement is not None:
        eng_mod = company_engagement["company_engagement_modifier"].to_dict()
        eligible["engagement_modifier_for_forecast"] = eligible["canonical_name"].map(eng_mod).fillna(1.0)

        eligible["slippage_discount"] = eligible.apply(
            lambda row: get_slippage_discount(row["slippage_days"], row["engagement_modifier_for_forecast"]),
            axis=1,
        )
    else:
        eligible["engagement_modifier_for_forecast"] = np.nan

    # Win rate base por canonical_segment
    def get_win_rate(segment):
        if segment in win_rates:
            return win_rates[segment]["win_rate_value"]
        return win_rates.get("global", {}).get("win_rate_value", 0.50)

    eligible["win_rate_base"] = eligible["canonical_segment"].apply(get_win_rate)

    # Adjusted win rate = win_rate_base * (1 - slippage_discount)
    eligible["adjusted_win_rate"] = (
        eligible["win_rate_base"] * (1 - eligible["slippage_discount"])
    ).round(4)

    # Forecast contribution
    eligible["forecast_contribution"] = (
        eligible["amount_brl"] * eligible["adjusted_win_rate"]
    ).round(2)

    return eligible


# ===========================================================================
# SUMARIO DO FORECAST (Secao 9, Passo 3)
# ===========================================================================

def build_forecast_summary(forecast_df: pd.DataFrame, win_rates: dict) -> dict:
    """
    Agrega forecast total e por segmento.
    Reporta pipeline coverage e gap vs. meta.
    """
    total_forecast    = forecast_df["forecast_contribution"].sum()
    total_pipeline    = forecast_df["amount_brl"].sum()
    n_opps            = len(forecast_df)

    # Por faixa de slippage
    bins   = [0, 30, 60, 90, 180, float("inf")]
    labels = ["<=30d", "31-60d", "61-90d", "91-180d", ">180d"]
    forecast_df["slippage_bucket"] = pd.cut(
        forecast_df["slippage_days"], bins=bins, labels=labels
    )
    by_slippage = (
        forecast_df.groupby("slippage_bucket", observed=True)
        .agg(n_opps=("opportunity_id", "count"), pipeline_brl=("amount_brl", "sum"),
             forecast_brl=("forecast_contribution", "sum"))
        .round(2)
    )

    # Por forecast_category
    by_category = (
        forecast_df.groupby("forecast_category_canonical")
        .agg(n_opps=("opportunity_id", "count"), pipeline_brl=("amount_brl", "sum"),
             forecast_brl=("forecast_contribution", "sum"))
        .round(2)
    )

    return {
        "total_forecast_brl":   round(total_forecast, 2),
        "total_pipeline_brl":   round(total_pipeline, 2),
        "n_eligible_opps":      n_opps,
        "win_rates":            win_rates,
        "by_slippage":          by_slippage,
        "by_category":          by_category,
    }


# ===========================================================================
# PIPELINE COMPLETO — FASE 3
# ===========================================================================

def run_forecast_model():
    print("\n" + "=" * 60)
    print("FORECAST MODEL — SLIPPAGE PROGRESSIVO v4")
    print("=" * 60)

    sf  = pd.read_csv(DATA_DIR / "sf_clean.csv")
    sf["close_date"]   = pd.to_datetime(sf["close_date"],   errors="coerce")
    sf["created_date"] = pd.to_datetime(sf["created_date"], errors="coerce")

    company_engagement = pd.read_csv(DATA_DIR / "company_engagement.csv", index_col="canonical_name")

    # Passo 1: Win rates
    print("\n[1/3] Calculando win rates por canonical_segment...")
    win_rates = compute_win_rates(sf)
    for seg, wr in win_rates.items():
        if seg == "global":
            print(f"  Global: {wr['win_rate_value']*100:.1f}% (n_won={wr['n_won']}, n_lost={wr['n_lost']})")
        else:
            print(f"  {seg}: {wr['win_rate_value']*100:.1f}% value | {wr['win_rate_count']*100:.1f}% count (n={wr['n_won']+wr['n_lost']})")

    # Passo 2-3: Forecast com slippage progressivo
    print("\n[2/3] Calculando forecast com slippage progressivo...")
    forecast_df = compute_forecast(sf, win_rates, company_engagement, apply_engagement_feedback=False)
    print(f"  Oportunidades elegiveis: {len(forecast_df)}")
    print(f"  Pipeline total elegivel: R$ {forecast_df['amount_brl'].sum():,.2f}")
    print(f"  Forecast total ajustado: R$ {forecast_df['forecast_contribution'].sum():,.2f}")

    # Distribuicao por slippage bucket
    bins   = [0, 30, 60, 90, 180, float("inf")]
    labels = ["<=30d", "31-60d", "61-90d", "91-180d", ">180d"]
    forecast_df["slippage_bucket"] = pd.cut(forecast_df["slippage_days"], bins=bins, labels=labels)
    print("\n  Distribuicao por aging de slippage:")
    for bucket, grp in forecast_df.groupby("slippage_bucket", observed=True):
        disc = grp["slippage_discount"].mean()
        print(f"    {bucket}: {len(grp)} opps | discount medio {disc*100:.0f}% | forecast R$ {grp['forecast_contribution'].sum():,.0f}")

    print("\n[3/3] Salvando resultados...")
    forecast_df.to_csv(DATA_DIR / "sf_forecast.csv", index=False)

    summary = build_forecast_summary(forecast_df, win_rates)

    print("\n[OK] Forecast model concluido.")
    return forecast_df, win_rates, summary


if __name__ == "__main__":
    run_forecast_model()
