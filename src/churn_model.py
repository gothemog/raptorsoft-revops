"""
churn_model.py — Fase 2: Modelo de Churn Risk em 2 Camadas + Backtesting
Plano Mestre v4 — RaptorSoft RevOps Case, Secao 8

Arquitetura:
  Camada 1 — Contract_Risk (dados do Catalyst, nivel contrato)
  Camada 2 — Company_Engagement_Modifier (dados de Telemetria, nivel empresa)
  Score final = min(1.0, Contract_Risk x Company_Engagement_Modifier)
  Backtesting: recall sobre os 189 contratos Churned (NOVO v4)
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils import (
    REFERENCE_DATE, CANONICAL_NAMES,
    health_trend_to_score, get_renewal_proximity_score,
    get_nps_bonus,
)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"


# ===========================================================================
# CAMADA 2 — COMPANY ENGAGEMENT MODIFIER (telemetria, nivel empresa)
# Calculado PRIMEIRO pois o modificador vem de dados externos ao Catalyst
# ===========================================================================

def compute_company_engagement(tel: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Company_Engagement_Modifier por empresa usando as ultimas 4 semanas
    de telemetria disponiveis (Secao 8, Camada 2).

    Retorna DataFrame indexado por canonical_name com:
    - avg_feature_depth, avg_sessions, last_login_recent
    - engagement_score, company_engagement_modifier
    """
    results = []

    for company in CANONICAL_NAMES:
        comp_tel = tel[tel["canonical_name"] == company].copy()

        if comp_tel.empty:
            # Sem telemetria: modifier neutro (sem informacao)
            results.append({
                "canonical_name": company,
                "avg_feature_depth": np.nan,
                "avg_sessions": np.nan,
                "last_login_recent": False,
                "engagement_score": np.nan,
                "company_engagement_modifier": 1.0,
                "low_engagement_absolute": False,
            })
            continue

        # Ultimas 4 semanas disponiveis
        last4 = comp_tel.nlargest(4, "week_start")

        avg_feature_depth = last4["feature_depth_score"].mean()
        avg_sessions      = last4["sessions_total"].mean()

        # last_login_recent: last_login_days_ago <= 14 na semana mais recente
        most_recent_row   = comp_tel.nlargest(1, "week_start").iloc[0]
        last_login_recent = bool(most_recent_row["last_login_days_ago"] <= 14)

        # Engagement score (Secao 8, Camada 2)
        engagement_score = (
            0.50 * avg_feature_depth
            + 0.30 * min(1.0, avg_sessions / 100.0)
            + 0.20 * (1.0 if last_login_recent else 0.0)
        )

        # Modifier: 0.70 a 1.40
        # engagement=0.0 -> 1.40 (amplifica 40%) | =1.0 -> 0.70 (reduz 30%)
        modifier = 1.40 - (0.70 * engagement_score)
        modifier = max(0.70, min(1.40, modifier))

        # DETRACTOR: low engagement absoluto
        low_abs = bool(avg_feature_depth < 0.20 and avg_sessions < 10)
        if low_abs:
            modifier = max(modifier, 1.30)

        results.append({
            "canonical_name":             company,
            "avg_feature_depth":          round(avg_feature_depth, 4),
            "avg_sessions":               round(avg_sessions, 2),
            "last_login_recent":          last_login_recent,
            "engagement_score":           round(engagement_score, 4),
            "company_engagement_modifier": round(modifier, 4),
            "low_engagement_absolute":    low_abs,
        })

    return pd.DataFrame(results).set_index("canonical_name")


# ===========================================================================
# CAMADA 1 — CONTRACT RISK (Catalyst, nivel contrato)
# ===========================================================================

def compute_contract_risk(cat: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Contract_Risk para cada contrato do Catalyst (Secao 8, Camada 1).

    Componentes:
      W1 (40%): health_score invertido normalizado
      W2 (25%): health_trend score
      W4 (10%): support_pressure (normalizada)
      W5 (25%): renewal_proximity_score
      BONUS_nps: ajuste de NPS (+/-0.08/0.10)
      DETRACTOR_no_qbr: +0.08

    Denominador dinamico quando componentes tem NULL.
    """
    df = cat.copy()

    # W1: health_score invertido (40%)
    df["w1_raw"]    = (100 - df["health_score"]) / 100.0
    df["w1_valid"]  = df["health_score"].notna()

    # W2: health_trend score (25%) — ja calculado no data_treatment
    # health_trend_score: Declining=1.0, Stable=0.3, Improving=0.0, NULL=0.5
    df["w2_raw"]   = df["health_trend_score"]
    df["w2_valid"] = True  # NULL ja virou 0.50 neutro

    # W4: support_pressure (10%) — ja normalizada no data_treatment
    df["w4_raw"]   = df["support_pressure"].fillna(0.0)
    df["w4_valid"] = True

    # W5: renewal_proximity_score (25%) — ja calculado no data_treatment
    df["w5_raw"]   = df["renewal_proximity_score"].fillna(0.0)
    df["w5_valid"] = True

    # Pesos originais
    weights = {"w1": 0.40, "w2": 0.25, "w4": 0.10, "w5": 0.25}

    # Denominador dinamico: redistribuir pesos quando W1 e NULL
    def compute_weighted_risk(row):
        active_weights = {}
        active_weights["w2"] = weights["w2"]
        active_weights["w4"] = weights["w4"]
        active_weights["w5"] = weights["w5"]

        if row["w1_valid"]:
            active_weights["w1"] = weights["w1"]

        total_weight = sum(active_weights.values())
        if total_weight == 0:
            return 0.5

        score = 0.0
        for comp, w in active_weights.items():
            score += (w / total_weight) * row[f"{comp}_raw"]
        return score

    df["contract_risk_base"] = df.apply(compute_weighted_risk, axis=1)

    # Ajustes: BONUS_nps + DETRACTOR_no_qbr
    df["contract_risk"] = (
        df["contract_risk_base"]
        + df["nps_bonus"]
        + df["no_qbr_signal"].astype(float) * 0.08
    ).clip(0.0, 1.0)

    return df


# ===========================================================================
# SCORE FINAL + CLASSIFICACAO
# ===========================================================================

def compute_final_score(
    cat_risk: pd.DataFrame,
    company_engagement: pd.DataFrame,
    threshold_alto: float = 0.65,
    threshold_medio: float = 0.35,
) -> pd.DataFrame:
    """
    Combina Contract_Risk x Company_Engagement_Modifier -> Churn_Risk_Score.
    Classifica em Alto / Medio / Baixo.
    """
    df = cat_risk.copy()

    # Join com engagement modifier
    df = df.join(
        company_engagement[["company_engagement_modifier", "engagement_score",
                             "avg_feature_depth", "avg_sessions"]],
        on="canonical_name",
        how="left",
    )

    # Modifier padrao = 1.0 quando nao disponivel
    df["company_engagement_modifier"] = df["company_engagement_modifier"].fillna(1.0)

    # Score final
    df["churn_risk_score"] = (
        df["contract_risk"] * df["company_engagement_modifier"]
    ).clip(0.0, 1.0).round(4)

    # Classificacao
    def classify_risk(score):
        if pd.isna(score):
            return "Sem dados"
        if score >= threshold_alto:
            return "Alto"
        elif score >= threshold_medio:
            return "Medio"
        else:
            return "Baixo"

    df["risk_level"] = df["churn_risk_score"].apply(classify_risk)
    df["threshold_alto"]  = threshold_alto
    df["threshold_medio"] = threshold_medio

    return df


# ===========================================================================
# BACKTESTING (NOVO v4) — Recall sobre contratos Churned
# ===========================================================================

def run_backtesting(cat_scored: pd.DataFrame) -> dict:
    """
    Calcula recall do modelo sobre os 189 contratos Churned (Secao 8, NOVO v4).
    O backtesting e indicativo (snapshot pos-fato — documentar como limitacao).

    Retorna dict com:
    - n_churned, recall_alto, recall_alto_medio
    - threshold sugerido se recall_alto < 40%
    """
    churned = cat_scored[cat_scored["status"] == "Churned"].copy()
    n_churned = len(churned)

    n_alto       = (churned["risk_level"] == "Alto").sum()
    n_alto_medio = churned["risk_level"].isin(["Alto", "Medio"]).sum()

    recall_alto       = round(n_alto / n_churned, 4) if n_churned > 0 else 0
    recall_alto_medio = round(n_alto_medio / n_churned, 4) if n_churned > 0 else 0

    # Sugerir threshold se recall_alto < 40%
    threshold_suggested = None
    if recall_alto < 0.40:
        # Encontrar threshold que capture >= 40% dos churned
        churned_scores = churned["churn_risk_score"].dropna().sort_values(ascending=False)
        target_idx = max(0, int(np.ceil(0.40 * n_churned)) - 1)
        if target_idx < len(churned_scores):
            threshold_suggested = round(churned_scores.iloc[target_idx], 4)

    result = {
        "n_churned":             n_churned,
        "n_alto":                int(n_alto),
        "n_alto_medio":          int(n_alto_medio),
        "recall_alto":           recall_alto,
        "recall_alto_medio":     recall_alto_medio,
        "recall_alto_pct":       f"{recall_alto*100:.1f}%",
        "recall_alto_medio_pct": f"{recall_alto_medio*100:.1f}%",
        "meets_alto_target":     recall_alto >= 0.40,
        "meets_combo_target":    recall_alto_medio >= 0.70,
        "threshold_suggested":   threshold_suggested,
        "note": (
            "Backtesting indicativo: snapshot pos-fato. "
            "Contratos Churned podem ter health_score/trend refletindo estado pos-churn. "
            "Ver Data Treatment Report."
        ),
    }

    print("\n  === Backtesting — Recall sobre contratos Churned ===")
    print(f"  Contratos Churned: {n_churned}")
    print(f"  Recall Alto: {result['recall_alto_pct']} ({n_alto}/{n_churned}) — target >= 40%: {'OK' if result['meets_alto_target'] else 'ABAIXO'}")
    print(f"  Recall Alto+Medio: {result['recall_alto_medio_pct']} ({n_alto_medio}/{n_churned}) — target >= 70%: {'OK' if result['meets_combo_target'] else 'ABAIXO'}")
    if threshold_suggested:
        print(f"  Threshold sugerido para recall_alto >= 40%: {threshold_suggested}")

    return result


# ===========================================================================
# PIPELINE COMPLETO — FASE 2
# ===========================================================================

def run_churn_model(threshold_alto=0.65, threshold_medio=0.35):
    print("\n" + "=" * 60)
    print("CHURN RISK MODEL — 2 CAMADAS + BACKTESTING")
    print("=" * 60)

    # Carregar dados tratados
    cat = pd.read_csv(DATA_DIR / "catalyst_clean.csv")
    tel = pd.read_csv(DATA_DIR / "telemetry_clean.csv")
    tel["week_start"] = pd.to_datetime(tel["week_start"], errors="coerce")

    # Camada 2 primeiro (engagement modifier por empresa)
    print("\n[1/4] Calculando Company Engagement Modifier (Camada 2 — Telemetria)...")
    company_engagement = compute_company_engagement(tel)
    print(f"  Modifier range: {company_engagement['company_engagement_modifier'].min():.3f} — {company_engagement['company_engagement_modifier'].max():.3f}")
    print(f"  Empresas com low engagement absoluto: {company_engagement['low_engagement_absolute'].sum()}")

    # Camada 1 (contract risk por contrato)
    print("\n[2/4] Calculando Contract Risk (Camada 1 — Catalyst)...")
    cat_risk = compute_contract_risk(cat)
    print(f"  Contract_Risk range: {cat_risk['contract_risk'].min():.3f} — {cat_risk['contract_risk'].max():.3f}")
    print(f"  Contratos com health_score NULL: {cat_risk['health_score'].isna().sum()} (denominador dinamico aplicado)")

    # Score final + classificacao
    print(f"\n[3/4] Calculando score final e classificando (thresholds: Alto>={threshold_alto}, Medio>={threshold_medio})...")
    cat_scored = compute_final_score(cat_risk, company_engagement, threshold_alto, threshold_medio)

    # Distribuicao por nivel
    dist = cat_scored[cat_scored["status"] == "Active"]["risk_level"].value_counts()
    print("  Distribuicao nos contratos Active:")
    for level in ["Alto", "Medio", "Baixo"]:
        n = dist.get(level, 0)
        pct = n / (cat_scored["status"] == "Active").sum() * 100
        mrr = cat_scored.loc[(cat_scored["status"] == "Active") & (cat_scored["risk_level"] == level), "mrr_usd"].sum()
        print(f"    {level}: {n} contratos ({pct:.1f}%) — MRR em risco: USD {mrr:,.0f}")

    # Backtesting
    print("\n[4/4] Executando backtesting...")
    backtest_results = run_backtesting(cat_scored)

    # Ajuste automatico de threshold se recall insuficiente
    if not backtest_results["meets_alto_target"] and backtest_results["threshold_suggested"]:
        new_threshold = backtest_results["threshold_suggested"]
        print(f"\n  Recall abaixo de 40% — recalculando com threshold sugerido: {new_threshold}")
        cat_scored = compute_final_score(cat_risk, company_engagement, new_threshold, threshold_medio)
        backtest_results = run_backtesting(cat_scored)
        backtest_results["threshold_adjusted"] = True
        backtest_results["original_threshold_alto"] = threshold_alto
    else:
        backtest_results["threshold_adjusted"] = False

    # Salvar
    cat_scored.to_csv(DATA_DIR / "catalyst_scored.csv", index=False)
    company_engagement.to_csv(DATA_DIR / "company_engagement.csv")

    print("\n[OK] Churn model concluido.")
    return cat_scored, company_engagement, backtest_results


if __name__ == "__main__":
    run_churn_model()
