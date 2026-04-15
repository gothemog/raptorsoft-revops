"""
app.py — Dashboard Interativo RaptorSoft RevOps
Plano Mestre v4 — Pipeline Health Intelligence & Churn Risk Signal

Secoes:
  A — Pipeline Health Overview
  B — Churn Risk Signal
  C — Simulador de Cenarios
  D — Bowtie View (4 metricas WBD)
"""

import sys
import warnings
warnings.filterwarnings("ignore")

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from src.utils import (
    REFERENCE_DATE, CANONICAL_NAMES, DOMAIN_TO_CANONICAL,
    get_slippage_discount, OPEN_STAGES,
)

# ===========================================================================
# CONFIGURACAO DA PAGINA
# ===========================================================================

st.set_page_config(
    page_title="RaptorSoft RevOps — Pipeline & Churn Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

BRAND_COLORS = {
    "alto":   "#E53935",
    "medio":  "#FB8C00",
    "baixo":  "#43A047",
    "neutral":"#1565C0",
    "bg":     "#0E1117",
    "card":   "#1E2130",
}


# ===========================================================================
# CARGA DE DADOS (cacheada)
# ===========================================================================

@st.cache_data(ttl=300)
def load_data():
    sf       = pd.read_csv(DATA_DIR / "sf_clean.csv",         parse_dates=["close_date", "created_date"])
    catalyst = pd.read_csv(DATA_DIR / "catalyst_scored.csv",  parse_dates=["renewal_date", "churn_date", "last_qbr_date"])
    tel      = pd.read_csv(DATA_DIR / "telemetry_clean.csv",  parse_dates=["week_start"])
    hs       = pd.read_csv(DATA_DIR / "hs_clean.csv",         parse_dates=["mql_date"])
    master   = pd.read_csv(DATA_DIR / "master_accounts.csv",  index_col="canonical_name")
    cr4      = pd.read_csv(DATA_DIR / "cr4_trendline.csv")
    cr5      = pd.read_csv(DATA_DIR / "cr5_trendline.csv")
    dt6      = pd.read_csv(DATA_DIR / "dt6_analysis.csv",     parse_dates=["close_date", "first_impact_date"])
    icp      = pd.read_csv(DATA_DIR / "icp_company_profile.csv", index_col="canonical_name")
    icp_src  = pd.read_csv(DATA_DIR / "icp_lead_source_comparison.csv", index_col="lead_source_canonical")
    grr_co   = pd.read_csv(DATA_DIR / "grr_by_company.csv")
    forecast = pd.read_csv(DATA_DIR / "sf_forecast.csv",      parse_dates=["close_date", "created_date"])
    eng      = pd.read_csv(DATA_DIR / "company_engagement.csv", index_col="canonical_name")
    return sf, catalyst, tel, hs, master, cr4, cr5, dt6, icp, icp_src, grr_co, forecast, eng

sf, catalyst, tel, hs, master, cr4, cr5, dt6, icp, icp_src, grr_co, forecast, eng = load_data()

MEDIAN_MRR_USD = 8319.83


# ===========================================================================
# SIDEBAR
# ===========================================================================

st.sidebar.image("https://img.icons8.com/color/96/bar-chart.png", width=50)
st.sidebar.title("RaptorSoft RevOps")
st.sidebar.caption("Pipeline Health Intelligence & Churn Risk Signal")
st.sidebar.markdown("---")

secao = st.sidebar.radio(
    "Navegar para:",
    ["A — Pipeline Health", "B — Churn Risk Signal",
     "C — Simulador de Cenarios", "D — Bowtie View"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.markdown(f"**Data de referencia:** {REFERENCE_DATE.date()}")
st.sidebar.markdown("**Taxa cambial:** R$ 5,70 / USD")
st.sidebar.markdown("**Quarter atual:** Q2 2025")
st.sidebar.markdown("**Empresas:** 20 | **Contratos:** 680")
st.sidebar.markdown("""
---
**Legenda de join:**
- 🟢 HIGH — chave tecnica
- 🟡 MEDIUM — canonical_name
""")

# ===========================================================================
# METRICAS GLOBAIS (header compartilhado)
# ===========================================================================

active     = catalyst[catalyst["status"] == "Active"]
churned    = catalyst[catalyst["status"] == "Churned"]
mrr_total  = active["mrr_usd"].sum()
mrr_risk   = active[active["risk_level"].isin(["Alto", "Medio"])]["mrr_usd"].sum()
grr_pct    = len(active) / (len(active) + len(churned))

open_pipeline = sf[sf["is_open"] & ~sf["is_negative_amount"] & (sf["amount_brl"] > 0)]
total_pipeline_brl = open_pipeline["amount_brl"].sum()
total_forecast_brl = forecast["forecast_contribution"].sum()

st.markdown("""
<style>
.metric-card {
    background: #1E2130;
    border-radius: 10px;
    padding: 16px 20px;
    margin: 4px;
    border-left: 4px solid #1565C0;
}
.metric-value { font-size: 1.8rem; font-weight: 700; color: #FFFFFF; }
.metric-label { font-size: 0.8rem; color: #AAAAAA; text-transform: uppercase; letter-spacing: 0.05em; }
</style>
""", unsafe_allow_html=True)


# ===========================================================================
# SECAO A — PIPELINE HEALTH OVERVIEW
# ===========================================================================

if secao == "A — Pipeline Health":
    st.title("📊 Seção A — Pipeline Health Overview")
    st.caption("Lado esquerdo do Bowtie: VM3→VM5, CR4, aging de pipeline")

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Pipeline Total (aberto)", f"R$ {total_pipeline_brl/1e6:.1f}M",
                  help="Oportunidades abertas, excluindo omit e amounts negativos")
    with col2:
        st.metric("Forecast Ajustado Q2/25", f"R$ {total_forecast_brl/1e6:.1f}M",
                  delta=f"{(total_forecast_brl/total_pipeline_brl*100):.0f}% do pipeline",
                  help="Pipeline com desconto progressivo por aging de slippage")
    with col3:
        n_slippage = sf["is_slippage"].sum()
        st.metric("Em Slippage", f"{n_slippage}",
                  delta="100% do pipeline aberto", delta_color="inverse",
                  help="Oportunidades com stage aberto e close_date no passado")
    with col4:
        n_commit = sf[(sf["forecast_category_canonical"] == "commit") & sf["is_open"]].shape[0]
        commit_val = sf.loc[(sf["forecast_category_canonical"] == "commit") & sf["is_open"], "amount_brl"].sum()
        st.metric("Commit em Aberto", f"{n_commit} opps",
                  delta=f"R$ {commit_val/1e6:.1f}M",
                  help="Oportunidades commit ainda sem fechamento")
    with col5:
        n_neg = sf["is_negative_amount"].sum()
        neg_val = sf.loc[sf["is_negative_amount"], "amount_brl"].sum()
        st.metric("Estornos (amount < 0)", f"{n_neg}",
                  delta=f"R$ {neg_val/1e6:.2f}M",
                  delta_color="inverse",
                  help="Registros com amount_brl < 0 — excluidos do forecast")

    st.markdown("---")

    col_left, col_right = st.columns([1.2, 1])

    # Pipeline por stage
    with col_left:
        st.subheader("Distribuição do Pipeline por Stage e Forecast Category")
        stage_order = ["Prospecting", "Discovery/Qualification", "Proposal", "Negotiation",
                       "Closed Won", "Closed Lost"]
        pipeline_stage = (
            sf[~sf["is_negative_amount"]]
            .groupby(["stage_canonical", "forecast_category_canonical"])
            .agg(n_opps=("opportunity_id", "count"), total_brl=("amount_brl", "sum"))
            .reset_index()
        )
        pipeline_stage["stage_canonical"] = pd.Categorical(
            pipeline_stage["stage_canonical"], categories=stage_order, ordered=True
        )
        pipeline_stage = pipeline_stage.sort_values("stage_canonical")

        fig_stage = px.bar(
            pipeline_stage,
            x="stage_canonical", y="total_brl",
            color="forecast_category_canonical",
            title="Pipeline por Stage (R$)",
            labels={"stage_canonical": "Stage", "total_brl": "Valor (R$)",
                    "forecast_category_canonical": "Forecast Category"},
            color_discrete_map={"commit": "#1565C0", "best_case": "#42A5F5",
                                 "pipeline": "#78909C", "omit": "#CFD8DC"},
            height=380,
        )
        fig_stage.update_layout(bargap=0.2, plot_bgcolor="#0E1117", paper_bgcolor="#0E1117",
                                font_color="white")
        st.plotly_chart(fig_stage, use_container_width=True)

    # Slippage heatmap
    with col_right:
        st.subheader("Slippage Heatmap — Aging do Pipeline")
        bins   = [0, 30, 60, 90, 180, float("inf")]
        labels = ["<=30d", "31-60d", "61-90d", "91-180d", ">180d"]
        slippage_df = sf[sf["is_slippage"]].copy()
        slippage_df["slippage_bucket"] = pd.cut(slippage_df["slippage_days"], bins=bins, labels=labels)
        heatmap_data = (
            slippage_df.groupby("slippage_bucket", observed=True)
            .agg(n_opps=("opportunity_id", "count"), pipeline_brl=("amount_brl", "sum"))
            .reset_index()
        )
        heatmap_data["discount_pct"] = heatmap_data["slippage_bucket"].map({
            "<=30d": "15%", "31-60d": "30%", "61-90d": "45%", "91-180d": "65%", ">180d": "85%"
        })

        fig_heat = px.bar(
            heatmap_data, x="slippage_bucket", y="n_opps",
            color="slippage_bucket",
            text=heatmap_data.apply(lambda r: f"{r['n_opps']} opps\nR${r['pipeline_brl']/1e6:.1f}M\nDesc: {r['discount_pct']}", axis=1),
            title="Oportunidades por Faixa de Slippage",
            color_discrete_sequence=["#43A047", "#FDD835", "#FB8C00", "#E53935", "#B71C1C"],
            height=380,
        )
        fig_heat.update_traces(textposition="outside")
        fig_heat.update_layout(showlegend=False, plot_bgcolor="#0E1117",
                               paper_bgcolor="#0E1117", font_color="white",
                               xaxis_title="Aging de Slippage", yaxis_title="Qtd Oportunidades")
        st.plotly_chart(fig_heat, use_container_width=True)

    # Aging detalhado + sinalizacoes de risco
    st.subheader("Sinalização de Oportunidades em Risco")
    st.caption("Oportunidades abertas com indicadores de risco: slippage, stale, commit sem fechamento")

    risk_flags = sf[
        sf["is_open"]
        & (sf["is_slippage"] | sf["is_stale_opportunity"])
        & ~sf["is_negative_amount"]
    ].copy()

    if not risk_flags.empty:
        risk_flags["risk_flags"] = ""
        risk_flags.loc[risk_flags["is_slippage"],          "risk_flags"] += "⏰ Slippage "
        risk_flags.loc[risk_flags["is_stale_opportunity"], "risk_flags"] += "🕸️ Stale "
        risk_flags.loc[risk_flags["forecast_category_canonical"] == "commit", "risk_flags"] += "⚠️ Commit "

        risk_display = risk_flags[[
            "opportunity_id", "canonical_name", "stage_canonical",
            "amount_brl", "close_date", "slippage_days",
            "forecast_category_canonical", "risk_flags"
        ]].rename(columns={
            "opportunity_id": "Opp ID", "canonical_name": "Empresa",
            "stage_canonical": "Stage", "amount_brl": "Valor (R$)",
            "close_date": "Close Date", "slippage_days": "Slippage (dias)",
            "forecast_category_canonical": "Forecast Cat.", "risk_flags": "Flags"
        })
        risk_display["Valor (R$)"] = risk_display["Valor (R$)"].apply(lambda x: f"R$ {x:,.0f}")
        risk_display = risk_display.sort_values("Slippage (dias)", ascending=False)
        st.dataframe(risk_display.head(50), use_container_width=True, height=350)
    else:
        st.info("Nenhuma oportunidade com flags de risco encontrada.")

    # Monitor de Estornos
    st.subheader("Monitor de Estornos (amount_brl < 0)")
    estornos = sf[sf["is_negative_amount"]].copy()
    if not estornos.empty:
        col_e1, col_e2, col_e3 = st.columns(3)
        with col_e1:
            st.metric("Total de Estornos", f"{len(estornos)} registros")
        with col_e2:
            st.metric("Valor Total", f"R$ {estornos['amount_brl'].sum():,.0f}")
        with col_e3:
            st.metric("Empresas afetadas", f"{estornos['canonical_name'].nunique()}")

        estorno_by_company = estornos.groupby("canonical_name")["amount_brl"].sum().reset_index()
        fig_est = px.bar(estorno_by_company.sort_values("amount_brl"),
                         x="amount_brl", y="canonical_name", orientation="h",
                         title="Estornos por Empresa (R$)",
                         color_discrete_sequence=["#E53935"],
                         height=350)
        fig_est.update_layout(plot_bgcolor="#0E1117", paper_bgcolor="#0E1117",
                              font_color="white", xaxis_title="Valor (R$)", yaxis_title="")
        st.plotly_chart(fig_est, use_container_width=True)


# ===========================================================================
# SECAO B — CHURN RISK SIGNAL
# ===========================================================================

elif secao == "B — Churn Risk Signal":
    st.title("🚨 Seção B — Churn Risk Signal")
    st.caption("Lado direito do Bowtie: CR6, CR7 (GRR), Modelo de Churn Risk em 2 Camadas")

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("MRR Total Ativo", f"USD {mrr_total/1e6:.2f}M")
    with col2:
        st.metric("GRR (taxa de sobrevivencia)", f"{grr_pct*100:.1f}%",
                  help="n_active / (n_active + n_churned). MRR pre-churn estimado pela mediana global.")
    with col3:
        mrr_alto = active[active["risk_level"] == "Alto"]["mrr_usd"].sum()
        st.metric("MRR em Risco Alto", f"USD {mrr_alto/1e3:.0f}K",
                  delta=f"{mrr_alto/mrr_total*100:.1f}% do MRR",
                  delta_color="inverse")
    with col4:
        n_renew_60 = active[
            active["days_to_renewal"].notna() & (active["days_to_renewal"] <= 60) & (active["days_to_renewal"] > 0)
        ].shape[0]
        st.metric("Renovacoes nos proximos 60d", f"{n_renew_60} contratos")
    with col5:
        n_imputed = active["mrr_imputed"].sum() if "mrr_imputed" in active.columns else 0
        mrr_imputed = n_imputed * MEDIAN_MRR_USD
        st.metric("MRR Imputado *", f"USD {mrr_imputed/1e3:.0f}K",
                  help="20 contratos Active com mrr_usd=0 receberam imputacao pela mediana global (USD 8,319.83)")

    # Aviso MRR imputado
    if n_imputed > 0:
        st.warning(f"⚠️ **{n_imputed} contratos** com MRR imputado (USD {mrr_imputed:,.0f} total) — marcados com * no dashboard. Ver Data Treatment Report.")

    st.markdown("---")

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("Distribuição por Nivel de Risco (Active)")
        risk_dist = active["risk_level"].value_counts().reset_index()
        risk_dist.columns = ["risk_level", "n_contratos"]
        risk_dist["mrr_usd"] = risk_dist["risk_level"].map(
            active.groupby("risk_level")["mrr_usd"].sum()
        )
        color_map = {"Alto": "#E53935", "Medio": "#FB8C00", "Baixo": "#43A047"}
        fig_risk = px.pie(risk_dist, values="n_contratos", names="risk_level",
                          title="Contratos Active por Nivel de Risco",
                          color="risk_level", color_discrete_map=color_map,
                          hole=0.4)
        fig_risk.update_layout(plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", font_color="white")
        st.plotly_chart(fig_risk, use_container_width=True)

    with col_right:
        st.subheader("MRR em Risco por CSM Owner")
        mrr_csm = (
            active[active["risk_level"].isin(["Alto", "Medio"])]
            .groupby(["csm_owner", "risk_level"])["mrr_usd"]
            .sum()
            .reset_index()
        )
        mrr_csm["csm_owner"] = mrr_csm["csm_owner"].fillna("Nao Atribuido")
        fig_csm = px.bar(mrr_csm, x="mrr_usd", y="csm_owner", color="risk_level",
                         orientation="h",
                         title="MRR em Risco por CSM (Alto + Medio)",
                         color_discrete_map=color_map,
                         height=400)
        fig_csm.update_layout(plot_bgcolor="#0E1117", paper_bgcolor="#0E1117",
                              font_color="white", xaxis_title="MRR (USD)", yaxis_title="")
        st.plotly_chart(fig_csm, use_container_width=True)

    # Timeline de renovacoes
    st.subheader("Timeline de Renovacoes — Proximos 90 dias")
    renew_90 = active[
        active["days_to_renewal"].notna()
        & (active["days_to_renewal"] > 0)
        & (active["days_to_renewal"] <= 90)
    ].copy().sort_values("days_to_renewal")

    if not renew_90.empty:
        renew_90["renewal_urgency"] = renew_90["days_to_renewal"].apply(
            lambda d: "Critico (<=30d)" if d <= 30 else ("Atencao (31-60d)" if d <= 60 else "Monitor (61-90d)")
        )
        col_renew = ["customer_id", "canonical_name", "csm_owner", "mrr_usd",
                     "risk_level", "days_to_renewal", "renewal_date", "renewal_urgency",
                     "health_score", "health_trend", "mrr_imputed"]
        avail_cols = [c for c in col_renew if c in renew_90.columns]
        renew_display = renew_90[avail_cols].rename(columns={
            "customer_id": "Contrato", "canonical_name": "Empresa",
            "csm_owner": "CSM", "mrr_usd": "MRR (USD)", "risk_level": "Risco",
            "days_to_renewal": "Dias p/ Renovacao", "renewal_date": "Data Renovacao",
            "renewal_urgency": "Urgencia", "health_score": "Health Score",
            "health_trend": "Tendencia", "mrr_imputed": "MRR Imputado*",
        })
        renew_display["MRR (USD)"] = renew_display["MRR (USD)"].apply(lambda x: f"USD {x:,.0f}")

        def color_risk(val):
            colors = {"Alto": "background-color: #7f0000", "Medio": "background-color: #7f4000",
                      "Baixo": "background-color: #003a00"}
            return colors.get(val, "")

        st.dataframe(renew_display, use_container_width=True, height=320)
        st.caption(f"{len(renew_90)} contratos com renovacao nos proximos 90 dias | "
                   f"MRR total em jogo: USD {renew_90['mrr_usd'].sum():,.0f}")
    else:
        st.info("Nenhum contrato com renovacao nos proximos 90 dias.")

    # Lista de acao CS
    st.subheader("Lista de Acao para CS — Alto Risco + Renovacao <= 60 dias")
    action_list = active[
        (active["risk_level"] == "Alto")
        & active["days_to_renewal"].notna()
        & (active["days_to_renewal"] <= 60)
        & (active["days_to_renewal"] > 0)
    ].sort_values("days_to_renewal")

    if not action_list.empty:
        st.warning(f"🚨 **{len(action_list)} contratos** requerem acao imediata de CS")
        action_cols = ["customer_id", "canonical_name", "csm_owner", "mrr_usd",
                       "health_score", "health_trend", "days_to_renewal",
                       "churn_risk_score", "contract_risk", "company_engagement_modifier"]
        avail_action_cols = [c for c in action_cols if c in action_list.columns]
        action_display = action_list[avail_action_cols].rename(columns={
            "customer_id": "Contrato", "canonical_name": "Empresa", "csm_owner": "CSM",
            "mrr_usd": "MRR (USD)", "health_score": "Health Score", "health_trend": "Tendencia",
            "days_to_renewal": "Dias p/ Renovacao", "churn_risk_score": "Score Final",
            "contract_risk": "Contract Risk", "company_engagement_modifier": "Eng. Modifier",
        })
        action_display["MRR (USD)"] = action_display["MRR (USD)"].apply(lambda x: f"USD {x:,.0f}")
        st.dataframe(action_display, use_container_width=True)
    else:
        st.success("Nenhum contrato Alto Risco com renovacao nos proximos 60 dias.")

    # Backtesting info
    st.subheader("Poder Discriminativo do Modelo — Backtesting")
    col_bt1, col_bt2, col_bt3 = st.columns(3)
    churned_scored = catalyst[catalyst["status"] == "Churned"]
    n_churned_total = len(churned_scored)
    n_alto_churned  = (churned_scored["risk_level"] == "Alto").sum()
    n_combo_churned = churned_scored["risk_level"].isin(["Alto", "Medio"]).sum()

    with col_bt1:
        st.metric("Contratos Churned (total)", f"{n_churned_total}")
    with col_bt2:
        recall_a = n_alto_churned / n_churned_total if n_churned_total > 0 else 0
        st.metric("Recall Alto", f"{recall_a*100:.1f}%",
                  delta="target >= 40%" if recall_a >= 0.40 else "abaixo do target",
                  delta_color="normal" if recall_a >= 0.40 else "inverse")
    with col_bt3:
        recall_c = n_combo_churned / n_churned_total if n_churned_total > 0 else 0
        st.metric("Recall Alto+Medio", f"{recall_c*100:.1f}%",
                  delta="target >= 70%" if recall_c >= 0.70 else "abaixo do target",
                  delta_color="normal" if recall_c >= 0.70 else "inverse")
    st.caption("Backtesting indicativo (snapshot pos-fato). Ver Data Treatment Report para limitacoes.")


# ===========================================================================
# SECAO C — SIMULADOR DE CENARIOS
# ===========================================================================

elif secao == "C — Simulador de Cenarios":
    st.title("⚙️ Seção C — Simulador de Cenários")
    st.caption("Ajuste parametros e veja o impacto em tempo real no forecast e no risco de churn")

    tab_forecast, tab_churn, tab_cs = st.tabs(["📈 Forecast", "🔄 Churn Threshold", "🎯 Intervencao CS"])

    # --- Tab 1: Forecast ---
    with tab_forecast:
        st.subheader("Simulador de Forecast — Slippage e Win Rate")

        c1, c2 = st.columns([1, 2])
        with c1:
            st.markdown("**Meta de Receita Q2/25 (R$)**")
            meta_receita = st.number_input("Meta (R$)", min_value=0.0,
                                           value=10_000_000.0,
                                           step=500000.0, format="%.0f",
                                           help="Valor de referencia para gap analysis. Ajuste conforme target definido pela lideranca.")
            st.markdown("**Win Rate por Segmento**")
            wr_sim = st.slider("Win Rate Geral (%)", 20, 80, 49, step=1) / 100.0

            st.markdown("**Descontos de Slippage (ajustaveis)**")
            d_30  = st.slider("<=30d desconto (%)",  0, 100,  15, step=5) / 100.0
            d_60  = st.slider("31-60d desconto (%)", 0, 100,  30, step=5) / 100.0
            d_90  = st.slider("61-90d desconto (%)", 0, 100,  45, step=5) / 100.0
            d_180 = st.slider("91-180d desconto (%)",0, 100,  65, step=5) / 100.0
            d_max = st.slider(">180d desconto (%)",  0, 100,  85, step=5) / 100.0

        with c2:
            # Recalcular forecast com parametros ajustados
            forecast_sim = forecast.copy()

            def get_custom_discount(slippage_days, d30, d60, d90, d180, dmax):
                if pd.isna(slippage_days) or slippage_days <= 0:
                    return 0.0
                if slippage_days <= 30:   return d30
                elif slippage_days <= 60: return d60
                elif slippage_days <= 90: return d90
                elif slippage_days <= 180: return d180
                else: return dmax

            forecast_sim["sim_discount"] = forecast_sim["slippage_days"].apply(
                lambda d: get_custom_discount(d, d_30, d_60, d_90, d_180, d_max)
            )
            forecast_sim["sim_adjusted_wr"]     = wr_sim * (1 - forecast_sim["sim_discount"])
            forecast_sim["sim_contribution"]    = forecast_sim["amount_brl"] * forecast_sim["sim_adjusted_wr"]
            sim_total_forecast                  = forecast_sim["sim_contribution"].sum()
            gap_vs_meta                         = sim_total_forecast - meta_receita
            coverage_ratio                      = total_pipeline_brl / meta_receita if meta_receita > 0 else 0

            # KPIs simulados
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("Forecast Simulado", f"R$ {sim_total_forecast/1e6:.2f}M")
            with m2:
                st.metric("Meta", f"R$ {meta_receita/1e6:.2f}M")
            with m3:
                gap_color = "normal" if gap_vs_meta >= 0 else "inverse"
                st.metric("Gap vs. Meta", f"R$ {gap_vs_meta/1e6:.2f}M",
                          delta_color=gap_color)
            with m4:
                coverage_color = "normal" if coverage_ratio >= 3.0 else "inverse"
                st.metric("Pipeline Coverage", f"{coverage_ratio:.1f}x",
                          delta="target: 3x", delta_color=coverage_color)

            # Grafico por bucket de slippage
            bins   = [0, 30, 60, 90, 180, float("inf")]
            labels = ["<=30d", "31-60d", "61-90d", "91-180d", ">180d"]
            forecast_sim["slippage_bucket"] = pd.cut(forecast_sim["slippage_days"], bins=bins, labels=labels)
            by_bucket = forecast_sim.groupby("slippage_bucket", observed=True).agg(
                pipeline=("amount_brl", "sum"),
                forecast=("sim_contribution", "sum"),
                n_opps=("opportunity_id", "count"),
            ).reset_index()

            fig_fc = go.Figure()
            fig_fc.add_bar(x=by_bucket["slippage_bucket"].astype(str),
                           y=by_bucket["pipeline"], name="Pipeline", marker_color="#1565C0")
            fig_fc.add_bar(x=by_bucket["slippage_bucket"].astype(str),
                           y=by_bucket["forecast"], name="Forecast Ajustado", marker_color="#43A047")
            if meta_receita > 0:
                fig_fc.add_hline(y=meta_receita, line_dash="dash", line_color="#E53935",
                                 annotation_text=f"Meta: R${meta_receita/1e6:.1f}M")
            fig_fc.update_layout(
                title="Pipeline vs. Forecast Simulado por Aging",
                barmode="group",
                plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", font_color="white",
                height=350, yaxis_title="R$",
            )
            st.plotly_chart(fig_fc, use_container_width=True)

    # --- Tab 2: Churn Threshold ---
    with tab_churn:
        st.subheader("Simulador de Threshold de Churn Risk")

        c1, c2 = st.columns([1, 2])
        with c1:
            st.markdown("**Ajustar Thresholds**")
            th_alto  = st.slider("Threshold Alto (>=)", 0.30, 0.90, 0.526, step=0.01)
            th_medio = st.slider("Threshold Medio (>=)", 0.10, 0.60, 0.35,  step=0.01)
            if th_medio >= th_alto:
                st.error("Threshold Medio deve ser menor que Alto")

        with c2:
            # Recalcular com thresholds ajustados
            def classify(score, ta, tm):
                if pd.isna(score): return "Sem dados"
                if score >= ta:    return "Alto"
                if score >= tm:    return "Medio"
                return "Baixo"

            active_sim = active.copy()
            active_sim["risk_sim"] = active_sim["churn_risk_score"].apply(
                lambda s: classify(s, th_alto, th_medio)
            )

            dist_sim = active_sim["risk_sim"].value_counts().reset_index()
            dist_sim.columns = ["nivel", "n_contratos"]
            dist_sim["mrr_usd"] = dist_sim["nivel"].map(
                active_sim.groupby("risk_sim")["mrr_usd"].sum()
            )

            mrr_alto_sim  = active_sim[active_sim["risk_sim"] == "Alto"]["mrr_usd"].sum()
            mrr_medio_sim = active_sim[active_sim["risk_sim"] == "Medio"]["mrr_usd"].sum()

            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Contratos Alto Risco", f"{(active_sim['risk_sim']=='Alto').sum()}")
            with m2:
                st.metric("MRR Exposto (Alto)", f"USD {mrr_alto_sim/1e3:.0f}K")
            with m3:
                st.metric("MRR Exposto (Alto+Medio)", f"USD {(mrr_alto_sim+mrr_medio_sim)/1e3:.0f}K")

            # Recall simulado sobre churned
            churned_sim = catalyst[catalyst["status"] == "Churned"].copy()
            churned_sim["risk_sim"] = churned_sim["churn_risk_score"].apply(
                lambda s: classify(s, th_alto, th_medio)
            )
            recall_alto_sim  = (churned_sim["risk_sim"] == "Alto").sum() / len(churned_sim)
            recall_combo_sim = churned_sim["risk_sim"].isin(["Alto","Medio"]).sum() / len(churned_sim)

            m4, m5 = st.columns(2)
            with m4:
                st.metric("Recall Alto (backtesting)", f"{recall_alto_sim*100:.1f}%",
                          delta="OK" if recall_alto_sim>=0.40 else "Abaixo 40%",
                          delta_color="normal" if recall_alto_sim>=0.40 else "inverse")
            with m5:
                st.metric("Recall Alto+Medio", f"{recall_combo_sim*100:.1f}%",
                          delta="OK" if recall_combo_sim>=0.70 else "Abaixo 70%",
                          delta_color="normal" if recall_combo_sim>=0.70 else "inverse")

            color_map_sim = {"Alto": "#E53935", "Medio": "#FB8C00", "Baixo": "#43A047", "Sem dados": "#78909C"}
            fig_sim = px.bar(dist_sim, x="nivel", y="mrr_usd",
                             color="nivel", color_discrete_map=color_map_sim,
                             text=dist_sim.apply(lambda r: f"{r['n_contratos']} contratos\nUSD {r['mrr_usd']/1e3:.0f}K", axis=1),
                             title="MRR por Nivel de Risco (Simulado)",
                             height=300)
            fig_sim.update_layout(plot_bgcolor="#0E1117", paper_bgcolor="#0E1117",
                                  font_color="white", showlegend=False)
            st.plotly_chart(fig_sim, use_container_width=True)

    # --- Tab 3: Intervencao CS ---
    with tab_cs:
        st.subheader("Simulador de Intervencao de CS")
        st.caption("Selecione contratos de alto risco para receber acao de CS e veja o MRR recalculado")

        alto_risk_active = active[active["risk_level"] == "Alto"].copy()
        alto_risk_active["label"] = alto_risk_active.apply(
            lambda r: f"{r.get('customer_id','?')} — {r.get('canonical_name','?')} — USD {r.get('mrr_usd',0):,.0f} — Score: {r.get('churn_risk_score',0):.2f}", axis=1
        )

        col_cs1, col_cs2 = st.columns([1, 2])
        with col_cs1:
            reducao_risco = st.slider(
                "Reducao de risco por intervencao (%)",
                10, 80, 40, step=5,
                help="Assume que a intervencao do CS reduz o churn_risk_score por este percentual"
            )
            selected_contracts = st.multiselect(
                "Selecionar contratos para intervencao:",
                options=alto_risk_active["label"].tolist(),
                default=alto_risk_active["label"].tolist()[:5] if len(alto_risk_active) >= 5 else alto_risk_active["label"].tolist()
            )

        with col_cs2:
            n_selected    = len(selected_contracts)
            mrr_intervened = alto_risk_active[
                alto_risk_active["label"].isin(selected_contracts)
            ]["mrr_usd"].sum()
            mrr_remaining_risk = alto_risk_active[
                ~alto_risk_active["label"].isin(selected_contracts)
            ]["mrr_usd"].sum()

            mrr_saved_estimate = mrr_intervened * (reducao_risco / 100)
            mrr_still_at_risk  = alto_risk_active["mrr_usd"].sum() - mrr_saved_estimate

            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("Contratos p/ Intervencao", f"{n_selected}")
            with m2:
                st.metric("MRR Protegido (est.)", f"USD {mrr_saved_estimate/1e3:.0f}K",
                          help=f"Assumindo {reducao_risco}% de reducao de risco pela intervencao")
            with m3:
                st.metric("MRR Ainda em Risco", f"USD {mrr_still_at_risk/1e3:.0f}K")

            st.info(f"Sem intervencao: USD {alto_risk_active['mrr_usd'].sum()/1e3:.0f}K em risco Alto. "
                    f"Com intervencao em {n_selected} contratos: risco cai para USD {mrr_still_at_risk/1e3:.0f}K")


# ===========================================================================
# SECAO D — BOWTIE VIEW
# ===========================================================================

elif secao == "D — Bowtie View":
    st.title("🦋 Seção D — Bowtie View")
    st.caption("4 metricas focadas de alto impacto WBD — fechar o loop entre aquisicao e retencao")

    tab_cr4, tab_dt6, tab_icp, tab_cr5 = st.tabs([
        "📉 CR4 Win Rate Trendline",
        "⏱️ Δt6 Time to First Impact",
        "🎯 Closed Loop ICP",
        "💰 CR5 ACV Trendline",
    ])

    # --- CR4 Win Rate Trendline ---
    with tab_cr4:
        st.subheader("CR4 — Win Rate por Quarter")
        st.caption("Se CR4 cai enquanto VM3 sobe: marketing gera volume sem qualidade (Constraint 2 WBD)")
        st.info("Base atual: 100% Mid-Market (mediana de employees 194-495). Corte por segmento ganha utilidade quando a base expandir.")

        if cr4.empty:
            st.info("Sem dados de CR4.")
        else:
            # Agregar por quarter (sem split por segmento, pois ha apenas Mid-Market)
            cr4_agg = cr4.groupby("close_quarter").agg(
                n_won=("n_won", "sum"), n_lost=("n_lost", "sum"),
                won_brl=("won_brl", "sum"), lost_brl=("lost_brl", "sum"),
                total_opps=("total_opps", "sum"),
            ).reset_index()
            cr4_agg["win_rate_value"] = (cr4_agg["won_brl"] / (cr4_agg["won_brl"] + cr4_agg["lost_brl"])).round(4)

            fig_cr4 = px.line(
                cr4_agg, x="close_quarter", y="win_rate_value",
                markers=True,
                title="Win Rate (valor) por Quarter de Fechamento",
                labels={"close_quarter": "Quarter", "win_rate_value": "Win Rate"},
                height=400,
            )
            fig_cr4.update_traces(line=dict(width=3, color="#1565C0"), marker=dict(size=8))
            fig_cr4.add_hline(y=0.50, line_dash="dash", line_color="#78909C",
                              annotation_text="50% baseline")
            fig_cr4.update_layout(yaxis_tickformat=".0%", showlegend=False,
                                  plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", font_color="white")
            st.plotly_chart(fig_cr4, use_container_width=True)

            # Volume de opps
            fig_vol = px.bar(cr4_agg, x="close_quarter", y="total_opps",
                             title="Volume de Oportunidades por Quarter (VM3 a VM5)",
                             color_discrete_sequence=["#1565C0"],
                             height=300)
            fig_vol.update_layout(showlegend=False, plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", font_color="white")
            st.plotly_chart(fig_vol, use_container_width=True)

            # Achado
            if len(cr4) >= 3:
                recent_wr = cr4.sort_values("close_quarter").tail(3)["win_rate_value"].tolist()
                if recent_wr[-1] < recent_wr[0]:
                    st.warning("📉 Win Rate em queda nos ultimos quarters — investigar qualidade do pipeline e ICP.")
                else:
                    st.success("📈 Win Rate em recuperacao nos ultimos quarters.")

    # --- dt6 Time to First Impact ---
    with tab_dt6:
        st.subheader("Δt6 — Time to First Impact")
        st.caption("Dias do Closed Won ate feature_depth >= 0.40. Δt6 alto = maior probabilidade de churn.")
        st.warning("Δt6 mede adocao da **empresa**, nao do contrato individual. A telemetria disponivel e company-level. "
                   "Valores baixos (mediana 4 dias) podem refletir uso pre-existente de contratos anteriores na mesma empresa.")

        valid_dt6 = dt6[~dt6["telemetry_not_available"] & dt6["dt6_days"].notna()]
        failed_dt6 = dt6[~dt6["telemetry_not_available"] & dt6["onboarding_failed"]]
        no_tel_dt6 = dt6[dt6["telemetry_not_available"]]

        col_d1, col_d2, col_d3 = st.columns(3)
        with col_d1:
            st.metric("Mediana dt6 Global", f"{valid_dt6['dt6_days'].median():.0f} dias")
        with col_d2:
            fail_rate = len(failed_dt6) / (len(valid_dt6) + len(failed_dt6)) if (len(valid_dt6) + len(failed_dt6)) > 0 else 0
            st.metric("Taxa de Falha Onboarding", f"{fail_rate*100:.1f}%",
                      help="Nao atingiu feature_depth >= 0.40 dentro da janela do segmento")
        with col_d3:
            st.metric("Sem Telemetria (pre-Jan/24)", f"{len(no_tel_dt6)}")

        if not valid_dt6.empty:
            fig_dt6 = px.histogram(
                valid_dt6, x="dt6_days",
                title="Distribuicao de Δt6 (dias ate First Impact)",
                nbins=20, height=350,
                color_discrete_sequence=["#1565C0"],
            )
            fig_dt6.update_layout(plot_bgcolor="#0E1117", paper_bgcolor="#0E1117",
                                  font_color="white",
                                  xaxis_title="Dias (close_date ate feature_depth >= 0.40)",
                                  yaxis_title="Qtd de Deals")
            st.plotly_chart(fig_dt6, use_container_width=True)

        # Onboarding failure por empresa
        if not failed_dt6.empty:
            fail_by_company = failed_dt6["canonical_name"].value_counts().reset_index()
            fail_by_company.columns = ["empresa", "n_failures"]
            fig_fail = px.bar(fail_by_company.head(10), x="n_failures", y="empresa",
                              orientation="h",
                              title="Empresas com Falha de Onboarding (sem First Impact na janela)",
                              color_discrete_sequence=["#E53935"],
                              height=300)
            fig_fail.update_layout(plot_bgcolor="#0E1117", paper_bgcolor="#0E1117",
                                   font_color="white")
            st.plotly_chart(fig_fail, use_container_width=True)

        st.caption("Nota: telemetria e company-level (nao contract-level). dt6 mede adocao da empresa, "
                   "nao do deal especifico.")

    # --- Closed Loop ICP ---
    with tab_icp:
        st.subheader("Closed Loop — ICP Health")
        st.caption("Comparativo: perfil de clientes saudaveis vs. perfil de leads que marketing esta gerando")

        if icp.empty:
            st.info("Nenhuma empresa classifica como saudavel com os dados disponiveis.")
        else:
            # Destacar o finding
            n_healthy = icp["is_healthy"].sum() if "is_healthy" in icp.columns else 0
            max_health = icp["health_score_empresa"].max() if "health_score_empresa" in icp.columns else 0

            if max_health < 75:
                st.info(
                    f"**Finding importante:** Nenhuma empresa atinge health_score >= 75 (maximo: {max_health:.1f}). "
                    "Threshold relativo (p70) aplicado para identificar os clientes comparativamente mais saudaveis. "
                    "Isso confirma a hipotese central do case: a RaptorSoft entrega Value mas nao verifica Impact."
                )

            col_i1, col_i2 = st.columns(2)
            with col_i1:
                # Scatter: health_score vs feature_depth
                if "health_score_empresa" in icp.columns and "avg_feature_depth_empresa" in icp.columns:
                    icp_plot = icp.reset_index()
                    icp_plot["is_healthy_str"] = icp_plot["is_healthy"].map(
                        {True: "Saudavel (p70)", False: "Em risco/monitoramento"}
                    )
                    fig_scatter = px.scatter(
                        icp_plot,
                        x="avg_feature_depth_empresa",
                        y="health_score_empresa",
                        color="is_healthy_str",
                        text="canonical_name",
                        title="Mapa de Saude das Empresas",
                        color_discrete_map={
                            "Saudavel (p70)":             "#43A047",
                            "Em risco/monitoramento":     "#E53935",
                        },
                        height=450,
                    )
                    fig_scatter.add_vline(x=0.60, line_dash="dash", line_color="#FB8C00",
                                         annotation_text="Depth threshold (plano)")
                    fig_scatter.add_hline(y=75, line_dash="dash", line_color="#FB8C00",
                                          annotation_text="Health threshold (plano)")
                    fig_scatter.update_traces(textposition="top right", marker=dict(size=10))
                    fig_scatter.update_layout(plot_bgcolor="#0E1117", paper_bgcolor="#0E1117",
                                             font_color="white",
                                             xaxis_title="Avg Feature Depth (ultimas 4 semanas)",
                                             yaxis_title="Health Score Empresa (pond. MRR)")
                    st.plotly_chart(fig_scatter, use_container_width=True)

            with col_i2:
                # Lead source comparison
                if not icp_src.empty:
                    icp_src_plot = icp_src.reset_index()
                    fig_icp = go.Figure()
                    fig_icp.add_bar(name="Clientes Saudaveis",
                                   x=icp_src_plot["lead_source_canonical"],
                                   y=icp_src_plot["healthy_pct"],
                                   marker_color="#43A047")
                    fig_icp.add_bar(name="Todos os Leads",
                                   x=icp_src_plot["lead_source_canonical"],
                                   y=icp_src_plot["all_leads_pct"],
                                   marker_color="#1565C0")
                    fig_icp.update_layout(
                        barmode="group",
                        title="Lead Source: Saudaveis vs. Todos (%)",
                        plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", font_color="white",
                        height=350,
                        xaxis=dict(tickangle=-30),
                    )
                    st.plotly_chart(fig_icp, use_container_width=True)

                    top_gap = icp_src["gap_pct"].abs().idxmax() if not icp_src.empty else None
                    if top_gap:
                        gap_val = icp_src.loc[top_gap, "gap_pct"]
                        direction = "sobre-indexado" if gap_val > 0 else "sub-indexado"
                        st.info(f"Maior gap: **{top_gap}** ({direction} em {abs(gap_val):.1f}p.p. nos clientes saudaveis)")

    # --- CR5 ACV Trendline ---
    with tab_cr5:
        st.subheader("CR5 — ACV Trendline por Quarter")
        st.caption("ACV caindo + win rate estavel = problema de pricing/discounting (Constraint 2 WBD)")

        if cr5.empty:
            st.info("Sem dados de CR5.")
        else:
            # Agregar por quarter (sem split por segmento)
            cr5_agg = cr5.groupby("close_quarter").agg(
                acv_mean=("total_won_brl", "sum"),
                n_deals=("n_deals", "sum"),
            ).reset_index()
            cr5_agg["acv_mean"] = (cr5_agg["acv_mean"] / cr5_agg["n_deals"]).round(2)

            fig_cr5 = px.line(
                cr5_agg, x="close_quarter", y="acv_mean",
                markers=True,
                title="ACV Medio dos Closed Won por Quarter (R$)",
                labels={"close_quarter": "Quarter", "acv_mean": "ACV Medio (R$)"},
                height=400,
                text="n_deals",
            )
            fig_cr5.update_traces(line=dict(width=3, color="#43A047"), marker=dict(size=8), textposition="top center")
            fig_cr5.update_layout(
                showlegend=False,
                plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", font_color="white",
                yaxis_title="ACV Medio (R$)",
            )
            st.plotly_chart(fig_cr5, use_container_width=True)

            # Volume de deals
            fig_vol5 = px.bar(cr5_agg, x="close_quarter", y="n_deals",
                              title="Volume de Deals Closed Won por Quarter",
                              color_discrete_sequence=["#43A047"],
                              height=300)
            fig_vol5.update_layout(showlegend=False, plot_bgcolor="#0E1117", paper_bgcolor="#0E1117", font_color="white")
            st.plotly_chart(fig_vol5, use_container_width=True)

            # Diagnostico automatico
            recent_cr4 = cr4.sort_values("close_quarter").tail(4)["win_rate_value"].tolist() if not cr4.empty else []
            recent_cr5 = cr5.sort_values("close_quarter").tail(4)["acv_mean"].tolist() if not cr5.empty else []

            if len(recent_cr4) >= 2 and len(recent_cr5) >= 2:
                wr_trend  = "caindo" if recent_cr4[-1] < recent_cr4[-2] else "subindo"
                acv_trend = "caindo" if recent_cr5[-1] < recent_cr5[-2] else "subindo"

                if acv_trend == "caindo" and wr_trend != "caindo":
                    st.warning("⚠️ **ACV caindo, win rate estavel** — provavel problema de pricing/discounting (Constraint 2 WBD).")
                elif acv_trend == "caindo" and wr_trend == "caindo":
                    st.error("🚨 **ACV e win rate ambos caindo** — degradacao sistemica da operacao de vendas.")
                else:
                    st.success("✅ ACV em recuperacao.")

# ===========================================================================
# FOOTER
# ===========================================================================

st.markdown("---")
st.caption(
    "RaptorSoft RevOps Dashboard v4 | "
    "Data de referencia: 2025-04-15 | "
    "Ferramenta: Streamlit + Plotly | "
    "Desenvolvido com Claude Code (Anthropic)"
)
