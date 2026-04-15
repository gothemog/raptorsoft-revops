"""
utils.py — Funções utilitárias e constantes compartilhadas
Plano Mestre v4 — RaptorSoft RevOps Case
"""

import re
import unicodedata
import pandas as pd

# ===========================================================================
# CONSTANTES GLOBAIS
# ===========================================================================

# Data de referência (P-D4)
REFERENCE_DATE = pd.Timestamp("2025-04-15")

# Taxa de câmbio BRL/USD (P-D3)
BRL_USD_RATE = 5.70

# 20 empresas canônicas (Seção 2)
CANONICAL_NAMES = [
    "acme", "amplia", "betasolutions", "bluepath", "connectwave",
    "corelogic", "dataprime", "fluxo", "gridsoft", "innova",
    "mindbox", "nexus", "orbit", "polare", "polaris",
    "rushflow", "skyops", "tapflow", "vertice", "zentra",
]

# Mapeamento domínio → canonical_name (P-A1)
DOMAIN_TO_CANONICAL = {
    "acmecorp.com.br":       "acme",
    "ampliasolucoes.com.br": "amplia",
    "betasolutions.com.br":  "betasolutions",
    "bluepath.com.br":       "bluepath",
    "connectwave.com.br":    "connectwave",
    "corelogic.com.br":      "corelogic",
    "dataprime.com.br":      "dataprime",
    "fluxosistemas.com.br":  "fluxo",
    "gridsoft.com.br":       "gridsoft",
    "innova.com.br":         "innova",
    "mindbox.com.br":        "mindbox",
    "gruponexus.com.br":     "nexus",
    "orbitdigital.com.br":   "orbit",
    "polare.com.br":         "polare",
    "polaris.com.br":        "polaris",
    "rushflow.com.br":       "rushflow",
    "skyops.com.br":         "skyops",
    "tapflow.com.br":        "tapflow",
    "verticetech.com.br":    "vertice",
    "zentra.com.br":         "zentra",
}

# ===========================================================================
# NORMALIZAÇÃO DE NOMES — ENTITY RESOLUTION (Seção 2)
# ===========================================================================

def normalize_canonical(name: str) -> str:
    """
    Normaliza nome de empresa para forma canônica.
    Algoritmo validado do plano v4, Seção 2.
    """
    n = str(name).lower().strip()
    # Remove sufixos empresariais
    for suffix in [
        "ltda", "ltd", "corp", "corp.", "tech",
        "soluções", "solucoes", "sistemas", "digital", "group", "grupo",
    ]:
        n = re.sub(r"\b" + suffix + r"\b", "", n).strip()
    # Remove acentos
    n = unicodedata.normalize("NFD", n)
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")
    # Remove espaços, pontos e hifens
    n = re.sub(r"[\s.\-]+", "", n).strip()
    return n


# ===========================================================================
# NORMALIZAÇÃO — SALESFORCE
# ===========================================================================

# Stage normalization — 14 variantes → 6 canônicos (SF-01)
STAGE_MAP = {
    "prospecting":          "Prospecting",
    "qualification":        "Discovery/Qualification",
    "discovery":            "Discovery/Qualification",
    "proposal":             "Proposal",
    "prop":                 "Proposal",
    "negotiation":          "Negotiation",
    "closed won":           "Closed Won",
    "closed-won":           "Closed Won",
    "closed_won":           "Closed Won",
    "closed lost":          "Closed Lost",
    "closed_lost":          "Closed Lost",
}

OPEN_STAGES = ["Prospecting", "Discovery/Qualification", "Proposal", "Negotiation"]
CLOSED_WON_STAGE = "Closed Won"
CLOSED_LOST_STAGE = "Closed Lost"

def normalize_stage(val: str) -> str:
    if pd.isna(val):
        return None
    return STAGE_MAP.get(str(val).lower().strip(), str(val).strip())


# Forecast category normalization — 8 variantes → 4 (SF-02)
FORECAST_CATEGORY_MAP = {
    "pipeline":   "pipeline",
    "best case":  "best_case",
    "best_case":  "best_case",
    "commit":     "commit",
    "omit":       "omit",
}

def normalize_forecast_category(val: str) -> str:
    if pd.isna(val):
        return None
    return FORECAST_CATEGORY_MAP.get(str(val).lower().strip(), str(val).lower().strip())


# ===========================================================================
# NORMALIZAÇÃO — HUBSPOT
# ===========================================================================

# Lead source normalization — 18 variantes → 7 grupos (HS-02)
LEAD_SOURCE_MAP = {
    "organic search":    "Organic Search",
    "organic":           "Organic Search",
    "seo":               "Organic Search",
    "paid search":       "Paid Search",
    "paid":              "Paid Search",
    "google ads":        "Paid Search",
    "social media":      "Social Media",
    "social":            "Social Media",
    "content download":  "Content Download",
    "webinar":           "Webinar",
    "partner":           "Partner/Referral",
    "partner referral":  "Partner/Referral",
    "referral":          "Partner/Referral",
    "email marketing":   "Email Marketing",
    "email":             "Email Marketing",
    "direct":            "Other",
    "event":             "Other",
}

def normalize_lead_source(val: str) -> str:
    if pd.isna(val):
        return "Other"
    return LEAD_SOURCE_MAP.get(str(val).lower().strip(), "Other")


# ===========================================================================
# NORMALIZAÇÃO — TELEMETRIA
# ===========================================================================

# Plan tier normalization — 10 variantes → 4 tiers (TE-03)
PLAN_TIER_MAP = {
    "starter":       "Starter",
    "growth":        "Growth",
    "growth plan":   "Growth",
    "business":      "Business",
    "enterprise":    "Enterprise",
    "enterprise plan": "Enterprise",
}

def normalize_plan_tier(val: str) -> str:
    if pd.isna(val):
        return None
    return PLAN_TIER_MAP.get(str(val).lower().strip(), str(val).strip())


# ===========================================================================
# SEGMENTAÇÃO — CANONICAL SEGMENT (Seção 5)
# ===========================================================================

def employees_to_segment(median_employees) -> str:
    """
    Converte mediana de funcionários em canonical_segment.
    Thresholds: ≤100 → SMB, 101-500 → Mid-Market, >500 → Enterprise
    """
    if pd.isna(median_employees):
        return None
    if median_employees <= 100:
        return "SMB"
    elif median_employees <= 500:
        return "Mid-Market"
    else:
        return "Enterprise"


# ===========================================================================
# FUNÇÕES DE SCORE — CHURN RISK (Seção 8)
# ===========================================================================

def health_trend_to_score(trend) -> float:
    """Converte health_trend em score de risco (W2). NULL → 0.50 neutro."""
    if pd.isna(trend):
        return 0.50
    mapping = {
        "declining": 1.00,
        "stable":    0.30,
        "improving": 0.00,
    }
    return mapping.get(str(trend).lower().strip(), 0.50)


def get_renewal_proximity_score(days_to_renewal) -> float:
    """Converte days_to_renewal em score de proximidade de risco (W5)."""
    if pd.isna(days_to_renewal):
        return 0.0
    d = float(days_to_renewal)
    if d <= 30:
        return 1.00
    elif d <= 60:
        return 0.60
    elif d <= 90:
        return 0.30
    else:
        return 0.00


def get_nps_bonus(nps) -> float:
    """
    Retorna ajuste de NPS sobre Contract_Risk (Seção 8).
    NULL → 0.0 (nunca penaliza).
    """
    if pd.isna(nps):
        return 0.0
    n = float(nps)
    if n <= 6:
        return 0.10   # detrator
    elif n <= 8:
        return 0.00   # passivo
    else:
        return -0.08  # promotor


# ===========================================================================
# FUNÇÕES DE FORECAST (Seção 9)
# ===========================================================================

def get_slippage_discount(slippage_days: float, engagement_modifier: float = None) -> float:
    """
    Retorna fator de desconto progressivo por aging (Seção 9).
    Optional: engagement modifier adjustment (NOVO v4 — experimental).
    """
    if pd.isna(slippage_days) or slippage_days <= 0:
        return 0.0
    d = float(slippage_days)
    if d <= 30:
        discount = 0.15
    elif d <= 60:
        discount = 0.30
    elif d <= 90:
        discount = 0.45
    elif d <= 180:
        discount = 0.65
    else:
        discount = 0.85

    # NOVO v4 — feedback loop telemetria → forecast (opcional)
    if engagement_modifier is not None and engagement_modifier < 0.90:
        discount = discount * 0.80

    return discount
