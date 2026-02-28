"""
Geopolitical Risk Dashboard v0.9.3 — Stage-Based Architecture
==============================================================
Live multi-signal geopolitical risk scoring across 3 regions.

Signals (Stage 2+3 scored):
  Stage 1 - Physical Observable: Cloudflare Radar (early warning, not scored)
  Stage 2 - Ground Signal: Google Trends Panic (25% of composite)
  Stage 3 - Market Positioning: Polymarket (37.5%) + Financial Markets (37.5%)
  Stage 4 - Narrative Formation: GDELT News Tone + GT Global (context only)

Supplemental: ACLED CAST monthly conflict forecasts (display only)

Architecture:
  - Flask backend, single file
  - Supabase for persistent storage
  - Background threads for GDELT (2h), Google Trends (2h), Cloudflare (1h)
  - Main refresh: Polymarket + Markets (~20s)
  - Live in-place JS updates every 60s via /api/scores

SETUP:
  pip install flask requests python-dateutil yfinance numpy pandas supabase pytrends google-cloud-bigquery
  Secrets: ACLED_EMAIL, ACLED_PASSWORD, SUPABASE_URL, SUPABASE_KEY, GOOGLE_CREDENTIALS, GOOGLE_PROJECT_ID, CLOUDFLARE_RADAR_TOKEN
"""

# ============================================================
# IMPORTS
# ============================================================

import json
import os
import re
import threading
import time as _time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dateutil import parser as dateparser
from flask import Flask, render_template_string
from supabase import create_client

# ============================================================
# APP + GLOBAL STATE
# ============================================================

app = Flask(__name__)

_dashboard_state = {"html": None, "last_updated": 0, "updating": False}
_gtrends_state = {"regional": {}, "last_updated": 0, "fetching": False}
_last_contract_snapshot_time = 0

# ============================================================
# SUPABASE
# ============================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
CLOUDFLARE_RADAR_TOKEN = os.environ.get("CLOUDFLARE_RADAR_TOKEN", "")
_supabase = None


def get_supabase():
    global _supabase
    if _supabase is None and SUPABASE_URL and SUPABASE_KEY:
        try:
            _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        except Exception as e:
            print(f"Supabase connection error: {e}")
    return _supabase


# ============================================================
# CONFIG — SCORING
# ============================================================

WEIGHTS = {"polymarket": 0.375, "markets": 0.375, "gt_panic": 0.25}
GLOBAL_REGIONAL_WEIGHT = 0.85
GLOBAL_PM_WEIGHT = 0.15
PM_MATURITY_THRESHOLD = 50
PM_STD_FLOOR = 0.01
LOOKBACK_DAYS = 120

# ============================================================
# CONFIG — REGIONS
# ============================================================

REGIONS = {
    "europe": {
        "name": "Europe",
        "emoji": "🇪🇺",
        "colour": "#818cf8",
        "keywords": [
            "russia", "ukraine", "nato", "crimea", "baltics", "poland", "romania",
            "moldova", "transnistria", "kursk", "greenland", "denmark", "greece",
        ],
        "acled_countries": [
            "Russia", "Ukraine", "Poland", "Romania", "Moldova", "Lithuania", "Latvia",
            "Estonia", "Finland", "Sweden", "Norway", "Georgia", "Armenia", "Azerbaijan",
            "Belarus", "Serbia", "Kosovo", "Bosnia and Herzegovina", "Montenegro",
            "North Macedonia", "Albania", "Greece", "Turkey", "Bulgaria", "Hungary",
            "Czech Republic", "Slovakia", "Croatia", "Slovenia", "United Kingdom",
            "France", "Germany", "Italy", "Spain", "Netherlands", "Belgium", "Denmark", "Cyprus",
        ],
    },
    "middle_east": {
        "name": "Middle East",
        "emoji": "🕌",
        "colour": "#fbbf24",
        "keywords": [
            "iran", "israel", "gaza", "west bank", "palestine", "lebanon", "syria",
            "yemen", "houthi", "hezbollah", "hamas", "red sea", "qatar", "iraq",
            "beirut", "damascus", "turkey", "jerusalem", "baghdad",
        ],
        "acled_countries": [
            "Iran", "Israel", "Palestine", "Lebanon", "Syria", "Yemen", "Iraq", "Jordan",
            "Saudi Arabia", "Bahrain", "Qatar", "United Arab Emirates", "Oman", "Kuwait",
            "Egypt", "Libya", "Tunisia", "Algeria", "Morocco",
        ],
    },
    "asia_pacific": {
        "name": "Asia-Pacific",
        "emoji": "🌏",
        "colour": "#34d399",
        "keywords": [
            "china", "taiwan", "north korea", "south china sea", "korean peninsula",
            "india", "pakistan", "kashmir", "philippines", "japan", "cambodia",
            "thailand", "south korea", "korea", "afghanistan",
        ],
        "acled_countries": [
            "China", "Taiwan", "North Korea", "South Korea", "Japan", "Philippines",
            "Vietnam", "Indonesia", "Malaysia", "Thailand", "Myanmar", "India",
            "Pakistan", "Afghanistan", "Bangladesh", "Cambodia", "Laos",
        ],
    },
}

GLOBAL_KEYWORDS = [
    "nuclear weapon detonation", "nuclear detonation", "world war",
    "u.s. test a nuclear", "u.s.-russia nuclear deal", "us-russia nuclear",
    "u.s. x russia nuclear", "russia test a nuclear", "russia nuclear test",
    "u.s. x china military clash", "us x china military",
    "u.s. x russia military clash", "us x russia military clash",
    "invade a latin american", "invade latin america",
]

# ============================================================
# CONFIG — MARKET TICKERS
# ============================================================

MARKET_TICKERS = {
    "RHM.DE":    {"name": "Rheinmetall",     "type": "defence",   "region": "europe"},
    "BA.L":      {"name": "BAE Systems",     "type": "defence",   "region": "europe"},
    "HO.PA":     {"name": "Thales",          "type": "defence",   "region": "europe"},
    "LDO.MI":    {"name": "Leonardo",        "type": "defence",   "region": "europe"},
    "SAAB-B.ST": {"name": "Saab",            "type": "defence",   "region": "europe"},
    "RR.L":      {"name": "Rolls-Royce",     "type": "defence",   "region": "europe"},
    "TTF=F":     {"name": "EU Natural Gas",  "type": "commodity", "region": "europe"},
    "EWG":       {"name": "Germany Index",   "type": "index",     "region": "europe",       "inverted": True},
    "USDRUB=X":  {"name": "USD/RUB",         "type": "currency",  "region": "europe",       "inverted": True},
    "BZ=F":      {"name": "Brent Crude",     "type": "commodity", "region": "middle_east"},
    "ESLT":      {"name": "Elbit Systems",   "type": "defence",   "region": "middle_east"},
    "EIS":       {"name": "Israel Index",    "type": "index",     "region": "middle_east",  "inverted": True},
    "USDILS=X":  {"name": "USD/ILS",         "type": "currency",  "region": "middle_east",  "inverted": True},
    "012450.KS": {"name": "Hanwha Aerospace","type": "defence",   "region": "asia_pacific"},
    "7011.T":    {"name": "Mitsubishi Heavy", "type": "defence",  "region": "asia_pacific"},
    "EWT":       {"name": "Taiwan Index",    "type": "index",     "region": "asia_pacific",  "inverted": True},
    "ASHR":      {"name": "China A-Shares",  "type": "index",     "region": "asia_pacific",  "inverted": True},
    "EWJ":       {"name": "Japan Index",     "type": "index",     "region": "asia_pacific",  "inverted": True},
    "USDCNY=X":  {"name": "USD/CNY",         "type": "currency",  "region": "asia_pacific",  "inverted": True},
    "USDTWD=X":  {"name": "USD/TWD",         "type": "currency",  "region": "asia_pacific",  "inverted": True},
    "USDINR=X":  {"name": "USD/INR",         "type": "currency",  "region": "asia_pacific",  "inverted": True},
    "GC=F":      {"name": "Gold",            "type": "commodity", "region": "global"},
    "ITA":       {"name": "US Defence ETF",  "type": "defence",   "region": "global"},
}

# ============================================================
# CONFIG — POLYMARKET KEYWORDS
# ============================================================

STRONG_KEYWORDS = [
    "invasion", "invade", "military strike", "airstrike", "air strike",
    "bombing", "bomb strike", "missile", "nuclear war", "nuclear weapon",
    "ceasefire", "peace deal", "regime change", "regime fall", "martial law",
    "no-fly zone", "airspace closure", "ground troops", "deploy troops",
    "world war", "chemical weapon", "biological weapon",
    "houthi", "hezbollah", "hamas", "nato article 5",
    "military action", "military operation", "military intervention",
    "armed conflict", "escalation", "ground offensive", "ground operation",
    "cyberattack", "cyber attack", "war powers", "missile launch",
    "military clash", "military engagement", "normalize relations",
    "security agreement",
    "strike iran", "strike on israel", "strike on iraq", "strike on yemen",
    "strike on qatar", "strike on cuba", "strike on somalia", "strike on mexico",
    "strike on nigeria", "strike on venezuela", "strike on damascus",
    "strike on beirut", "strike on", "strikes iran", "strikes iraq", "strikes yemen",
    "invade greenland", "invade cuba", "invade mexico", "invade colombia",
    "invade iran", "anti-cartel", "seizes", "downs a", "forces enter",
    "visit north korea", "direct talks", "deal signed", "end enrichment",
    "oil tanker", "strike greater beirut", "ground offensive in gaza",
    "ground offensive in lebanon", "forces in venezuela", "strike on poland",
    "internet blackout", "evacuates", "evacuate embassy",
    "close airspace", "closes airspace", "diplomatic meeting",
    "visits china", "visits north korea", "rubio visits",
    "total internet blackout", "evacuates beirut", "evacuates jerusalem",
    "evacuates baghdad", "us x iran diplomatic", "iran diplomatic meeting",
]

CONTEXT_KEYWORDS = [
    "war", "attack", "troops", "conflict", "sanctions", "assassination",
    "coup", "blockade", "annex", "nuclear", "nato", "defense", "defence",
    "drone", "enrichment", "strike", "strikes", "invade", "invasion",
    "clash", "engagement", "offensive", "evacuate", "evacuates",
    "blackout", "airspace", "diplomatic",
]

CONFLICT_REGIONS_KW = [
    "iran", "israel", "russia", "ukraine", "china", "taiwan", "north korea",
    "gaza", "crimea", "red sea", "south china sea", "syria", "yemen",
    "lebanon", "west bank", "palestine", "korean peninsula", "baltics",
    "poland", "romania", "moldova", "transnistria", "kursk", "india",
    "pakistan", "kashmir", "philippines", "japan", "cambodia", "thailand",
    "iraq", "qatar", "turkey", "greece", "denmark", "greenland", "cuba",
    "mexico", "venezuela", "colombia", "nigeria", "somalia", "beirut",
    "damascus", "latin america", "south korea", "saudi arabia",
]

MEME_TERMS = [
    "gta vi", "gta 6", "before gta", "before elder scrolls",
    "valorant", "counter-strike", "dota", "league of legends",
    "map winner", "map handicap", "games total", "bo3", "bo5",
    "game changers", "vct ", "esl ", "blast ", "major winner",
    "overwatch", "call of duty", "fortnite", "apex legends",
]

DE_ESCALATION_KEYWORDS = [
    "ceasefire", "peace deal", "peace agreement", "peace treaty",
    "withdrawal", "withdraw troops", "disarm", "de-escalation", "truce",
    "armistice", "diplomatic resolution", "nuclear deal",
    "normalize relations", "normalise relations", "security agreement",
    "end enrichment", "direct talks", "deal signed", "visit north korea",
    "diplomatic meeting", "visits china", "visits north korea",
    "rubio visits china", "iran diplomatic meeting", "us x iran diplomatic",
    "conflict ends", "end of military operations", "ends military operations",
    "announces end of", "operations end by",
]

DE_ESCALATION_NEGATORS = [
    "broken", "break", "cancelled", "violated", "collapses", "collapse",
    "fails", "failed",
]

EXCLUDE_KEYWORDS = [
    "joins nato", "join nato", "agrees not to join", "secretary-general",
    "secretary general", "out as leader", "out as head", "how many countries",
    "indonesia normalize", "peacekeeping force",
    "us or israel strike iran", "will us or israel strike",
    "not strike iran by", "iranian regime survive",
    "israel or the us target", "win the most seats", "parliamentary election",
    "strike cuba next", "strike somalia next", "strike yemen next",
    "next diplomatic us-iran meeting be",
    "no qualifying diplomatic", "will the next diplomatic",
    "next diplomatic", "us next strike iran",
    "strike cuba next", "strike somalia next", "strike yemen next",
    "strike iran next", "strike iraq next", "strike mexico next",
    "strike nigeria next", "strike venezuela next", "strike syria next",
    "strike lebanon next", "strike north korea next", "strike colombia next",
    "us strike next", "will the us strike next",
]

EXCLUDE_PATTERNS = [
    re.compile(r"strike \d+ countr"),
    re.compile(r"strike [≤≥<>]\d+ countr"),
    re.compile(r"\d+ or more countr"),
    re.compile(r"\d+ or more .+ strikes"),
    re.compile(r"\d+ or fewer .+ strikes"),
    re.compile(r"between \d+ and \d+ .+ strikes"),
    re.compile(r"strike .+ on (january|february|march|april|may|june|july|august|september|october|november|december) \d"),
    re.compile(r"will .+ strike .+ on (january|february|march|april|may|june|july|august|september|october|november|december) \d"),
    re.compile(r"strike .+ next\?"),
    re.compile(r"\d+ or fewer .+ strike"),
    re.compile(r"strike .+ during week of"),
    re.compile(r"strike (no|one|two|three|four|five|six|seven|eight|nine|ten|\d+) countr"),
    re.compile(r"strike .+ (no|one|two|three|four|five|six|seven|eight|nine|ten) countr"),
    re.compile(r"strike (no|one|two|three|four|five|six|seven|eight|nine|ten|\d+) or (more|fewer) countr"),
]

# ============================================================
# CONFIG — CLOUDFLARE RADAR COUNTRIES
# ============================================================

CLOUDFLARE_COUNTRIES = {
    "europe": ["RU", "UA", "PL", "RO", "MD", "FI", "SE", "DE", "GB", "FR"],
    "middle_east": ["IR", "IL", "LB", "SY", "YE", "IQ", "SA", "EG"],
    "asia_pacific": ["CN", "TW", "KP", "KR", "IN", "PK", "MM", "PH", "JP"],
}

COUNTRY_CODE_NAMES = {
    "RU": "Russia", "UA": "Ukraine", "PL": "Poland", "RO": "Romania",
    "MD": "Moldova", "FI": "Finland", "SE": "Sweden", "DE": "Germany",
    "GB": "United Kingdom", "FR": "France", "IR": "Iran", "IL": "Israel",
    "LB": "Lebanon", "SY": "Syria", "YE": "Yemen", "IQ": "Iraq",
    "SA": "Saudi Arabia", "EG": "Egypt", "CN": "China", "TW": "Taiwan",
    "KP": "North Korea", "KR": "South Korea", "IN": "India", "PK": "Pakistan",
    "MM": "Myanmar", "PH": "Philippines", "JP": "Japan",
}

# ============================================================
# CONFIG — GOOGLE TRENDS KEYWORDS
# ============================================================

GTRENDS_GLOBAL_TERMS = {
    "europe": [
        "Russia Ukraine war", "NATO threat", "Ukraine news", "Russia nuclear",
        "Russia NATO", "Ukraine ceasefire", "European rearmament", "Russia sanctions",
    ],
    "middle_east": [
        "Iran war", "Israel Iran", "Iran strike", "Iran military", "Iran protest",
        "Houthi attack", "Israel Gaza", "Syria conflict",
    ],
    "asia_pacific": [
        "China Taiwan", "North Korea missile", "South China Sea",
        "India Pakistan conflict", "Taiwan strait", "India China border",
        "North Korea nuclear", "Philippines China",
    ],
}

GTRENDS_COUNTRY_TERMS = {
    "europe": [
        {"term": "VPN",           "geo": "RU", "label": "VPN (Russia)"},
        {"term": "VPN",           "geo": "UA", "label": "VPN (Ukraine)"},
        {"term": "bomb shelter",  "geo": "PL", "label": "Bomb shelter (Poland)"},
        {"term": "conscription",  "geo": "UA", "label": "Conscription (Ukraine)"},
        {"term": "martial law",   "geo": "UA", "label": "Martial law (Ukraine)"},
        {"term": "missile",       "geo": "PL", "label": "Missile (Poland)"},
        {"term": "air raid",      "geo": "UA", "label": "Air raid (Ukraine)"},
        {"term": "Russian drone", "geo": "PL", "label": "Russian drone (Poland)"},
        {"term": "nuclear shelter", "geo": "FI", "label": "Nuclear shelter (Finland)"},
        {"term": "nuclear shelter", "geo": "SE", "label": "Nuclear shelter (Sweden)"},
    ],
    "middle_east": [
        {"term": "VPN",            "geo": "IR", "label": "VPN (Iran)"},
        {"term": "bomb shelter",   "geo": "IL", "label": "Bomb shelter (Israel)"},
        {"term": "evacuation",     "geo": "IL", "label": "Evacuation (Israel)"},
        {"term": "gas mask",       "geo": "IL", "label": "Gas mask (Israel)"},
        {"term": "missile strike", "geo": "LB", "label": "Missile strike (Lebanon)"},
        {"term": "bomb shelter",   "geo": "LB", "label": "Bomb shelter (Lebanon)"},
        {"term": "VPN",            "geo": "SY", "label": "VPN (Syria)"},
        {"term": "evacuation",     "geo": "LB", "label": "Evacuation (Lebanon)"},
        {"term": "air raid",       "geo": "IQ", "label": "Air raid (Iraq)"},
    ],
    "asia_pacific": [
        {"term": "VPN",                "geo": "CN", "label": "VPN (China)"},
        {"term": "evacuation",         "geo": "TW", "label": "Evacuation (Taiwan)"},
        {"term": "bomb shelter",       "geo": "KR", "label": "Bomb shelter (S. Korea)"},
        {"term": "conscription",       "geo": "TW", "label": "Conscription (Taiwan)"},
        {"term": "India Pakistan war", "geo": "IN", "label": "India Pakistan war (India)"},
        {"term": "air raid",           "geo": "TW", "label": "Air raid (Taiwan)"},
        {"term": "missile",            "geo": "KR", "label": "Missile (S. Korea)"},
        {"term": "shelter",            "geo": "JP", "label": "Shelter (Japan)"},
        {"term": "VPN",                "geo": "MM", "label": "VPN (Myanmar)"},
        {"term": "drone attack",       "geo": "IN", "label": "Drone attack (India)"},
    ],
}

# ============================================================
# GDELT COUNTRY CODES (FIPS 10-4)
# ============================================================

GDELT_COUNTRY_CODES = {
    "europe": ["RS", "UP", "PL", "RO", "MD", "LH", "LG", "EN", "FI", "SW",
               "NO", "GG", "AM", "AJ", "BO", "RI", "KV", "BK", "MJ", "MK",
               "AL", "GR", "TU", "BU", "HU", "EZ", "LO", "HR", "SI", "UK",
               "FR", "GM", "IT", "SP", "NL", "BE", "DA", "CY"],
    "middle_east": ["IR", "IS", "GZ", "LE", "SY", "YM", "IZ", "JO", "SA",
                     "BA", "QA", "AE", "MU", "KU", "EG", "LY", "TS", "AG", "MO"],
    "asia_pacific": ["CH", "TW", "KN", "KS", "JA", "RP", "VM", "ID", "MY",
                      "TH", "BM", "IN", "PK", "AF", "BG", "CB", "LA"],
}

BQ_MAX_BYTES = 150 * 1024 * 1024

# ============================================================
# CACHING (ACLED only)
# ============================================================

_cache = {
    "acled": {"data": None, "stats": None, "score": None, "raw": None, "prev_raw": None, "time": 0, "ttl": 3600},
}


def get_cached(key):
    c = _cache.get(key)
    if c and c["data"] is not None and (_time.time() - c["time"]) < c["ttl"]:
        return c
    return None


def set_cache(key, **kw):
    if key in _cache:
        _cache[key].update(kw)
        _cache[key]["time"] = _time.time()


# ============================================================
# HELPERS — SCORING & DISPLAY
# ============================================================

def z_to_centred_score(z):
    """Sigmoid scoring curve. Asymptotically approaches 0 and 100,
    never reaching either — always leaves room for further escalation.
    k=0.65 calibrated so z=1→~65, z=2→~79, z=3→~88, z=4→~93, z=5→~96.
    Replaces the old linear 50 + z*15 which hit ceiling at z≈3.33."""
    import math
    return round(max(0.1, min(99.9, 100 / (1 + math.exp(-0.65 * z)))), 1)


def get_risk_level(s):
    if s < 15:   return "UNUSUALLY CALM", "#34d399"
    elif s < 30: return "CALM", "#6ee7b7"
    elif s < 42: return "BELOW NORMAL", "#a3b8a0"
    elif s < 58: return "NORMAL", "#b0b0b0"
    elif s < 70: return "ELEVATED", "#fbbf24"
    elif s < 85: return "HIGH", "#fb923c"
    else:        return "CRITICAL", "#f87171"


def get_score_colour(s):
    if s < 15:   return "#34d399"
    elif s < 30: return "#6ee7b7"
    elif s < 42: return "#a3b8a0"
    elif s < 58: return "#b0b0b0"
    elif s < 70: return "#fbbf24"
    elif s < 85: return "#fb923c"
    else:        return "#f87171"


def get_prob_colour(p):
    if p < 15:   return "#22c55e"
    elif p < 30: return "#84cc16"
    elif p < 50: return "#eab308"
    elif p < 70: return "#f97316"
    else:        return "#ef4444"


def get_change_colour(c):
    if c > 2:     return "#ef4444"
    elif c > 0.5: return "#f97316"
    elif c > -0.5: return "#888888"
    else:         return "#22c55e"


def make_sparkline_svg(vals, colour="#888", w=60, h=20):
    if not vals or len(vals) < 2:
        return ""
    mn, mx = min(vals), max(vals)
    rng = mx - mn if mx != mn else 1
    pts = " ".join(
        f"{round(i / (len(vals) - 1) * w, 1)},{round(h - ((v - mn) / rng) * h, 1)}"
        for i, v in enumerate(vals)
    )
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="vertical-align:middle;">'
        f'<polyline points="{pts}" fill="none" stroke="{colour}" stroke-width="1.5" '
        f'stroke-linecap="round" stroke-linejoin="round"/></svg>'
    )


def get_convergence_status(scores):
    active = {k: v for k, v in scores.items() if v is not None}
    if not active:
        return "No Data", "", "#888"
    names = {
        "polymarket": "Polymarket", "markets": "Markets",
        "acled": "ACLED", "gdelt": "GDELT", "gtrends": "Google Trends",
        "gt_panic": "Panic Search", "gt_global": "Global Search",
        "cloudflare": "Internet",
    }
    elev = [names.get(k, k) for k, v in active.items() if v >= 58]
    depr = [names.get(k, k) for k, v in active.items() if v <= 42]
    ne, nd = len(elev), len(depr)
    if ne == 0 and nd == 0:
        return "Quiet", "All signals within normal range", "#b0b0b0"
    elif ne >= 3:
        return "Broad Escalation", " + ".join(elev) + " elevated", "#f87171"
    elif nd >= 3:
        return "Broad De-escalation", " + ".join(depr) + " below baseline", "#34d399"
    elif ne >= 2 and nd == 0:
        return "Confirming", " + ".join(elev) + " elevated", "#fb923c"
    elif nd >= 2 and ne == 0:
        return "Calming", " + ".join(depr) + " below baseline", "#6ee7b7"
    elif ne >= 1 and nd >= 1:
        return "Mixed Signals", f"{' + '.join(elev)} elevated, {' + '.join(depr)} below", "#fbbf24"
    elif ne == 1:
        return "Early Signal", f"{elev[0]} elevated", "#fbbf24"
    elif nd == 1:
        return "Early Calm", f"{depr[0]} below baseline", "#6ee7b7"
    return "Quiet", "All signals within normal range", "#b0b0b0"


# ============================================================
# SUPABASE — SAVE & FETCH
# ============================================================

def save_score_snapshot(composite, pm, mkt, acled, gdelt, pm_stats, acled_stats, gdelt_stats, risk_level):
    db = get_supabase()
    if not db: return
    try:
        recent = db.table("daily_scores").select("recorded_at").order("recorded_at", desc=True).limit(1).execute()
        if recent.data:
            last_time = dateparser.parse(recent.data[0]["recorded_at"])
            if (datetime.now(timezone.utc) - last_time).total_seconds() < 900: return  # 15-min cadence (temporarily accelerated from 30-min for crisis period)
        row = {
            "composite_score": composite, "polymarket_score": pm, "markets_score": mkt,
            "acled_score": acled, "gdelt_score": gdelt,
            "polymarket_contracts": pm_stats.get("num_contracts", 0),
            "polymarket_volume": pm_stats.get("total_volume", 0),
            "polymarket_avg_risk": pm_stats.get("avg_risk_pct", 0) / 100 if pm_stats.get("avg_risk_pct") else None,
            "acled_total_forecast": acled_stats.get("total_forecast") if isinstance(acled_stats, dict) else None,
            "gdelt_recent_tone": gdelt_stats.get("recent_tone") if isinstance(gdelt_stats, dict) else None,
            "gdelt_baseline_tone": gdelt_stats.get("baseline_tone") if isinstance(gdelt_stats, dict) else None,
            "gdelt_z_score": gdelt_stats.get("z_score") if isinstance(gdelt_stats, dict) else None,
            "risk_level": risk_level,
        }
        db.table("daily_scores").insert(row).execute()
        print(f"Saved score snapshot: {composite}")
    except Exception as e:
        print(f"Supabase save error: {e}")


def save_contract_snapshots(geo_markets):
    global _last_contract_snapshot_time
    if not geo_markets: return
    now = _time.time()
    if now - _last_contract_snapshot_time < 180: return  # 3-min cadence (temporarily accelerated from 10-min for crisis period)
    db = get_supabase()
    if not db: return
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        rows = [{
            "recorded_at": now_iso, "contract_slug": m.get("slug", ""),
            "question": m.get("question", ""), "yes_price": m.get("yes_price", 0),
            "risk_price": m.get("risk_price", 0), "volume": m.get("volume", 0),
            "is_deescalation": m.get("is_deescalation", False), "region": m.get("region", "global"),
        } for m in geo_markets]
        for i in range(0, len(rows), 50):
            db.table("contract_snapshots").insert(rows[i:i + 50]).execute()
        _last_contract_snapshot_time = now
        print(f"Saved {len(rows)} contract snapshots (10-min cadence)")
    except Exception as e:
        print(f"Contract snapshot save error: {e}")


def save_regional_scores(regional_composites, pm_regional, mkt_regional):
    db = get_supabase()
    if not db: return
    try:
        recent = db.table("regional_scores").select("recorded_at").order("recorded_at", desc=True).limit(1).execute()
        if recent.data:
            last_time = dateparser.parse(recent.data[0]["recorded_at"])
            if (datetime.now(timezone.utc) - last_time).total_seconds() < 1800: return
        now_iso = datetime.now(timezone.utc).isoformat()
        rows = []
        for rk, rd in regional_composites.items():
            pm_stats = pm_regional.get(rk, {}).get("stats", {})
            mkt_data = mkt_regional.get(rk, {})
            rows.append({
                "recorded_at": now_iso, "region": rk,
                "composite_score": rd["composite"],
                "polymarket_score": rd["signals"]["polymarket"],
                "markets_score": rd["signals"]["markets"],
                "gdelt_score": float(rd["signals"].get("gdelt", 50.0)),
                "gtrends_score": float(rd["signals"].get("gt_panic", 50.0)),
                "gt_panic_score": float(rd["signals"].get("gt_panic", 50.0)),
                "gt_global_score": float(rd["signals"].get("gt_global", 50.0)),
                "cloudflare_score": float(rd["signals"].get("cloudflare", 50.0)),
                "pm_contracts": pm_stats.get("num_contracts", 0),
                "pm_avg_risk": pm_stats.get("avg_risk_pct", 0) / 100 if pm_stats.get("avg_risk_pct") else None,
                "mkt_tickers": len(mkt_data.get("tickers", [])),
            })
        if rows:
            db.table("regional_scores").insert(rows).execute()
            print("Saved regional scores: " + ", ".join(f"{r['region']}={r['composite_score']}" for r in rows))
    except Exception as e:
        print(f"Regional scores save error: {e}")


def fetch_score_history(days=30):
    db = get_supabase()
    if not db: return []
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        r = db.table("daily_scores").select(
            "recorded_at,composite_score,polymarket_score,markets_score,acled_score,gdelt_score"
        ).gte("recorded_at", since).order("recorded_at", desc=False).execute()
        return r.data or []
    except Exception as e:
        print(f"Supabase fetch error: {e}")
        return []


def fetch_last_known_score(signal_column):
    db = get_supabase()
    if not db: return None, None
    try:
        r = db.table("daily_scores").select(
            f"{signal_column},recorded_at"
        ).not_.is_(signal_column, "null").order("recorded_at", desc=True).limit(1).execute()
        if r.data:
            return r.data[0].get(signal_column), r.data[0].get("recorded_at", "")
        return None, None
    except Exception as e:
        print(f"Supabase fallback error: {e}")
        return None, None


def fetch_regional_score_history(days=30):
    db = get_supabase()
    if not db: return {}
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        r = db.table("regional_scores").select(
            "recorded_at,region,composite_score,polymarket_score,markets_score,gdelt_score,gtrends_score,gt_panic_score,gt_global_score,cloudflare_score"
        ).gte("recorded_at", since).order("recorded_at", desc=False).execute()
        if not r.data: return {}
        history = {}
        for row in r.data:
            reg = row.get("region", "")
            if reg not in history: history[reg] = []
            history[reg].append({
                "recorded_at": row["recorded_at"], "score": row["composite_score"],
                "pm": row.get("polymarket_score"), "mkt": row.get("markets_score"),
                "gdelt": row.get("gdelt_score"), "gtrends": row.get("gtrends_score"),
                "gt_panic": row.get("gt_panic_score"), "gt_global": row.get("gt_global_score"),
                "cloudflare": row.get("cloudflare_score"),
            })
        return history
    except Exception as e:
        print(f"Regional history fetch error: {e}")
        return {}


# ============================================================
# POLYMARKET — FETCH & FILTER
# ============================================================

def classify_contract_region(st):
    if any(gk in st for gk in GLOBAL_KEYWORDS): return "global"
    hits = {}
    for rk, rc in REGIONS.items():
        h = sum(1 for kw in rc["keywords"] if kw in st)
        if h > 0: hits[rk] = h
    return max(hits, key=hits.get) if hits else "global"


def fetch_polymarket_events():
    url = "https://gamma-api.polymarket.com/events"
    all_ev, offset = [], 0
    while True:
        try:
            r = requests.get(url, params={"active": "true", "closed": "false", "limit": 100, "offset": offset}, timeout=15)
            r.raise_for_status()
            batch = r.json()
            if not batch: break
            all_ev.extend(batch)
            offset += 100
            if offset >= 10000: break
        except Exception as e:
            print(f"Polymarket error: {e}"); break
    print(f"Polymarket: fetched {len(all_ev)} events")
    return all_ev


def filter_geopolitical_markets(events):
    geo = []
    for ev in events:
        title, slug = ev.get("title", ""), ev.get("slug", "")
        for mkt in ev.get("markets", []):
            q = mkt.get("question", "")
            st = f"{title} {q} {slug}".lower()
            if mkt.get("closed") or mkt.get("resolved"):
                continue
            ed = mkt.get("endDate") or mkt.get("end_date_iso") or ""
            ev_ed = ev.get("endDate") or ev.get("end_date_iso") or ""
            if ed and ed != ev_ed:
                try:
                    edt = dateparser.parse(ed)
                    if edt.tzinfo is None: edt = edt.replace(tzinfo=timezone.utc)
                    if edt < datetime.now(timezone.utc): continue
                except: pass
            if any(m in st for m in MEME_TERMS): continue
            if any(x in st for x in EXCLUDE_KEYWORDS): continue
            if any(p.search(st) for p in EXCLUDE_PATTERNS): continue
            if not (any(k in st for k in STRONG_KEYWORDS) or (any(k in st for k in CONTEXT_KEYWORDS) and any(k in st for k in CONFLICT_REGIONS_KW))): continue
            try:
                pr = mkt.get("outcomePrices", "[]")
                prices = json.loads(pr) if isinstance(pr, str) else pr
                yp = float(prices[0]) if prices else 0.0
            except: yp = 0.0
            try: vol = float(mkt.get("volume", 0) or 0)
            except: vol = 0.0
            if yp <= 0.001: continue
            is_de = any(k in st for k in DE_ESCALATION_KEYWORDS)
            if is_de and any(n in st for n in DE_ESCALATION_NEGATORS): is_de = False
            rp = (1.0 - yp) if is_de else yp
            geo.append({
                "question": q or title, "yes_price": yp, "risk_price": rp,
                "probability_pct": round(yp * 100, 1), "risk_pct": round(rp * 100, 1),
                "volume": vol, "slug": mkt.get("marketSlug", slug),
                "is_deescalation": is_de, "region": classify_contract_region(st),
            })
    geo.sort(key=lambda x: x["volume"], reverse=True)
    return geo


# ============================================================
# POLYMARKET — PER-CONTRACT NORMALISATION
# ============================================================

def fetch_contract_baselines():
    db = get_supabase()
    if not db: return {}
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        all_rows, page_size, offset = [], 1000, 0
        while True:
            r = db.table("contract_snapshots").select("contract_slug,question,risk_price,recorded_at").gte("recorded_at", since).order("recorded_at", desc=False).range(offset, offset + page_size - 1).execute()
            if not r.data: break
            all_rows.extend(r.data)
            if len(r.data) < page_size: break
            offset += page_size
        if not all_rows:
            print("Contract baselines: no data"); return {}
        print(f"Contract baselines: fetched {len(all_rows)} total rows")
        by_question = defaultdict(list)
        for row in all_rows:
            q, rp, ra, slug = row.get("question", ""), row.get("risk_price"), row.get("recorded_at", ""), row.get("contract_slug", "")
            if q and rp is not None: by_question[q].append({"rp": float(rp), "at": ra, "slug": slug})
        now = datetime.now(timezone.utc)
        t_24h, t_48h = now - timedelta(hours=24), now - timedelta(hours=48)
        baselines, immature_details = {}, []
        for q, records in by_question.items():
            n = len(records)
            prices = [r["rp"] for r in records]
            m, s = np.mean(prices), np.std(prices)
            slug = records[0]["slug"] if records else ""
            recent_48h = [r for r in records if r["at"] and dateparser.parse(r["at"]) > t_48h]
            spark_prices = [r["rp"] for r in recent_48h] if recent_48h else prices[-12:]
            if len(spark_prices) > 12:
                step = max(1, len(spark_prices) // 11)
                spark_prices = spark_prices[::step][:11] + [spark_prices[-1]]
            price_24h = None
            for r in records:
                if r["at"]:
                    try:
                        rt = dateparser.parse(r["at"])
                        if rt <= t_24h: price_24h = r["rp"]
                    except: pass
            entry = {"mean": round(m, 4), "std": round(s, 4), "count": n, "slug": slug, "sparkline": spark_prices, "price_24h_ago": price_24h}
            if n < PM_MATURITY_THRESHOLD:
                immature_details.append(f"{q[:40]}({n})"); entry["mature"] = False
            else: entry["mature"] = True
            baselines[q] = entry
        mature_count = sum(1 for v in baselines.values() if v["mature"])
        print(f"Contract baselines: {mature_count} mature out of {len(baselines)} total")
        return baselines
    except Exception as e:
        print(f"Contract baseline fetch error: {e}"); return {}


def calculate_polymarket_score_normalised(contracts, baselines):
    if not contracts:
        return 50.0, {"num_contracts": 0, "total_volume": 0, "highest_prob": None, "threat_count": 0, "deesc_count": 0, "avg_risk_pct": 50.0, "mature_count": 0, "immature_count": 0, "method": "z-score", "agg_z": 0.0, "contract_zscores": []}
    mature, immature_slugs = [], []
    for m in contracts:
        q = m.get("question", "")
        bl = baselines.get(q)
        if bl is None or not bl.get("mature", False):
            immature_slugs.append(q[:40]); continue
        z = (m["risk_price"] - bl["mean"]) / max(bl["std"], PM_STD_FLOOR) if bl["std"] > 0.001 else 0.0
        mature.append({"contract": m, "z": round(z, 3), "baseline": bl})
    tc = sum(1 for m in contracts if not m["is_deescalation"])
    dc = sum(1 for m in contracts if m["is_deescalation"])
    tv = sum(m["volume"] for m in contracts)
    hp = max(contracts, key=lambda x: x["yes_price"]) if contracts else None
    if not mature:
        return 50.0, {"num_contracts": len(contracts), "total_volume": tv, "highest_prob": hp, "avg_risk_pct": 50.0, "threat_count": tc, "deesc_count": dc, "mature_count": 0, "immature_count": len(contracts), "method": "z-score", "agg_z": 0.0, "contract_zscores": []}
    total_vol = sum(m["contract"]["volume"] for m in mature)
    agg_z = sum(m["z"] * m["contract"]["volume"] for m in mature) / total_vol if total_vol > 0 else np.mean([m["z"] for m in mature])
    score = z_to_centred_score(agg_z)
    ar = sum(m["contract"]["risk_price"] * m["contract"]["volume"] for m in mature) / total_vol if total_vol > 0 else np.mean([m["contract"]["risk_price"] for m in mature])
    contract_zscores = []
    total_abs_contrib = sum(abs(m["z"] * m["contract"]["volume"]) for m in mature)
    for m in mature:
        contrib_pct = (abs(m["z"] * m["contract"]["volume"]) / total_abs_contrib * 100) if total_abs_contrib > 0 else 0
        contract_zscores.append({"q": m["contract"]["question"][:60], "z": m["z"], "rp": round(m["contract"]["risk_price"], 4), "mean": m["baseline"]["mean"], "std": m["baseline"]["std"], "n": m["baseline"]["count"], "contrib_pct": round(contrib_pct, 1), "contrib_dir": "up" if m["z"] > 0 else "down" if m["z"] < 0 else "neutral"})
    print(f"PM normalised: {len(mature)} mature, {len(immature_slugs)} immature, agg_z={agg_z:.3f}, score={score}")
    return round(score, 1), {"num_contracts": len(contracts), "total_volume": tv, "highest_prob": hp, "avg_risk_pct": round(ar * 100, 1), "threat_count": tc, "deesc_count": dc, "mature_count": len(mature), "immature_count": len(immature_slugs), "method": "z-score", "agg_z": round(agg_z, 3), "contract_zscores": contract_zscores}


def calculate_polymarket_regional_scores(geo_markets, baselines=None):
    if baselines is None: baselines = {}
    reg = {}
    for rk in REGIONS:
        rc = [m for m in geo_markets if m.get("region") == rk]
        if not rc:
            reg[rk] = {"score": 50.0, "stats": {"num_contracts": 0, "total_volume": 0, "threat_count": 0, "deesc_count": 0, "avg_risk_pct": 50.0, "mature_count": 0, "immature_count": 0, "method": "none"}}
        elif baselines:
            s, st = calculate_polymarket_score_normalised(rc, baselines)
            reg[rk] = {"score": s, "stats": st}
        else:
            avg = np.mean([m["risk_price"] for m in rc]) if rc else 0.5
            s = round(avg * 100, 1)
            reg[rk] = {"score": s, "stats": {"num_contracts": len(rc), "total_volume": 0, "threat_count": 0, "deesc_count": 0, "avg_risk_pct": s, "mature_count": 0, "immature_count": len(rc), "method": "crude"}}
    return reg


# ============================================================
# FINANCIAL MARKETS
# ============================================================

def fetch_market_data():
    results = {}
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    for ticker, info in MARKET_TICKERS.items():
        base = {"name": info["name"], "type": info["type"], "region": info.get("region", "global"), "inverted": info.get("inverted", False)}
        try:
            hist = yf.Ticker(ticker).history(start=start, end=end)
            if hist.empty or len(hist) < 10:
                results[ticker] = {**base, "error": "Insufficient data"}; continue
            hist["pct_change"] = hist["Close"].pct_change() * 100
            ch = hist["pct_change"].dropna()
            if len(ch) < 10:
                results[ticker] = {**base, "error": "Insufficient data"}; continue
            cp, lc = round(hist["Close"].iloc[-1], 2), round(ch.iloc[-1], 2)
            mc, sc = ch.mean(), ch.std()
            z = round((lc - mc) / sc, 2) if sc > 0 else 0.0
            score_z = -z if info.get("inverted", False) else z
            results[ticker] = {**base, "current_price": cp, "daily_change_pct": lc, "z_score": score_z, "score_z": score_z, "normalised_score": z_to_centred_score(score_z), "history_days": len(ch), "sparkline": ch.tail(7).tolist()}
        except Exception as e:
            results[ticker] = {**base, "error": str(e)}
    return results


def calculate_market_score(market_data):
    scores = [d["normalised_score"] for d in market_data.values() if "normalised_score" in d]
    return round(sum(scores) / len(scores), 1) if scores else 50.0


def calculate_market_regional_scores(market_data):
    global_tickers = [t for t, info in MARKET_TICKERS.items() if info.get("region") == "global"]
    global_scores = [market_data[t]["normalised_score"] for t in global_tickers if t in market_data and "normalised_score" in market_data[t]]
    reg = {}
    for rk in REGIONS:
        tks = [t for t, info in MARKET_TICKERS.items() if info.get("region") == rk]
        regional_scores = [market_data[t]["normalised_score"] for t in tks if t in market_data and "normalised_score" in market_data[t]]
        all_scores = regional_scores + global_scores
        all_tickers = [t for t in tks if t in market_data and "normalised_score" in market_data[t]] + [t for t in global_tickers if t in market_data and "normalised_score" in market_data[t]]
        reg[rk] = {"score": round(sum(all_scores) / len(all_scores), 1) if all_scores else 50.0, "tickers": all_tickers}
    return reg


# ============================================================
# ACLED CAST (supplemental — not scored)
# ============================================================

_acled_token_cache = {"access_token": None, "refresh_token": None, "expires_at": 0}

def acled_get_token():
    global _acled_token_cache
    now = datetime.now(timezone.utc).timestamp()
    if _acled_token_cache["access_token"] and now < _acled_token_cache["expires_at"] - 60: return _acled_token_cache["access_token"]
    if _acled_token_cache["refresh_token"]:
        try:
            r = requests.post("https://acleddata.com/oauth/token", data={"refresh_token": _acled_token_cache["refresh_token"], "grant_type": "refresh_token", "client_id": "acled"}, timeout=15)
            if r.status_code == 200:
                d = r.json()
                _acled_token_cache.update({"access_token": d["access_token"], "refresh_token": d.get("refresh_token", _acled_token_cache["refresh_token"]), "expires_at": now + d.get("expires_in", 86400)})
                return _acled_token_cache["access_token"]
        except: pass
    email, pw = os.environ.get("ACLED_EMAIL", ""), os.environ.get("ACLED_PASSWORD", "")
    if not email or not pw: return None
    try:
        r = requests.post("https://acleddata.com/oauth/token", data={"username": email, "password": pw, "grant_type": "password", "client_id": "acled"}, timeout=15)
        r.raise_for_status(); d = r.json()
        _acled_token_cache.update({"access_token": d["access_token"], "refresh_token": d.get("refresh_token"), "expires_at": now + d.get("expires_in", 86400)})
        return _acled_token_cache["access_token"]
    except Exception as e:
        print(f"ACLED auth error: {e}"); return None

def fetch_acled_cast():
    token = acled_get_token()
    if not token: return None, None, None
    now = datetime.now(timezone.utc)
    curr_data = prev_data = curr_period = None
    for off in range(4):
        cd = now - timedelta(days=30 * off)
        yr, mo = str(cd.year), cd.strftime("%B")
        try:
            r = requests.get("https://acleddata.com/api/cast/read", headers={"Authorization": f"Bearer {token}"}, params={"_format": "json", "year": yr, "month": mo, "limit": 50000}, timeout=60)
            r.raise_for_status(); data = r.json().get("data", [])
            if data:
                if curr_data is None: curr_data = data; curr_period = f"{mo} {yr}"
                elif prev_data is None: prev_data = data; break
        except Exception as e: print(f"ACLED CAST error {mo} {yr}: {e}")
    return curr_data, prev_data, curr_period

def sum_cast_forecasts(cast_data):
    total = battles = erv = vac = 0; countries = {}
    for row in cast_data:
        try: t, b, e, v = int(row.get("total_forecast", 0) or 0), int(row.get("battles_forecast", 0) or 0), int(row.get("erv_forecast", 0) or 0), int(row.get("vac_forecast", 0) or 0)
        except: continue
        total += t; battles += b; erv += e; vac += v
        c = row.get("country", "Unknown")
        if c not in countries: countries[c] = {"total": 0, "battles": 0, "erv": 0, "vac": 0}
        countries[c]["total"] += t; countries[c]["battles"] += b; countries[c]["erv"] += e; countries[c]["vac"] += v
    return total, battles, erv, vac, countries

def get_acled_supplemental(current_data, forecast_period):
    if not current_data: return {"error": "No data"}
    ct, cb, ce, cv, cc = sum_cast_forecasts(current_data)
    tc = sorted(cc.items(), key=lambda x: x[1]["total"], reverse=True)[:8]
    return {"total_forecast": ct, "battles_forecast": cb, "erv_forecast": ce, "vac_forecast": cv, "top_countries": tc, "num_countries": len(cc), "forecast_period": forecast_period}

def get_acled_regional_supplemental(current_data):
    reg = {}
    for rk, rc in REGIONS.items():
        rcs = set(rc.get("acled_countries", []))
        cr = [r for r in (current_data or []) if r.get("country", "") in rcs]
        if not cr: reg[rk] = {"total_forecast": 0, "top_countries": [], "num_countries": 0}; continue
        ct, cb, ce, cv, cc = sum_cast_forecasts(cr)
        tc = sorted(cc.items(), key=lambda x: x[1]["total"], reverse=True)[:5]
        reg[rk] = {"total_forecast": ct, "battles_forecast": cb, "erv_forecast": ce, "vac_forecast": cv, "top_countries": tc, "num_countries": len(cc)}
    return reg


# ============================================================
# GDELT — BIGQUERY
# ============================================================

_gdelt_state = {"regional": {}, "last_updated": 0, "fetching": False}

def _get_bigquery_client():
    try:
        creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
        if not creds_json: print("GDELT BigQuery: no GOOGLE_CREDENTIALS"); return None
        creds_path = "/tmp/gcloud_creds.json"
        with open(creds_path, "w") as f: json.dump(creds_json, f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        from google.cloud import bigquery
        return bigquery.Client(project=os.environ.get("GOOGLE_PROJECT_ID", ""))
    except Exception as e: print(f"GDELT BigQuery client error: {e}"); return None

def fetch_gdelt_bigquery_region(client, region_key):
    country_codes = GDELT_COUNTRY_CODES.get(region_key, [])
    if not country_codes: return None
    codes_str = ", ".join(f'"{c}"' for c in country_codes)
    query = f"""
    SELECT PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) as day, AVG(AvgTone) as avg_tone, COUNT(*) as article_count
    FROM `gdelt-bq.gdeltv2.events_partitioned`
    WHERE _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
      AND ActionGeo_CountryCode IN ({codes_str}) AND QuadClass IN (3, 4)
    GROUP BY day ORDER BY day
    """
    try:
        from google.cloud import bigquery as bq
        job_config = bq.QueryJobConfig(maximum_bytes_billed=BQ_MAX_BYTES)
        result = client.query(query, job_config=job_config).result()
        return [{"day": row.day, "tone": float(row.avg_tone), "count": int(row.article_count)} for row in result]
    except Exception as e: print(f"GDELT BigQuery error {region_key}: {e}"); return None

def calculate_gdelt_from_bigquery(rows):
    if not rows or len(rows) < 7: return None, {"error": "Insufficient data"}
    from datetime import date
    cutoff_3d = date.today() - timedelta(days=2)
    recent = [r for r in rows if r["day"] >= cutoff_3d]
    if not recent or len(rows) < 7: return None, {"error": "Insufficient data after filtering"}
    recent_tone = sum(r["tone"] * r["count"] for r in recent) / sum(r["count"] for r in recent)
    baseline_tone = sum(r["tone"] * r["count"] for r in rows) / sum(r["count"] for r in rows)
    baseline_std = np.std([r["tone"] for r in rows])
    z = (recent_tone - baseline_tone) / baseline_std if baseline_std > 0 else 0.0
    recent_vol, baseline_vol = np.mean([r["count"] for r in recent]), np.mean([r["count"] for r in rows])
    baseline_vol_std = np.std([r["count"] for r in rows])
    vol_z = (recent_vol - baseline_vol) / baseline_vol_std if baseline_vol_std > 0 else 0.0
    return z, {"recent_tone": round(recent_tone, 2), "baseline_tone": round(baseline_tone, 2), "baseline_std": round(baseline_std, 2), "z_score": round(z, 2), "vol_z": round(vol_z, 2), "recent_articles": sum(r["count"] for r in recent), "baseline_articles": sum(r["count"] for r in rows), "recent_days": len(recent), "baseline_days": len(rows), "tone_data_points": len(recent), "measurement_window": "72 hours", "baseline_window": "30 days", "source": "BigQuery"}

def fetch_gdelt_regional():
    client = _get_bigquery_client()
    if not client: return {}
    regional_results = {}
    for rk in REGIONS:
        t0 = _time.time(); rows = fetch_gdelt_bigquery_region(client, rk); elapsed = _time.time() - t0
        if rows:
            z, stats = calculate_gdelt_from_bigquery(rows)
            regional_results[rk] = {"z_score": z, "stats": stats}
            print(f"GDELT BQ {rk}: tone z={stats.get('z_score','?')}, vol z={stats.get('vol_z','?')}, {stats.get('recent_articles',0)}/{stats.get('baseline_articles',0)} articles, {elapsed:.1f}s")
        else: regional_results[rk] = {"z_score": None, "stats": {"error": "No data"}}
    return regional_results

def calculate_gdelt_score(z_score, stats):
    if z_score is None: return 50.0
    base = z_to_centred_score(-z_score)
    dp = stats.get("tone_data_points", 0) if isinstance(stats, dict) else 0
    if dp < 12: d = 1 - (dp / 12); base = base * (1 - d) + 50 * d
    return round(base, 1)

def calculate_gdelt_regional_scores(regional_gdelt):
    scores = {}
    for rk, data in regional_gdelt.items():
        scores[rk] = {"score": calculate_gdelt_score(data.get("z_score"), data.get("stats", {})), "stats": data.get("stats", {})}
    for rk in REGIONS:
        if rk not in scores: scores[rk] = {"score": 50.0, "stats": {"error": "No data"}}
    return scores

def _gdelt_background_loop():
    while True:
        if not _gdelt_state["fetching"]:
            _gdelt_state["fetching"] = True
            try:
                t0 = _time.time(); raw = fetch_gdelt_regional()
                if raw:
                    scored = calculate_gdelt_regional_scores(raw)
                    _gdelt_state["regional"] = scored; _gdelt_state["last_updated"] = _time.time()
                    print(f"GDELT background: updated in {_time.time()-t0:.1f}s — " + ", ".join(f"{rk}={v.get('score','?')}" for rk, v in scored.items()))
            except Exception as e: print(f"GDELT background error: {e}"); import traceback; traceback.print_exc()
            finally: _gdelt_state["fetching"] = False
        _time.sleep(7200)


# ============================================================
# GOOGLE TRENDS
# ============================================================

def _gtrends_fetch_batch(pytrends, terms, geo="", retries=2):
    for attempt in range(retries):
        try:
            pytrends.build_payload(terms, cat=0, timeframe="today 1-m", geo=geo)
            df = pytrends.interest_over_time()
            if df is None or df.empty:
                if attempt < retries - 1: _time.sleep(15 * (attempt + 1)); continue
                return {}
            return {term: df[term].tolist() for term in terms if term in df.columns and df[term].tolist()}
        except Exception as e:
            if attempt < retries - 1: _time.sleep(20 * (attempt + 1))
            else: print(f"GTrends failed for {terms[:2]}...: {e}"); return {}
    return {}

def _gtrends_z_score(values, recent_days=7):
    if not values or len(values) < 14: return None
    recent, baseline = values[-recent_days:], values[:-recent_days]
    if len(baseline) < 7: return None
    b_std = np.std(baseline)
    return (np.mean(recent) - np.mean(baseline)) / b_std if b_std > 0 else 0.0

def fetch_gtrends_regional():
    try:
        from pytrends.request import TrendReq
        pytrends = TrendReq(hl="en-US", tz=0)
    except Exception as e: print(f"GTrends init error: {e}"); return {}
    import random
    region_order = list(REGIONS.keys()); random.shuffle(region_order)
    results = {}; total_requests = 0; BATCH_DELAY = 8
    for ri, rk in enumerate(region_order):
        if ri > 0: _time.sleep(10)
        l1_terms = GTRENDS_GLOBAL_TERMS.get(rk, []); l1_zscores, l1_details = [], []
        for i in range(0, len(l1_terms), 5):
            _time.sleep(BATCH_DELAY); total_requests += 1
            batch_result = _gtrends_fetch_batch(pytrends, l1_terms[i:i+5], geo="")
            for term in l1_terms[i:i+5]:
                vals = batch_result.get(term)
                if vals:
                    z = _gtrends_z_score(vals)
                    if z is not None: l1_zscores.append(z); l1_details.append({"term": term, "z": round(z, 2), "type": "global"})
        l2_terms_by_geo = {}
        for ct in GTRENDS_COUNTRY_TERMS.get(rk, []):
            l2_terms_by_geo.setdefault(ct["geo"], []).append(ct)
        l2_zscores, l2_details, l2_alerts = [], [], []
        for geo, country_terms in l2_terms_by_geo.items():
            for i in range(0, len(country_terms), 5):
                batch_cts = country_terms[i:i+5]
                _time.sleep(BATCH_DELAY); total_requests += 1
                batch_result = _gtrends_fetch_batch(pytrends, [ct["term"] for ct in batch_cts], geo=geo)
                for ct in batch_cts:
                    vals = batch_result.get(ct["term"])
                    if vals:
                        z = _gtrends_z_score(vals)
                        if z is not None:
                            l2_zscores.append(z); l2_details.append({"term": ct["label"], "z": round(z, 2), "type": "panic"})
                            if z > 1.5: l2_alerts.append({"term": ct["label"], "z": round(z, 2)})
        l1_avg, l2_avg = np.mean(l1_zscores) if l1_zscores else 0.0, np.mean(l2_zscores) if l2_zscores else 0.0
        l1_valid, l2_valid = len(l1_zscores) >= 2, len(l2_zscores) >= 2
        if l1_valid and l2_valid: combined_z = l1_avg * 0.3 + l2_avg * 0.7
        elif l1_valid: combined_z = l1_avg
        elif l2_valid: combined_z = l2_avg
        else: combined_z = 0.0
        results[rk] = {
            "panic_score": z_to_centred_score(l2_avg) if l2_valid else 50.0,
            "global_score": z_to_centred_score(l1_avg) if l1_valid else 50.0,
            "combined_score": z_to_centred_score(combined_z),
            "combined_z": round(combined_z, 2),
            "layer1_z": round(l1_avg, 2),
            "layer2_z": round(l2_avg, 2),
            "layer1_terms": l1_details,
            "layer2_terms": l2_details,
            "panic_alerts": l2_alerts,
            "l1_count": len(l1_zscores),
            "l2_count": len(l2_zscores),
        }
        print(f"GTrends {rk}: L1={len(l1_zscores)}/{len(l1_terms)}, L2={len(l2_zscores)}/{len(GTRENDS_COUNTRY_TERMS.get(rk, []))}")
    print(f"GTrends: {total_requests} API requests total")
    return results

def _gtrends_background_loop():
    while True:
        if not _gtrends_state["fetching"]:
            _gtrends_state["fetching"] = True
            try:
                t0 = _time.time(); data = fetch_gtrends_regional()
                if data:
                    new_terms = sum(v.get("l1_count", 0) + v.get("l2_count", 0) for v in data.values())
                    old_terms = sum(v.get("l1_count", 0) + v.get("l2_count", 0) for v in _gtrends_state["regional"].values()) if _gtrends_state["regional"] else 0
                    if new_terms >= old_terms * 0.5 or old_terms == 0:
                        _gtrends_state["regional"] = data; _gtrends_state["last_updated"] = _time.time()
                        print(f"GTrends background: updated in {_time.time()-t0:.1f}s ({new_terms} terms)")
                    else: print(f"GTrends background: discarding poor fetch ({new_terms} vs {old_terms})")
            except Exception as e: print(f"GTrends background error: {e}"); import traceback; traceback.print_exc()
            finally: _gtrends_state["fetching"] = False
        _time.sleep(7200)


# ============================================================
# CLOUDFLARE RADAR
# ============================================================

_cloudflare_state = {"regional": {}, "outages": [], "last_updated": 0, "fetching": False}

def fetch_cloudflare_country_traffic(country_code):
    if not CLOUDFLARE_RADAR_TOKEN: return None
    try:
        r = requests.get("https://api.cloudflare.com/client/v4/radar/http/timeseries", headers={"Authorization": f"Bearer {CLOUDFLARE_RADAR_TOKEN}"}, params={"dateRange": "30d", "location": country_code}, timeout=15)
        if r.status_code == 200:
            series = r.json().get("result", {}).get("serie_0", {})
            values = [float(x) for x in series.get("values", []) if x]
            if values and len(values) > 48: return {"timestamps": series.get("timestamps", []), "values": values}
        return None
    except Exception as e: print(f"Cloudflare traffic error {country_code}: {e}"); return None

def fetch_cloudflare_outages():
    if not CLOUDFLARE_RADAR_TOKEN: return []
    try:
        r = requests.get("https://api.cloudflare.com/client/v4/radar/annotations/outages", headers={"Authorization": f"Bearer {CLOUDFLARE_RADAR_TOKEN}"}, params={"limit": 30, "dateRange": "30d"}, timeout=15)
        return r.json().get("result", {}).get("annotations", []) if r.status_code == 200 else []
    except Exception as e: print(f"Cloudflare outages error: {e}"); return []

def calculate_cloudflare_country_zscore(traffic_data):
    if not traffic_data: return None, None
    vals = traffic_data["values"]
    if len(vals) < 72: return None, None
    current_val = vals[-1]
    same_hour_vals = []
    for day_back in range(1, len(vals) // 24):
        idx = len(vals) - 1 - (day_back * 24)
        if 0 <= idx < len(vals):
            same_hour_vals.append(vals[idx])
    if len(same_hour_vals) < 7: return None, None
    mean = np.mean(same_hour_vals)
    std = np.std(same_hour_vals)
    if std < 0.01: return 0.0, {"current": round(current_val, 4), "baseline_avg": round(mean, 4), "baseline_days": len(same_hour_vals)}
    z = (current_val - mean) / std
    detail = {"current": round(current_val, 4), "baseline_avg": round(mean, 4), "baseline_days": len(same_hour_vals)}
    return z, detail

def _cloudflare_dynamic_weight(z):
    if z >= -1.0:
        return 1.0
    excess = -z - 1.0
    return 1.0 + excess ** 2 * 12

def fetch_cloudflare_regional():
    if not CLOUDFLARE_RADAR_TOKEN: return {}, []
    regional_results = {}
    for rk, codes in CLOUDFLARE_COUNTRIES.items():
        country_zscores, country_details = [], []
        for code in codes:
            traffic = fetch_cloudflare_country_traffic(code)
            if traffic:
                z, detail = calculate_cloudflare_country_zscore(traffic)
                if z is not None:
                    country_zscores.append(z)
                    entry = {"code": code, "z": round(z, 2)}
                    if detail:
                        entry["current_hour"] = detail["current"]
                        entry["baseline_hour_avg"] = detail["baseline_avg"]
                        entry["baseline_days"] = detail["baseline_days"]
                    else:
                        entry["current_hour"] = round(traffic["values"][-1], 4)
                        entry["baseline_hour_avg"] = None
                        entry["baseline_days"] = 0
                    country_details.append(entry)
                    if z < -2: print(f"Cloudflare ALERT: {code} z={z:.2f}")
        if country_zscores:
            weights = [_cloudflare_dynamic_weight(z) for z in country_zscores]
            total_w = sum(weights)
            weighted_mean = sum(z * w for z, w in zip(country_zscores, weights)) / total_w
            agg_z = -weighted_mean
            score = z_to_centred_score(agg_z)
        else:
            agg_z, score = 0.0, 50.0
        regional_results[rk] = {"score": score, "agg_z": round(agg_z, 2), "countries_checked": len(codes), "countries_with_data": len(country_zscores), "country_details": country_details}
    outages = fetch_cloudflare_outages()
    region_outages = {rk: [] for rk in REGIONS}
    for outage in outages:
        locs = [l.get("code", "") for l in outage.get("locationsDetails", [])]
        for rk, codes in CLOUDFLARE_COUNTRIES.items():
            if any(loc in codes for loc in locs):
                region_outages[rk].append({"date": (outage.get("startDate") or "")[:16], "end": (outage.get("endDate") or "ongoing")[:16], "location": ", ".join(l.get("name", "?") for l in (outage.get("locationsDetails") or [])), "description": outage.get("description") or "", "type": outage.get("eventType") or "", "scope": outage.get("scope") or ""})
    for rk in regional_results: regional_results[rk]["outages"] = region_outages.get(rk, [])
    return regional_results, outages

def _cloudflare_background_loop():
    while True:
        if not _cloudflare_state["fetching"]:
            _cloudflare_state["fetching"] = True
            try:
                t0 = _time.time(); regional, outages = fetch_cloudflare_regional()
                if regional:
                    _cloudflare_state["regional"] = regional; _cloudflare_state["outages"] = outages; _cloudflare_state["last_updated"] = _time.time()
                    alerts = [f"{c['code']} z={c['z']}" for v in regional.values() for c in v.get("country_details", []) if c["z"] < -1.5]
                    print(f"Cloudflare background: updated in {_time.time()-t0:.1f}s — " + ", ".join(f"{rk}={v.get('score','?')}" for rk, v in regional.items()) + (f" | ALERTS: {', '.join(alerts)}" if alerts else ""))
            except Exception as e: print(f"Cloudflare background error: {e}"); import traceback; traceback.print_exc()
            finally: _cloudflare_state["fetching"] = False
        _time.sleep(3600)


# ============================================================
# COMPOSITE SCORING
# ============================================================

def calculate_regional_composite(pm_score, mkt_score, gt_panic_score=None, gdelt_score=None, gt_global_score=None, cloudflare_score=None):
    """Composite from Stage 2+3 signals only. Stage 1 (Cloudflare) and Stage 4
    (GDELT, GT Global) excluded from composite — shown as separate layers.
    Weights: Polymarket 37.5%, Financial Markets 37.5%, GT Panic 25%."""
    weighted = []
    if pm_score is not None:
        weighted.append((pm_score, 0.375))
    if mkt_score is not None:
        weighted.append((mkt_score, 0.375))
    if gt_panic_score is not None:
        weighted.append((gt_panic_score, 0.25))
    if not weighted:
        return 50.0
    total_w = sum(w for _, w in weighted)
    return round(sum(s * w / total_w for s, w in weighted), 1)

def calculate_regional_composites(pm_reg, mkt_reg, gdelt_reg=None, gtrends_reg=None, cloudflare_reg=None):
    gdelt_reg, gtrends_reg, cloudflare_reg = gdelt_reg or {}, gtrends_reg or {}, cloudflare_reg or {}
    rc = {}
    for rk in REGIONS:
        ps = pm_reg.get(rk, {}).get("score", 50.0)
        ms = mkt_reg.get(rk, {}).get("score", 50.0)
        gs = gdelt_reg.get(rk, {}).get("score", None)
        gt_data = gtrends_reg.get(rk, {})
        gt_panic = gt_data.get("panic_score", None)
        gt_global = gt_data.get("global_score", None)
        cfs = cloudflare_reg.get(rk, {}).get("score", None)
        comp = calculate_regional_composite(ps, ms, gt_panic, gs, gt_global, cfs)
        scored_sigs = {
            "polymarket": ps, "markets": ms,
            "gt_panic": gt_panic if gt_panic is not None else 50.0,
        }
        cl, cd, cc = get_convergence_status(scored_sigs)
        sigs = {
            "polymarket": ps, "markets": ms,
            "gt_panic": gt_panic if gt_panic is not None else 50.0,
            "gdelt": gs if gs is not None else 50.0,
            "gt_global": gt_global if gt_global is not None else 50.0,
            "cloudflare": cfs if cfs is not None else 50.0,
        }
        rc[rk] = {"composite": comp, "colour": get_score_colour(comp), "signals": sigs, "convergence_label": cl, "convergence_detail": cd, "convergence_colour": cc, "pm_stats": pm_reg.get(rk, {}).get("stats", {}), "mkt_tickers": mkt_reg.get(rk, {}).get("tickers", []), "gdelt_stats": gdelt_reg.get(rk, {}).get("stats", {}), "gtrends_stats": gtrends_reg.get(rk, {}), "cloudflare_stats": cloudflare_reg.get(rk, {})}
    return rc

def calculate_dynamic_global_score(regional_composites, global_pm_score=50.0):
    gpm = float(global_pm_score) if global_pm_score is not None else 50.0
    if not regional_composites: return round(gpm, 1), {}
    bw = 1.0 / len(regional_composites); wts = {}; tw = 0
    for rk, d in regional_composites.items():
        s = d["composite"]
        amp = 1.0 + (s - 58) * 0.02 if s > 58 else (1.0 + (42 - s) * 0.01 if s < 42 else 1.0)
        wts[rk] = bw * amp; tw += wts[rk]
    if tw == 0: return 50.0, {}
    rs = sum(regional_composites[r]["composite"] * (wts[r] / tw) for r in regional_composites)
    gs = rs * GLOBAL_REGIONAL_WEIGHT + gpm * GLOBAL_PM_WEIGHT
    return round(float(max(0, min(100, gs))), 1), {rk: round(wts[rk] / tw, 3) for rk in wts}


# ============================================================
# >>> PART 1 ENDS HERE — HTML TEMPLATE IS IN PART 2 <<<
# >>> In main.py, the next line is: DASHBOARD_HTML = """
# >>> Paste Part 2 content below this point
# ============================================================

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Geopolitical Risk Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#08080d;color:#f0f0f0;min-height:100vh;border-top:3px solid {{ risk_colour }}}
        .ctr{max-width:900px;margin:0 auto;padding:0 16px}

        /* Ticker Strip */
        .ticker{background:#06060a;border-bottom:1px solid #1a1a2e;padding:6px 20px;overflow:hidden;white-space:nowrap}
        .ticker-inner{display:flex;gap:32px;font-size:0.65em;color:#888;animation:tickerScroll 30s linear infinite}
        .ticker:hover .ticker-inner{animation-play-state:paused}
        @keyframes tickerScroll{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
        .ticker-alert{color:#f87171}
        .ticker-move{color:#ef4444}

        /* Header */
        header{text-align:center;padding:20px 0 8px}
        h1{font-size:1.3em;font-weight:700;letter-spacing:3px;color:#fff}
        .sub{color:#888;font-size:0.75em;margin-top:4px}
        .live-counter{color:#555;font-size:0.6em;margin-top:2px}

        /* Tabs */
        .tabs{display:flex;gap:4px;margin-bottom:16px;background:#111118;border-radius:12px;padding:4px;border:1px solid #1e1e2e}
        .tab{flex:1;padding:8px 6px;text-align:center;border-radius:8px;cursor:pointer;font-size:0.7em;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:#666;transition:all 0.2s}
        .tab:hover{color:#ccc;background:rgba(255,255,255,0.04)}
        .tab.active{background:#1a1a2e;color:#fff}
        .tab-score{display:block;font-size:1.6em;font-weight:800;margin-top:2px}
        .view{display:none}.view.active{display:block}

        /* Score Card */
        .sc{background:linear-gradient(145deg,#111118,#0d0d14);border:2px solid #333;border-radius:16px;padding:28px 24px;text-align:center;margin-bottom:16px;position:relative;overflow:hidden}
        .sc-glow{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:300px;height:300px;border-radius:50%;filter:blur(80px);opacity:0.15;pointer-events:none}
        .sc-num{font-size:4.5em;font-weight:800;line-height:1;position:relative;text-shadow:0 0 30px rgba(255,255,255,0.15)}
        .sc-label{font-size:1em;font-weight:700;letter-spacing:3px;margin-top:4px;position:relative}
        .sc-conv{font-size:0.8em;font-weight:600;letter-spacing:1px;margin-top:6px;position:relative}
        .sc-conv span{font-weight:400;opacity:0.7}
        .sc-sub{color:#888;font-size:0.7em;margin-top:6px;position:relative}

        /* Gradient Bar */
        .bar{width:100%;height:20px;position:relative;margin-top:12px}
        .bar-track{position:absolute;top:50%;left:0;right:0;height:5px;border-radius:3px;background:linear-gradient(to right,#059669 0%,#34d399 22%,#b0b0b0 50%,#eab308 65%,#f97316 80%,#ef4444 90%,#dc2626 100%);transform:translateY(-50%);opacity:0.75}
        .bar-mid{position:absolute;top:50%;left:50%;width:1px;height:14px;background:#444;transform:translate(-50%,-50%);z-index:1}
        .bar-dot{position:absolute;top:50%;width:14px;height:14px;border-radius:50%;transform:translate(-50%,-50%);z-index:2;border:2px solid rgba(255,255,255,0.25);transition:left 0.8s ease}

        /* Stage Indicators */
        .si{display:flex;gap:3px;align-items:center;justify-content:center;margin-top:8px}
        .si-dot{display:flex;align-items:center;gap:4px;padding:3px 8px;border-radius:4px;font-size:0.55em;font-weight:700;letter-spacing:1px;color:#888}
        .si-dot .dot{width:6px;height:6px;border-radius:50%}
        .si-quiet{background:#111118;border:1px solid #1e1e2e}
        .si-quiet .dot{background:#555}
        .si-active{background:rgba(251,191,36,0.08);border:1px solid rgba(251,191,36,0.25)}
        .si-active .dot{background:#fbbf24;box-shadow:0 0 6px #fbbf24}
        .si-alert{background:rgba(248,113,113,0.08);border:1px solid rgba(248,113,113,0.25)}
        .si-alert .dot{background:#f87171;box-shadow:0 0 6px #f87171}
        .si-alert .dot{animation:pulse 2s ease-in-out infinite}
        @keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 6px currentColor}50%{opacity:0.6;box-shadow:0 0 12px currentColor}}

        /* Region Cards */
        .rgrid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:16px}
        @media(max-width:600px){.rgrid{grid-template-columns:1fr}}
        .rcard{background:#111118;border:2px solid #333;border-radius:12px;padding:14px;text-align:center;cursor:pointer;transition:all 0.2s}
        .rcard:hover{border-color:#555;transform:translateY(-2px)}
        .rcard-name{font-size:0.6em;text-transform:uppercase;letter-spacing:1.5px;color:#eee;font-weight:600}
        .rcard-score{font-size:2em;font-weight:800}
        .rcard-conv{font-size:0.6em;font-weight:600;margin-top:2px}
        .rcard-wt{font-size:0.55em;color:#555;margin-top:4px}

        /* Stage Sections */
        .stage{margin-bottom:12px;background:#0a0a12;border:1px solid #1a1a2e;border-radius:12px;overflow:hidden;transition:border-color 0.3s}
        .stage-head{padding:12px 16px;cursor:pointer;display:flex;justify-content:space-between;align-items:center}
        .stage-icon{font-size:1.1em;margin-right:10px}
        .stage-title{font-size:0.65em;font-weight:700;color:#eee;letter-spacing:1.5px}
        .stage-desc{font-size:0.6em;color:#666;margin-top:2px}
        .stage-status{font-size:0.6em;font-weight:700;letter-spacing:1px;padding:3px 10px;border-radius:4px}
        .stage-body{padding:0 16px 14px;display:none}
        .stage.open .stage-body{display:block}
        .stage-arrow{color:#555;font-size:0.8em;transition:transform 0.2s}
        .stage.open .stage-arrow{transform:rotate(90deg)}
        .stage-ew{font-size:0.5em;padding:2px 6px;border-radius:3px;background:rgba(249,115,22,0.12);color:#f97316;font-weight:700;letter-spacing:0.5px;margin-left:8px}

        /* Signal Cards */
        .sig{background:#0c0c14;border:1px solid #1e1e2e;border-radius:10px;padding:14px 16px;margin-bottom:8px;transition:border-color 0.3s}
        .sig-head{display:flex;justify-content:space-between;align-items:flex-start}
        .sig-name{font-size:0.75em;font-weight:700;color:#eee;letter-spacing:1.5px;text-transform:uppercase}
        .sig-src{font-size:0.55em;color:#888;margin-top:2px}
        .sig-score{font-size:1.8em;font-weight:800;line-height:1}
        .sig-detail{font-size:0.7em;color:#aaa;margin-top:6px}
        .sig-alert{font-size:0.7em;color:#f87171;margin-top:3px}

        /* Detail Dropdowns */
        .dd{background:#08080d;border:1px solid #1a1a2e;border-radius:10px;margin-top:8px;overflow:hidden}
        .dd-toggle{font-size:0.7em;color:#aaa;padding:10px 14px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;font-weight:600;letter-spacing:1px;text-transform:uppercase}
        .dd-toggle:hover{color:#eee}
        .dd-body{padding:0 14px 12px;display:none}
        .dd.open .dd-body{display:block}
        .dd-arrow{color:#555;font-size:0.7em;transition:transform 0.2s}
        .dd.open .dd-arrow{transform:rotate(90deg)}

        /* Ticker/Contract Rows */
        .trow{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:1px solid #111118}
        .trow:last-child{border-bottom:none}
        .trow-name{font-size:0.85em;color:#fff;font-weight:500}
        .trow-sub{font-size:0.65em;color:#888}
        .trow-stats{display:flex;gap:14px;align-items:center}
        .trow-immature{opacity:0.45}
        .trow-spark{min-width:60px;text-align:center}
        .trow-val{font-size:0.85em;font-weight:700;min-width:55px;text-align:right}
        .trow-change{font-size:0.75em;font-weight:600;min-width:50px;text-align:right}
        .trow-z{font-size:0.7em;color:#888;min-width:50px;text-align:right}
        .trow-wt{font-size:0.6em;min-width:40px;text-align:right}
        .badge{font-size:0.55em;padding:2px 6px;border-radius:4px;font-weight:600;letter-spacing:0.5px;margin-left:6px}
        .badge-threat{background:#3a1a1a;color:#ef4444}
        .badge-deesc{background:#1a2a1a;color:#22c55e}
        .badge-mature{background:#1a1a2e;color:#818cf8}
        .badge-immature{background:#1a1a1a;color:#555}
        .col-header{font-size:0.6em;color:#666;text-transform:uppercase;letter-spacing:1px}

        /* Graph */
        .graph-ctrl{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:10px;padding:0 4px}
        .graph-tgl{display:flex;align-items:center;gap:5px;font-size:0.7em;color:#aaa;cursor:pointer;user-select:none}
        .graph-tgl input{cursor:pointer}
        .tgl-sw{display:inline-block;width:12px;height:3px;border-radius:2px}
        .tf-btn{background:#1a1a2e;border:1px solid #333;color:#888;padding:4px 12px;border-radius:6px;font-size:0.65em;cursor:pointer;font-weight:600;letter-spacing:0.5px;transition:all 0.2s}
        .tf-btn:hover{color:#ccc;border-color:#555}
        .tf-btn.active{background:#2a2a4e;color:#eee;border-color:#555}

        /* Supplemental */
        .supp{background:#0a0a12;border:1px solid #1a1a2e;border-radius:12px;margin-bottom:12px;overflow:hidden}
        .supp-head{padding:12px 16px;cursor:pointer;display:flex;justify-content:space-between;align-items:center}
        .supp.open .dd-body{display:block}
        .supp-badge{font-size:0.5em;padding:2px 6px;border-radius:3px;background:#1a1a2e;color:#666;font-weight:700}

        /* Methodology link */
        .meth-link{display:block;text-align:center;padding:12px;color:#555;font-size:0.7em;text-decoration:none;letter-spacing:1px}
        .meth-link:hover{color:#888}

        footer{text-align:center;padding:16px 0;color:#444;font-size:0.65em}
        .err{background:#1a0a0a;border:1px solid #3a1a1a;border-radius:12px;padding:20px;text-align:center;color:#ef4444}
    </style>
</head>
<body>
{% if error %}
<div class="ctr"><div class="err" style="margin-top:40px">⚠ {{ error }}</div></div>
{% else %}

<!-- Ticker Strip -->
<div class="ticker">
    <div class="ticker-inner" id="ticker-content">
        {% for a in alerts[:10] %}
        {% if a.type == 'panic' %}<span>🔴 <span class="ticker-alert">{{ a.text }}</span> ({{ a.region|replace('_',' ')|title }})</span>
        {% elif a.type == 'cloudflare' %}<span>⚠ <span class="ticker-alert">{{ a.text }}</span> connectivity drop</span>
        {% elif a.type == 'contract' %}<span>{% if a.risk_rising is defined and not a.risk_rising %}▼ <span style="color:#22c55e">{{ a.text }}</span>{% else %}▲ <span class="ticker-move">{{ a.text }}</span>{% endif %}</span>
        {% endif %}
        {% endfor %}
        {% if not alerts %}<span style="color:#444">Monitoring — no active alerts</span>{% endif %}
    </div>
</div>

<div class="ctr">
    <header>
        <h1>GEOPOLITICAL RISK INDEX</h1>
        <div class="sub">3 Regions • Stage-Based Monitoring • Live</div>
        <div class="live-counter" id="live-counter">Live — just updated</div>
    </header>

    <!-- Tab Bar -->
    <div class="tabs">
        <div class="tab active" onclick="switchView('global')">Global<span class="tab-score" style="color:{{ risk_colour }}">{{ composite_score }}</span></div>
        {% for rk, rd in regional_composites.items() %}
        <div class="tab" onclick="switchView('{{ rk }}')">{{ regions[rk].name }}<span class="tab-score" style="color:{{ rd.colour }}">{{ rd.composite }}</span></div>
        {% endfor %}
    </div>

    <!-- ==================== GLOBAL VIEW ==================== -->
    <div class="view active" id="view-global">
        <div class="sc" style="border-color:{{ risk_colour }}">
            <div class="sc-glow" style="background:{{ risk_colour }}"></div>
            <div class="sc-num" style="color:{{ risk_colour }}">{{ composite_score }}</div>
            <div class="sc-label" style="color:{{ risk_colour }}">{{ risk_level }}</div>
            <div class="sc-sub">Dynamic regional aggregate — 50 = normal baseline</div>
            <div class="bar"><div class="bar-track"></div><div class="bar-mid"></div>
                <div class="bar-dot" style="left:{{ composite_score }}%;background:{{ risk_colour }};box-shadow:0 0 10px {{ risk_colour }}44"></div></div>
        </div>

        <div class="rgrid">
            {% for rk, rd in regional_composites.items() %}
            <div class="rcard" id="rcard-{{ rk }}" style="border-color:{{ rd.colour }}33" onclick="switchView('{{ rk }}')">
                <div class="rcard-name">{{ regions[rk].name }}</div>
                <div class="rcard-score" style="color:{{ rd.colour }}">{{ rd.composite }}</div>
                <div class="rcard-conv" style="color:{{ rd.convergence_colour }}">{{ rd.convergence_label }}</div>
                <div class="si">
                    {% set s1_max = rd.signals.cloudflare %}
                    {% set s2_max = rd.signals.gt_panic %}
                    {% set s3_max = [rd.signals.polymarket, rd.signals.markets]|max %}
                    {% set s4_max = [rd.signals.gdelt, rd.signals.gt_global]|max %}
                    {% for sv in [s1_max, s2_max, s3_max, s4_max] %}
                    <div class="si-dot {% if sv >= 70 %}si-alert{% elif sv >= 58 %}si-active{% else %}si-quiet{% endif %}">
                        S{{ loop.index }}<div class="dot"></div>
                    </div>
                    {% endfor %}
                </div>
                <div class="bar" style="height:14px;margin-top:6px"><div class="bar-track" style="height:3px"></div><div class="bar-mid" style="height:8px"></div>
                    <div class="bar-dot" style="left:{{ rd.composite }}%;background:{{ rd.colour }};width:10px;height:10px;box-shadow:0 0 8px {{ rd.colour }}44"></div></div>
                <div class="rcard-wt">Weight: {{ (regional_weights.get(rk, 0.333) * 100)|round(1) }}%</div>
            </div>
            {% endfor %}
        </div>

        <!-- Global PM -->
        {% if global_pm_score is defined %}
        <div style="background:#111118;border:1px solid #1e1e2e;border-radius:12px;padding:12px 18px;margin-bottom:14px;display:flex;justify-content:space-between;align-items:center">
            <div>
                <div style="font-size:0.65em;text-transform:uppercase;letter-spacing:1.5px;color:#eee;font-weight:600">Global Polymarket Contracts</div>
                <div style="font-size:0.65em;color:#888;margin-top:2px">{{ global_pm_stats.num_contracts }} contracts ({{ global_pm_stats.mature_count }} mature) • {{ (GLOBAL_PM_WEIGHT*100)|int }}% of global score{% if global_pm_stats.agg_z is defined %} • z={{ global_pm_stats.agg_z }}{% endif %}</div>
            </div>
            <div style="font-size:1.6em;font-weight:800;color:{{ global_pm_colour }}">{{ global_pm_score }}</div>
        </div>
        {% endif %}

        <!-- Global Contracts Dropdown -->
        {% if global_contracts %}
        <div class="dd" id="dd-global-contracts">
            <div class="dd-toggle" onclick="toggleDD('dd-global-contracts')">Global Risk Contracts — {{ global_contracts|length }} tracked <span class="dd-arrow">▸</span></div>
            <div class="dd-body">
                <div class="trow" style="border-bottom:2px solid #1a1a2e;padding-bottom:4px;margin-bottom:4px">
                    <div><span class="col-header">Contract</span></div>
                    <div class="trow-stats">
                        <div class="trow-spark"><span class="col-header">48h</span></div>
                        <div class="trow-val"><span class="col-header" style="font-weight:400">Odds</span></div>
                        <div class="trow-change"><span class="col-header" style="font-weight:400">24h Δ</span></div>
                        <div class="trow-z"><span class="col-header">Z</span></div>
                        <div class="trow-wt"><span class="col-header">Weight</span></div>
                    </div>
                </div>
                {% for c in global_contracts %}
                <div class="trow{% if not c.is_mature %} trow-immature{% endif %}">
                    <div style="flex:1;min-width:0">
                        <div style="font-size:0.82em;color:#eee;line-height:1.4;padding-right:10px">{{ c.question }}
                            {% if c.is_deescalation %}<span class="badge badge-deesc">DE-ESC</span>{% else %}<span class="badge badge-threat">THREAT</span>{% endif %}
                            {% if c.is_mature %}<span class="badge badge-mature">MATURE ({{ c.dp_count }})</span>{% else %}<span class="badge badge-immature">IMMATURE ({{ c.dp_count }})</span>{% endif %}
                        </div>
                    </div>
                    <div class="trow-stats">
                        <div class="trow-spark">{{ c.sparkline_svg|safe }}</div>
                        <div class="trow-val" style="color:{{ c.colour }}">{{ c.probability_pct }}%</div>
                        {% if c.change_24h is not none %}<div class="trow-change" style="color:{{ c.change_colour }}">{% if c.change_24h > 0 %}+{% endif %}{{ c.change_24h }}pp</div>{% else %}<div class="trow-change" style="color:#333">—</div>{% endif %}
                        {% if c.z_score is not none %}<div class="trow-z">{{ c.z_score }}</div>{% else %}<div class="trow-z" style="color:#333">—</div>{% endif %}
                        {% if c.contrib_pct is not none %}<div class="trow-wt" style="color:{% if c.contrib_dir == 'up' %}#f97316{% elif c.contrib_dir == 'down' %}#22c55e{% else %}#555{% endif %}">{{ c.contrib_pct }}%{% if c.contrib_dir == 'up' %}↑{% elif c.contrib_dir == 'down' %}↓{% endif %}</div>{% else %}<div class="trow-wt" style="color:#333">—</div>{% endif %}
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}

        <!-- Score History -->
        {% if score_history and score_history|length > 1 %}
        <div class="dd open" id="dd-global-history">
            <div class="dd-toggle" onclick="toggleDD('dd-global-history')">Score History ({{ score_history|length }} snapshots) <span class="dd-arrow">▸</span></div>
            <div class="dd-body">
                <div class="graph-ctrl">
                    <label class="graph-tgl"><input type="checkbox" id="tgl-composite" checked onchange="redrawGlobalChart()"><span class="tgl-sw" style="background:#eab308"></span> Global</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-europe" onchange="redrawGlobalChart()"><span class="tgl-sw" style="background:#818cf8"></span> Europe</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-middle_east" onchange="redrawGlobalChart()"><span class="tgl-sw" style="background:#fbbf24"></span> M.East</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-asia_pacific" onchange="redrawGlobalChart()"><span class="tgl-sw" style="background:#34d399"></span> Asia-Pac</label>
                </div>
                <div style="display:flex;gap:6px;margin-bottom:10px">
                    <button class="tf-btn active" onclick="setGlobalTF('48h')">48h</button>
                    <button class="tf-btn" onclick="setGlobalTF('1w')">1W</button>
                    <button class="tf-btn" onclick="setGlobalTF('1m')">1M</button>
                    <button class="tf-btn" onclick="setGlobalTF('all')">All</button>
                </div>
                <div style="width:100%;height:200px;position:relative"><canvas id="globalChart"></canvas></div>
            </div>
        </div>
        {% endif %}

        <!-- ACLED Supplemental -->
        {% if acled_supplemental and acled_supplemental.top_countries is defined %}
        <div class="supp" id="dd-acled-global">
            <div class="supp-head" onclick="toggleDD('dd-acled-global')">
                <div style="display:flex;align-items:center;gap:10px">
                    <span>📋</span>
                    <span style="font-size:0.65em;font-weight:700;color:#888;letter-spacing:1.5px">ACLED CONFLICT FORECAST — {{ acled_supplemental.forecast_period }}</span>
                    <span class="supp-badge">SUPPLEMENTAL</span>
                </div>
                <span class="dd-arrow">▸</span>
            </div>
            <div class="dd-body">
                <div style="font-size:0.7em;color:#666;margin-bottom:8px">Monthly forecast — shown for context, does not affect scores.</div>
                {% for cn, ct in acled_supplemental.top_countries %}
                <div class="trow"><div class="trow-name">{{ cn }} <span style="font-size:0.7em;color:#666;margin-left:6px">⚔{{ ct.battles }} 💥{{ ct.erv }} 👤{{ ct.vac }}</span></div>
                    <div style="font-size:0.85em;font-weight:600;color:#f97316">{{ "{:,}".format(ct.total) }}</div></div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
    </div>

    <!-- ==================== REGIONAL VIEWS ==================== -->
    {% for rk, rd in regional_composites.items() %}
    <div class="view" id="view-{{ rk }}">
        <!-- Region Score Card -->
        <div class="sc" style="border-color:{{ rd.colour }}">
            <div class="sc-glow" style="background:{{ rd.colour }}"></div>
            <div class="sc-num" style="color:{{ rd.colour }}">{{ rd.composite }}</div>
            <div class="sc-label" style="color:{{ rd.colour }}">{{ regions[rk].name|upper }}</div>
            <div class="sc-conv" style="color:{{ rd.convergence_colour }}">{{ rd.convergence_label }}
                {% if rd.convergence_detail %}<span>— {{ rd.convergence_detail }}</span>{% endif %}</div>
            <div class="si">
                {% set s1_max = rd.signals.cloudflare %}
                {% set s2_max = rd.signals.gt_panic %}
                {% set s3_max = [rd.signals.polymarket, rd.signals.markets]|max %}
                {% set s4_max = [rd.signals.gdelt, rd.signals.gt_global]|max %}
                {% for sv in [s1_max, s2_max, s3_max, s4_max] %}
                <div class="si-dot {% if sv >= 70 %}si-alert{% elif sv >= 58 %}si-active{% else %}si-quiet{% endif %}">
                    S{{ loop.index }}<div class="dot"></div>
                </div>
                {% endfor %}
            </div>
            <div class="sc-sub">Composite: 37.5% Polymarket • 37.5% Financial Markets • 25% Panic Search • Stage 1 & 4 = context</div>
            <div class="bar"><div class="bar-track"></div><div class="bar-mid"></div>
                <div class="bar-dot" style="left:{{ rd.composite }}%;background:{{ rd.colour }};box-shadow:0 0 10px {{ rd.colour }}44"></div></div>
        </div>

        <!-- STAGE 1 — PHYSICAL OBSERVABLE -->
        {% set cfs = rd.cloudflare_stats %}
        {% set s1_status = 'ALERT' if rd.signals.cloudflare >= 70 else ('ACTIVE' if rd.signals.cloudflare >= 58 else 'QUIET') %}
        {% set s1_col = '#f87171' if s1_status == 'ALERT' else ('#fbbf24' if s1_status == 'ACTIVE' else '#555') %}
        <div class="stage {% if s1_status != 'QUIET' %}open{% endif %}" id="stage-{{ rk }}-1" style="border-color:{{ s1_col }}44">
            <div class="stage-head" onclick="toggleStage('stage-{{ rk }}-1')">
                <div style="display:flex;align-items:center">
                    <span class="stage-icon">⚡</span>
                    <div>
                        <div class="stage-title">STAGE 1 — PHYSICAL OBSERVABLE <span class="stage-ew">EARLY WARNING</span></div>
                        <div class="stage-desc">Infrastructure disruptions detectable before anyone reports them</div>
                    </div>
                </div>
                <div style="display:flex;align-items:center;gap:10px">
                    <span class="stage-status" style="color:{{ s1_col }};background:{{ s1_col }}18;border:1px solid {{ s1_col }}33">{{ s1_status }}</span>
                    <span class="stage-arrow">▸</span>
                </div>
            </div>
            <div class="stage-body">
                <div class="sig" style="border-color:{{ rd.cloudflare_colour }}33">
                    <div class="sig-head">
                        <div><div class="sig-name">Internet Connectivity</div><div class="sig-src">Cloudflare Radar • Early Warning — Not Scored</div></div>
                        <div class="sig-score" style="color:{{ rd.cloudflare_colour }}">{{ rd.signals.cloudflare }}</div>
                    </div>
                    {% if cfs and cfs.countries_with_data is defined and cfs.countries_with_data > 0 %}
                    <div class="sig-detail">{{ cfs.countries_with_data }}/{{ cfs.countries_checked }} countries • agg z={{ cfs.agg_z }}</div>
                    {% if cfs.outages %}{% for o in cfs.outages[:2] %}
                    <div class="sig-alert" style="word-break:break-word">🔴 {{ o.date[:10] }} — {{ o.location }}: {{ o.description }}</div>
                    {% endfor %}{% endif %}
                    {% elif cloudflare_fetching %}
                    <div class="sig-detail" style="color:#fbbf24">⏳ Fetching Cloudflare data...</div>
                    {% else %}<div class="sig-detail" style="color:#666">Waiting for first fetch cycle</div>{% endif %}
                    <div class="bar" style="height:14px"><div class="bar-track" style="height:3px"></div><div class="bar-mid" style="height:8px"></div>
                        <div class="bar-dot" style="left:{{ rd.signals.cloudflare }}%;background:{{ rd.cloudflare_colour }};width:10px;height:10px"></div></div>
                </div>
                <!-- Cloudflare Country Detail -->
                {% if cfs and cfs.country_details is defined and cfs.country_details %}
                <div class="dd" id="dd-{{ rk }}-cf">
                    <div class="dd-toggle" onclick="toggleDD('dd-{{ rk }}-cf')">Country Details — {{ cfs.countries_with_data }}/{{ cfs.countries_checked }} <span class="dd-arrow">▸</span></div>
                    <div class="dd-body">
                        <div style="font-size:0.65em;color:#666;margin-bottom:6px">Current hour vs same hour over prior 29 days</div>
                        {% for c in cfs.country_details|sort(attribute='z') %}
                        <div class="trow">
                            <div><div class="trow-name">{% if c.z < -1.5 %}🔴 {% endif %}{{ country_names.get(c.code, c.code) }}</div><div class="trow-sub">{{ c.code }} • this hour: {{ c.current_hour }}{% if c.baseline_hour_avg %} • 29d avg: {{ c.baseline_hour_avg }}{% endif %}</div></div>
                            <div class="trow-z" style="color:{% if c.z < -1.5 %}#f87171{% elif c.z < -0.5 %}#ef4444{% elif c.z > 0.5 %}#22c55e{% else %}#888{% endif %}">z={{ c.z }}</div>
                        </div>
                        {% endfor %}
                        {% if cfs.outages %}
                        <div style="font-size:0.65em;color:#666;margin:10px 0 6px">VERIFIED OUTAGES — Cloudflare Radar Outage Center</div>
                        {% for o in cfs.outages %}
                        <div class="trow">
                            <div><div class="trow-name" style="color:#f87171">{{ o.location }}</div><div class="trow-sub">{{ o.date }} — {{ o.end }}{% if o.scope %} • {{ o.scope }}{% endif %}</div></div>
                            <div style="font-size:0.7em;color:#aaa;max-width:250px;word-break:break-word">{{ o.description }}</div>
                        </div>
                        {% endfor %}{% endif %}
                    </div>
                </div>
                {% endif %}
            </div>
        </div>

        <!-- STAGE 2 — GROUND SIGNAL -->
        {% set gts = rd.gtrends_stats %}
        {% set s2_status = 'ALERT' if rd.signals.gt_panic >= 70 else ('ACTIVE' if rd.signals.gt_panic >= 58 else 'QUIET') %}
        {% set s2_col = '#f87171' if s2_status == 'ALERT' else ('#fbbf24' if s2_status == 'ACTIVE' else '#555') %}
        <div class="stage {% if s2_status != 'QUIET' %}open{% endif %}" id="stage-{{ rk }}-2" style="border-color:{{ s2_col }}44">
            <div class="stage-head" onclick="toggleStage('stage-{{ rk }}-2')">
                <div style="display:flex;align-items:center">
                    <span class="stage-icon">📡</span>
                    <div>
                        <div class="stage-title">STAGE 2 — GROUND SIGNAL</div>
                        <div class="stage-desc">Local populations and early observers reacting</div>
                    </div>
                </div>
                <div style="display:flex;align-items:center;gap:10px">
                    <span class="stage-status" style="color:{{ s2_col }};background:{{ s2_col }}18;border:1px solid {{ s2_col }}33">{{ s2_status }}</span>
                    <span class="stage-arrow">▸</span>
                </div>
            </div>
            <div class="stage-body">
                <div class="sig" style="border-color:{{ rd.gt_panic_colour }}33">
                    <div class="sig-head">
                        <div><div class="sig-name">Panic Search Trends</div><div class="sig-src">Google Trends • Weight: 25%</div></div>
                        <div class="sig-score" style="color:{{ rd.gt_panic_colour }}">{{ rd.signals.gt_panic }}</div>
                    </div>
                    {% if gts and gts.l2_count is defined and gts.l2_count > 0 %}
                    <div class="sig-detail">Panic terms z={{ gts.layer2_z }} ({{ gts.l2_count }}/{{ gtrends_expected.get(rk, {}).get('l2', '?') }})</div>
                    {% if gts.panic_alerts %}{% for alert in gts.panic_alerts %}
                    <div class="sig-alert">🔴 {{ alert.term }}: z={{ alert.z }}</div>
                    {% endfor %}{% endif %}
                    {% elif gtrends_fetching and not gtrends_regional %}
                    <div class="sig-detail" style="color:#fbbf24">⏳ Fetching panic search data...</div>
                    {% else %}<div class="sig-detail" style="color:#666">Waiting for first fetch cycle</div>{% endif %}
                    <div class="bar" style="height:14px"><div class="bar-track" style="height:3px"></div><div class="bar-mid" style="height:8px"></div>
                        <div class="bar-dot" style="left:{{ rd.signals.gt_panic }}%;background:{{ rd.gt_panic_colour }};width:10px;height:10px"></div></div>
                </div>
                <!-- Panic Terms Detail -->
                {% if gts and gts.layer2_terms is defined and gts.layer2_terms %}
                <div class="dd" id="dd-{{ rk }}-panic">
                    <div class="dd-toggle" onclick="toggleDD('dd-{{ rk }}-panic')">Panic Terms — {{ gts.l2_count }}/{{ gtrends_expected.get(rk, {}).get('l2', '?') }} succeeded <span class="dd-arrow">▸</span></div>
                    <div class="dd-body">
                        {% set ns = namespace(shown=0) %}
                        {% for t in gts.layer2_terms %}
                        {% if t.z|abs > 0.3 %}{% set ns.shown = ns.shown + 1 %}
                        <div class="trow">
                            <div><div class="trow-name">{% if t.z > 1.5 %}🔴 {% endif %}{{ t.term }}</div><div class="trow-sub">panic indicator</div></div>
                            <div class="trow-z" style="color:{% if t.z > 1.5 %}#f87171{% elif t.z > 0.5 %}#ef4444{% elif t.z < -0.5 %}#22c55e{% else %}#888{% endif %}">z={{ t.z }}</div>
                        </div>
                        {% endif %}
                        {% endfor %}
                        {% if ns.shown == 0 %}<div style="font-size:0.7em;color:#555;padding:8px 0">All {{ gts.l2_count }} terms within normal range (|z| < 0.3)</div>
                        {% elif ns.shown < gts.l2_count %}<div style="font-size:0.7em;color:#555;padding:8px 0">{{ gts.l2_count - ns.shown }} more term{{ 's' if gts.l2_count - ns.shown != 1 }} within normal range</div>{% endif %}
                    </div>
                </div>
                {% endif %}
            </div>
        </div>

        <!-- STAGE 3 — MARKET POSITIONING -->
        {% set rps = rd.pm_stats %}
        {% set s3_pm = rd.signals.polymarket %}
        {% set s3_mkt = rd.signals.markets %}
        {% set s3_status = 'ALERT' if s3_pm >= 70 or s3_mkt >= 70 else ('ACTIVE' if s3_pm >= 58 or s3_mkt >= 58 else 'QUIET') %}
        {% set s3_col = '#f87171' if s3_status == 'ALERT' else ('#fbbf24' if s3_status == 'ACTIVE' else '#555') %}
        {% set s3_diverging = (s3_pm >= 58 and s3_mkt <= 42) or (s3_mkt >= 58 and s3_pm <= 42) or (s3_pm >= 70 and s3_mkt <= 50) or (s3_mkt >= 70 and s3_pm <= 50) %}
        <div class="stage open" id="stage-{{ rk }}-3" style="border-color:{{ s3_col }}44">
            <div class="stage-head" onclick="toggleStage('stage-{{ rk }}-3')">
                <div style="display:flex;align-items:center">
                    <span class="stage-icon">📈</span>
                    <div>
                        <div class="stage-title">STAGE 3 — MARKET POSITIONING</div>
                        <div class="stage-desc">Informed money moving</div>
                    </div>
                </div>
                <div style="display:flex;align-items:center;gap:10px">
                    <span class="stage-status" style="color:{{ s3_col }};background:{{ s3_col }}18;border:1px solid {{ s3_col }}33">{{ s3_status }}{% if s3_diverging %} <span style="color:#c084fc" title="Signals within this stage are diverging">↕</span>{% endif %}</span>
                    <span class="stage-arrow">▸</span>
                </div>
            </div>
            <div class="stage-body">
                <!-- Polymarket -->
                <div class="sig" style="border-color:{{ rd.pm_colour }}33">
                    <div class="sig-head">
                        <div><div class="sig-name">Polymarket</div><div class="sig-src">Prediction Markets • Weight: 37.5%</div></div>
                        <div class="sig-score" style="color:{{ rd.pm_colour }}">{{ s3_pm }}</div>
                    </div>
                    <div class="sig-detail">{{ rps.num_contracts }} contracts ({{ rps.threat_count }} threat, {{ rps.deesc_count }} de-esc) • {{ rps.mature_count }}/{{ rps.num_contracts }} mature</div>
                    <div class="sig-detail">Avg risk: {{ rps.avg_risk_pct }}% • ${{ "{:,.0f}".format(rps.total_volume) }} vol{% if rps.agg_z is defined %} • z={{ rps.agg_z }}{% endif %}</div>
                    <div class="bar" style="height:14px"><div class="bar-track" style="height:3px"></div><div class="bar-mid" style="height:8px"></div>
                        <div class="bar-dot" style="left:{{ s3_pm }}%;background:{{ rd.pm_colour }};width:10px;height:10px"></div></div>
                </div>
                <!-- Financial Markets -->
                <div class="sig" style="border-color:{{ rd.mkt_colour }}33">
                    <div class="sig-head">
                        <div><div class="sig-name">Financial Markets</div><div class="sig-src">yfinance • Weight: 37.5%</div></div>
                        <div class="sig-score" style="color:{{ rd.mkt_colour }}">{{ s3_mkt }}</div>
                    </div>
                    <div class="sig-detail">{{ rd.mkt_tickers|length }} tickers • today vs 90-day baseline</div>
                    <div class="bar" style="height:14px"><div class="bar-track" style="height:3px"></div><div class="bar-mid" style="height:8px"></div>
                        <div class="bar-dot" style="left:{{ s3_mkt }}%;background:{{ rd.mkt_colour }};width:10px;height:10px"></div></div>
                </div>

                <!-- Contract List -->
                {% set reg_contracts = regional_contracts.get(rk, []) %}
                {% if reg_contracts %}
                <div class="dd" id="dd-{{ rk }}-contracts">
                    <div class="dd-toggle" onclick="toggleDD('dd-{{ rk }}-contracts')">{{ regions[rk].name }} Contracts — {{ reg_contracts|length }} tracked <span class="dd-arrow">▸</span></div>
                    <div class="dd-body">
                        <div class="trow" style="border-bottom:2px solid #1a1a2e;padding-bottom:4px;margin-bottom:4px">
                            <div><span class="col-header">Contract</span></div>
                            <div class="trow-stats"><div class="trow-spark"><span class="col-header">48h</span></div><div class="trow-val"><span class="col-header" style="font-weight:400">Odds</span></div><div class="trow-change"><span class="col-header" style="font-weight:400">24h Δ</span></div><div class="trow-z"><span class="col-header">Z</span></div><div class="trow-wt"><span class="col-header">Weight</span></div></div>
                        </div>
                        {% for c in reg_contracts %}
                        <div class="trow{% if not c.is_mature %} trow-immature{% endif %}">
                            <div style="flex:1;min-width:0"><div style="font-size:0.82em;color:#eee;line-height:1.4;padding-right:10px">{{ c.question }}
                                {% if c.is_deescalation %}<span class="badge badge-deesc">DE-ESC</span>{% else %}<span class="badge badge-threat">THREAT</span>{% endif %}
                                {% if c.is_mature %}<span class="badge badge-mature">MATURE ({{ c.dp_count }})</span>{% else %}<span class="badge badge-immature">IMMATURE ({{ c.dp_count }})</span>{% endif %}
                            </div></div>
                            <div class="trow-stats">
                                <div class="trow-spark">{{ c.sparkline_svg|safe }}</div>
                                <div class="trow-val" style="color:{{ c.colour }}">{{ c.probability_pct }}%</div>
                                {% if c.change_24h is not none %}<div class="trow-change" style="color:{{ c.change_colour }}">{% if c.change_24h > 0 %}+{% endif %}{{ c.change_24h }}pp</div>{% else %}<div class="trow-change" style="color:#333">—</div>{% endif %}
                                {% if c.z_score is not none %}<div class="trow-z">{{ c.z_score }}</div>{% else %}<div class="trow-z" style="color:#333">—</div>{% endif %}
                                {% if c.contrib_pct is not none %}<div class="trow-wt" style="color:{% if c.contrib_dir == 'up' %}#f97316{% elif c.contrib_dir == 'down' %}#22c55e{% else %}#555{% endif %}">{{ c.contrib_pct }}%{% if c.contrib_dir == 'up' %}↑{% elif c.contrib_dir == 'down' %}↓{% endif %}</div>{% else %}<div class="trow-wt" style="color:#333">—</div>{% endif %}
                            </div>
                        </div>
                        {% endfor %}
                    </div>
                </div>
                {% endif %}

                <!-- Ticker List -->
                {% if rd.mkt_tickers %}
                <div class="dd" id="dd-{{ rk }}-tickers">
                    <div class="dd-toggle" onclick="toggleDD('dd-{{ rk }}-tickers')">Financial Tickers — {{ rd.mkt_tickers|length }} tracked <span class="dd-arrow">▸</span></div>
                    <div class="dd-body">
                        {% for tk in rd.mkt_tickers %}{% if tk in market_data and market_data[tk].error is not defined %}{% set d = market_data[tk] %}
                        <div class="trow">
                            <div><div class="trow-name">{{ d.name }}{% if d.inverted %} <span style="font-size:0.6em;color:#f97316">▼INV</span>{% endif %}{% if d.region == 'global' %} <span style="font-size:0.6em;color:#666">⊕</span>{% endif %}</div><div class="trow-sub">{{ tk }} • {{ d.type }}</div></div>
                            <div class="trow-stats">
                                <div class="trow-spark">{{ d.sparkline_svg|safe }}</div>
                                <div style="font-size:0.8em;color:#bbb">{{ d.current_price }}</div>
                                <div class="trow-change" style="color:{{ d.change_colour }}">{% if d.daily_change_pct > 0 %}+{% endif %}{{ d.daily_change_pct }}%</div>
                                <div class="trow-z">z={{ d.z_score }}</div>
                            </div>
                        </div>
                        {% endif %}{% endfor %}
                    </div>
                </div>
                {% endif %}
            </div>
        </div>

        <!-- STAGE 4 — NARRATIVE FORMATION -->
        {% set gs = rd.gdelt_stats %}
        {% set gts2 = rd.gtrends_stats %}
        {% set s4_status = 'ALERT' if rd.signals.gdelt >= 70 or rd.signals.gt_global >= 70 else ('ACTIVE' if rd.signals.gdelt >= 58 or rd.signals.gt_global >= 58 else 'QUIET') %}
        {% set s4_col = '#f87171' if s4_status == 'ALERT' else ('#fbbf24' if s4_status == 'ACTIVE' else '#555') %}
        {% set s4_diverging = (rd.signals.gdelt >= 58 and rd.signals.gt_global <= 42) or (rd.signals.gt_global >= 58 and rd.signals.gdelt <= 42) or (rd.signals.gdelt >= 70 and rd.signals.gt_global <= 50) or (rd.signals.gt_global >= 70 and rd.signals.gdelt <= 50) %}
        <div class="stage {% if s4_status != 'QUIET' %}open{% endif %}" id="stage-{{ rk }}-4" style="border-color:{{ s4_col }}44">
            <div class="stage-head" onclick="toggleStage('stage-{{ rk }}-4')">
                <div style="display:flex;align-items:center">
                    <span class="stage-icon">📰</span>
                    <div>
                        <div class="stage-title">STAGE 4 — NARRATIVE FORMATION</div>
                        <div class="stage-desc">Media and global public catching up</div>
                    </div>
                </div>
                <div style="display:flex;align-items:center;gap:10px">
                    <span class="stage-status" style="color:{{ s4_col }};background:{{ s4_col }}18;border:1px solid {{ s4_col }}33">{{ s4_status }}{% if s4_diverging %} <span style="color:#c084fc" title="Signals within this stage are diverging">↕</span>{% endif %}</span>
                    <span class="stage-arrow">▸</span>
                </div>
            </div>
            <div class="stage-body">
                <div class="sig" style="border-color:{{ rd.gdelt_colour }}33">
                    <div class="sig-head">
                        <div><div class="sig-name">GDELT News Tone</div><div class="sig-src">BigQuery • Context Only — Not Scored</div></div>
                        <div class="sig-score" style="color:{{ rd.gdelt_colour }}">{{ rd.signals.gdelt }}</div>
                    </div>
                    {% if gs and gs.recent_tone is defined %}
                    <div class="sig-detail">72h tone: {{ gs.recent_tone }} vs 30d: {{ gs.baseline_tone }} • Tone z={{ gs.z_score }} • Vol z={{ gs.vol_z }}</div>
                    <div class="sig-detail" style="color:#555">{{ gs.recent_articles|default(0) }} recent / {{ gs.baseline_articles|default(0) }} baseline • {{ gs.source|default('API') }}</div>
                    {% elif gdelt_fetching %}<div class="sig-detail" style="color:#fbbf24">⏳ Fetching GDELT data...</div>
                    {% elif gs and gs.error is defined %}<div class="sig-detail" style="color:#ef4444">{{ gs.error }}</div>
                    {% endif %}
                    <div class="bar" style="height:14px"><div class="bar-track" style="height:3px"></div><div class="bar-mid" style="height:8px"></div>
                        <div class="bar-dot" style="left:{{ rd.signals.gdelt }}%;background:{{ rd.gdelt_colour }};width:10px;height:10px"></div></div>
                </div>
                <div class="sig" style="border-color:{{ rd.gt_global_colour }}33">
                    <div class="sig-head">
                        <div><div class="sig-name">Global Search Trends</div><div class="sig-src">Google Trends • Context Only — Not Scored</div></div>
                        <div class="sig-score" style="color:{{ rd.gt_global_colour }}">{{ rd.signals.gt_global }}</div>
                    </div>
                    {% if gts2 and gts2.l1_count is defined %}
                    <div class="sig-detail">Global terms z={{ gts2.layer1_z }} ({{ gts2.l1_count }}/{{ gtrends_expected.get(rk, {}).get('l1', '?') }})</div>
                    {% elif gtrends_fetching %}<div class="sig-detail" style="color:#fbbf24">⏳ Fetching global search data...</div>
                    {% else %}<div class="sig-detail" style="color:#666">Waiting for first fetch cycle</div>{% endif %}
                    <div class="bar" style="height:14px"><div class="bar-track" style="height:3px"></div><div class="bar-mid" style="height:8px"></div>
                        <div class="bar-dot" style="left:{{ rd.signals.gt_global }}%;background:{{ rd.gt_global_colour }};width:10px;height:10px"></div></div>
                </div>

                <!-- Global Terms Detail -->
                {% if gts2 and gts2.layer1_terms is defined and gts2.layer1_terms %}
                <div class="dd" id="dd-{{ rk }}-global-terms">
                    <div class="dd-toggle" onclick="toggleDD('dd-{{ rk }}-global-terms')">Global Terms — {{ gts2.l1_count }}/{{ gtrends_expected.get(rk, {}).get('l1', '?') }} <span class="dd-arrow">▸</span></div>
                    <div class="dd-body">
                        {% set ns = namespace(shown=0) %}
                        {% for t in gts2.layer1_terms %}{% if t.z|abs > 0.3 %}{% set ns.shown = ns.shown + 1 %}
                        <div class="trow">
                            <div><div class="trow-name">{{ t.term }}</div><div class="trow-sub">worldwide • conflict term</div></div>
                            <div class="trow-z" style="color:{% if t.z > 0.5 %}#ef4444{% elif t.z < -0.5 %}#22c55e{% else %}#888{% endif %}">z={{ t.z }}</div>
                        </div>
                        {% endif %}{% endfor %}
                        {% if ns.shown == 0 %}<div style="font-size:0.7em;color:#555;padding:8px 0">All {{ gts2.l1_count }} terms within normal range (|z| < 0.3)</div>
                        {% elif ns.shown < gts2.l1_count %}<div style="font-size:0.7em;color:#555;padding:8px 0">{{ gts2.l1_count - ns.shown }} more term{{ 's' if gts2.l1_count - ns.shown != 1 }} within normal range</div>{% endif %}
                    </div>
                </div>
                {% endif %}
            </div>
        </div>

        <!-- SUPPLEMENTAL -->
        {% set racled = acled_regional.get(rk, {}) %}
        {% if racled and racled.top_countries is defined and racled.top_countries %}
        <div class="supp" id="dd-{{ rk }}-acled">
            <div class="supp-head" onclick="toggleDD('dd-{{ rk }}-acled')">
                <div style="display:flex;align-items:center;gap:10px">
                    <span>📋</span>
                    <span style="font-size:0.65em;font-weight:700;color:#888;letter-spacing:1.5px">ACLED FORECAST — {{ regions[rk].name|upper }}</span>
                    <span class="supp-badge">SUPPLEMENTAL</span>
                </div>
                <span class="dd-arrow">▸</span>
            </div>
            <div class="dd-body">
                {% for cn, ct in racled.top_countries %}
                <div class="trow"><div class="trow-name">{{ cn }} <span style="font-size:0.7em;color:#666;margin-left:6px">⚔{{ ct.battles }} 💥{{ ct.erv }} 👤{{ ct.vac }}</span></div>
                    <div style="font-size:0.85em;font-weight:600;color:#f97316">{{ "{:,}".format(ct.total) }}</div></div>
                {% endfor %}
            </div>
        </div>
        {% endif %}

        <!-- Regional Score History -->
        {% set rh = regional_history.get(rk, []) %}
        {% if rh and rh|length > 1 %}
        <div class="dd open" id="dd-{{ rk }}-history">
            <div class="dd-toggle" onclick="toggleDD('dd-{{ rk }}-history')">Score History ({{ rh|length }} snapshots) <span class="dd-arrow">▸</span></div>
            <div class="dd-body">
                <div class="graph-ctrl">
                    <label class="graph-tgl"><input type="checkbox" id="tgl-{{ rk }}-comp" checked onchange="drawRC('{{ rk }}')"><span class="tgl-sw" style="background:{{ rd.fixed_colour }}"></span> Composite</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-{{ rk }}-pm" onchange="drawRC('{{ rk }}')"><span class="tgl-sw" style="background:#3b82f6"></span> PM</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-{{ rk }}-mkt" onchange="drawRC('{{ rk }}')"><span class="tgl-sw" style="background:#8b5cf6"></span> Mkt</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-{{ rk }}-gdelt" onchange="drawRC('{{ rk }}')"><span class="tgl-sw" style="background:#22c55e"></span> GDELT</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-{{ rk }}-panic" onchange="drawRC('{{ rk }}')"><span class="tgl-sw" style="background:#f97316"></span> Panic</label>
                    <label class="graph-tgl"><input type="checkbox" id="tgl-{{ rk }}-cf" onchange="drawRC('{{ rk }}')"><span class="tgl-sw" style="background:#06b6d4"></span> Internet</label>
                </div>
                <div style="display:flex;gap:6px;margin-bottom:10px">
                    <button class="tf-btn active" onclick="setRTF('{{ rk }}','48h')">48h</button>
                    <button class="tf-btn" onclick="setRTF('{{ rk }}','1w')">1W</button>
                    <button class="tf-btn" onclick="setRTF('{{ rk }}','1m')">1M</button>
                    <button class="tf-btn" onclick="setRTF('{{ rk }}','all')">All</button>
                </div>
                <div style="width:100%;height:180px;position:relative"><canvas id="chart-{{ rk }}"></canvas></div>
            </div>
        </div>
        {% endif %}
    </div>
    {% endfor %}

    <a class="meth-link" href="#" onclick="document.getElementById('methodology').style.display=document.getElementById('methodology').style.display==='none'?'block':'none';return false;">▸ METHODOLOGY</a>
    <div id="methodology" style="display:none;background:#0a0a12;border:1px solid #1a1a2e;border-radius:12px;padding:20px;margin-bottom:16px;font-size:0.75em;color:#888;line-height:1.7">

        <p style="color:#ccc;margin-bottom:16px">This dashboard measures how unusual current geopolitical conditions are compared to recent history. It does not predict whether specific events will happen — it detects when multiple independent systems are showing unusual activity simultaneously. A score of 50 means "normal." Higher means conditions are more tense than usual; lower means calmer than usual.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">🔗 THE INFORMATION FLOW CHAIN</p>
        <p style="margin-bottom:8px">The dashboard is organised around how geopolitical information typically propagates through different systems. During a developing crisis, signals tend to activate in a predictable sequence:</p>
        <p style="margin-bottom:4px"><strong style="color:#eee">Stage 1 — Physical Observable:</strong> Infrastructure disruptions detectable before anyone reports them. Internet connectivity drops, flight corridors empty, power grids go dark. These are the earliest possible signals — they happen in the physical world before human reporting catches up.</p>
        <p style="margin-bottom:4px"><strong style="color:#eee">Stage 2 — Ground Signal:</strong> Local populations and early observers react. People in affected countries search for "bomb shelter," "evacuation," and "VPN." OSINT accounts begin posting. This is the first human-generated signal layer.</p>
        <p style="margin-bottom:4px"><strong style="color:#eee">Stage 3 — Market Positioning:</strong> Informed money moves. Prediction market contracts shift as traders with information or analytical edge reposition. Defence stocks rise, regional currencies weaken, commodity prices adjust.</p>
        <p style="margin-bottom:8px"><strong style="color:#eee">Stage 4 — Narrative Formation:</strong> Media and the global public catch up. News tone shifts across thousands of articles. Worldwide search interest spikes as the broader public seeks information about the crisis.</p>
        <p style="margin-bottom:16px">The composite score is derived from <strong style="color:#eee">Stage 2 and Stage 3 signals only</strong> — these are the signals most likely to move early and most directly connected to geopolitical events. Stage 1 (Cloudflare internet connectivity) is shown as a separate early warning indicator because internet traffic patterns are influenced by non-geopolitical factors (religious observances, weekends, seasonal patterns) that inject noise into baseline scoring. Stage 4 (GDELT news tone, global search trends) is shown as context because these signals lag during early escalation — during a rapidly developing crisis, waiting for media narrative to shift actively suppresses the composite when earlier stages have already detected genuine signals. When signals within a multi-signal stage diverge significantly (one elevated, another depressed), a purple ↕ indicator appears next to the stage status, prompting you to expand the stage and examine the individual components.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">📊 HOW THE SCORE WORKS</p>
        <p style="margin-bottom:16px">Every signal is measured as a z-score — a statistical measure of how far something has moved from its recent average, expressed in standard deviations. A z-score of 0 means "exactly average." A z-score of +2 means "unusually high — this level occurs less than 5% of the time." Z-scores are converted to the 0–100 scale where 50 = normal, 65 ≈ 1 standard deviation above normal, 80 ≈ 2 standard deviations. The same logic applies in reverse: 35 ≈ 1 standard deviation below normal (calmer than usual).</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">⚡ STAGE 1 — INTERNET CONNECTIVITY (Early Warning)</p>
        <p style="margin-bottom:8px">Cloudflare Radar monitors internet traffic volumes across countries worldwide. During geopolitical crises, internet connectivity often drops — governments impose shutdowns, infrastructure is damaged by military action, or populations lose power. The dashboard tracks hourly traffic for 27 countries across the three regions, comparing the current hour's traffic to the same hour on each of the prior 29 days. This hour-matching accounts for normal daily usage patterns, and the 30-day baseline ensures robust standard deviations.</p>
        <p style="margin-bottom:16px">Country scores are aggregated using dynamic weighting rather than a simple average — a single country losing internet entirely receives amplified weight so it is not drowned out by neighbouring countries with normal connectivity. The amplification follows a quadratic curve: small deviations barely trigger, but severe drops escalate rapidly. Internet connectivity is shown as a separate early warning indicator rather than scored because traffic patterns are influenced by non-geopolitical factors (religious observances like Ramadan, weekends, seasonal patterns) that inject noise into baseline scoring.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">📡 STAGE 2 — PANIC SEARCH TRENDS (Weight: 25%)</p>
        <p style="margin-bottom:8px">Google Trends measures what people are searching for in real-time. The dashboard tracks country-specific panic terms — searches from within affected countries for terms like "VPN", "bomb shelter", "evacuation", "conscription", and "martial law". These are leading indicators — populations search for these terms when they feel personally threatened, often before international media fully covers a developing situation. A spike in "bomb shelter" searches from Israel, or "VPN" searches from Iran, is a qualitatively different signal from global news interest.</p>
        <p style="margin-bottom:16px">Panic terms that spike above 1.5 standard deviations are flagged as <span style="color:#f87171">PANIC ALERTS</span> on the dashboard. Terms are measured as the last 7 days vs the prior 30-day baseline, with a minimum of 2 successful terms required before the signal contributes to scoring.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">📈 STAGE 3 — POLYMARKET (Weight: 37.5%)</p>
        <p style="margin-bottom:8px">Polymarket is a prediction market where people bet real money on whether events will happen. When a contract shows "39% chance of Iran ceasefire breaking by March," that number represents the aggregate belief of thousands of traders putting money behind their predictions. Because real money is at stake, prediction markets tend to be more accurate than polls or pundit opinions.</p>
        <p style="margin-bottom:8px">The dashboard tracks dozens of geopolitical contracts across three regions. Each contract is classified as either a <strong style="color:#ef4444">THREAT</strong> (war, strikes, invasions — higher probability = more risk) or <strong style="color:#22c55e">DE-ESCALATION</strong> (ceasefires, peace deals, disarmament — higher probability = less risk). De-escalation contracts have their probabilities inverted so that all contracts are expressed on a common "risk" scale.</p>
        <p style="margin-bottom:8px">Each contract is scored against its own 7-day rolling baseline using z-scores. This means the dashboard doesn't react to the absolute level of a contract (a 10% invasion probability isn't inherently alarming) but rather to how much it has changed relative to its own recent history. A contract jumping from 3% to 8% produces a much stronger signal than one sitting steady at 40%.</p>
        <p style="margin-bottom:16px">Contracts require a minimum of 50 historical data points before being included in scoring. New contracts appear as <span style="color:#555">IMMATURE</span> on the dashboard and begin contributing to the score after approximately 8 hours of data collection. This prevents a brand-new contract from immediately skewing the regional score before a reliable baseline exists. A minimum volatility threshold of 1 percentage point is also applied — contracts that have barely moved don't generate disproportionate z-scores from statistical noise.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">📈 STAGE 3 — FINANCIAL MARKETS (Weight: 37.5%)</p>
        <p style="margin-bottom:8px">Geopolitical tension leaves fingerprints in financial markets. When conflict risk rises, certain assets tend to move in predictable ways: defence stocks rise (Rheinmetall, BAE Systems, Elbit Systems) as investors anticipate increased military spending. Commodities spike (oil, gold, natural gas) as supply disruption fears grow and investors seek safe havens. Regional equity indices fall (Germany, Israel, Taiwan) as local economic risk increases.</p>
        <p style="margin-bottom:16px">The dashboard tracks 21 tickers across the three regions plus 2 global tickers (Gold and US Defence ETF) that feed into every region. Each ticker's daily percentage move is compared to its own 90-day rolling average using z-scores — the same "how unusual is today compared to recent history" approach. Regional index ETFs have their z-scores <span style="color:#f97316">▼ INVERTED</span> so that a falling market registers as elevated risk. Tickers marked <span style="color:#666">⊕ GLOBAL</span> contribute to all three regional scores equally.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">📰 STAGE 4 — GDELT NEWS TONE (Context Only)</p>
        <p style="margin-bottom:8px">The GDELT Project monitors news media worldwide and measures the average "tone" of articles. The dashboard queries GDELT's full event database via Google BigQuery, filtering to conflict-related events only (verbal conflict such as threats and demands, and material conflict such as military action and armed clashes) geolocated to countries in each region. This captures thousands of conflict-specific articles per day across dozens of countries.</p>
        <p style="margin-bottom:16px">The dashboard compares the last 72 hours of conflict news tone against the 30-day baseline. It also tracks article volume — a sudden spike in the number of conflict articles about a region, even without a tone change, indicates increased attention. GDELT is shown as context rather than scored because it reflects the media cycle rather than real-time positioning — it confirms what faster signals have already detected but lags during early escalation.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">🔍 STAGE 4 — GLOBAL SEARCH TRENDS (Context Only)</p>
        <p style="margin-bottom:16px">Worldwide search interest for region-specific conflict keywords (e.g. "Iran strike", "Russia Ukraine war", "China Taiwan"). When a crisis develops, search volume for these terms spikes as the global public seeks information. Measured as the last 7 days vs the 30-day baseline. Shown as context rather than scored because global search interest lags behind ground-level panic indicators and market positioning.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">📋 SUPPLEMENTAL — ACLED CONFLICT FORECASTS</p>
        <p style="margin-bottom:16px">ACLED's CAST system provides monthly conflict event forecasts for countries worldwide. Because these forecasts only update monthly, they are shown as supplemental context — they do not affect the score.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">🗺️ REGIONAL & GLOBAL SCORING</p>
        <p style="margin-bottom:8px">Each region (Europe, Middle East, Asia-Pacific) scores three signals — Polymarket (37.5%), Financial Markets (37.5%), and Panic Search Trends (25%). The global score is 85% weighted regional aggregate plus 15% global-only Polymarket contracts (nuclear detonation, world war, and other contracts that don't belong to a single region). Regions with elevated scores receive amplified weighting in the global aggregate — a regional crisis pulls the global score up more than proportionally.</p>
        <p style="margin-bottom:16px">The convergence indicator shows whether the scored signals agree. A single elevated signal could be noise — but when Polymarket, financial markets, and panic searches all move in the same direction simultaneously, that's a much stronger signal. The indicator labels range from Quiet (all normal) through Early Signal (one signal elevated) to Broad Escalation (all signals elevated).</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">📋 READING THE CONTRACT LIST</p>
        <p style="margin-bottom:16px">Each Polymarket contract row shows: the contract question, its classification (<span style="color:#ef4444">THREAT</span> or <span style="color:#22c55e">DE-ESC</span>), maturity status with data point count (<span style="color:#818cf8">MATURE</span> or <span style="color:#555">IMMATURE</span>), a 48-hour sparkline showing recent price movement, the current probability, 24-hour change in percentage points (green = risk decreasing, red = risk increasing — contextual to whether the contract is a threat or de-escalation), the contract's z-score against its own 7-day baseline, and its weight contribution to the regional Polymarket score.</p>

        <p style="color:#eee;font-weight:700;font-size:1.1em;margin-bottom:8px;letter-spacing:1px">⚠️ LIMITATIONS</p>
        <p>This dashboard detects deviation from recent baseline, not absolute danger. A score of 70 during a period that has been generally calm means something different than 70 during an ongoing crisis — in the latter case, "normal" is already elevated, so 70 means conditions are unusual even by crisis standards. Polymarket contracts on geopolitical events tend to be thinly traded compared to financial markets, so individual contract moves can be noisy. The 7-day baseline and 24-hour change can tell different stories — a contract might show a positive 24h change but a negative z-score if the recent move is a partial recovery from a larger weekly decline. GDELT measures media narrative, not ground truth — media amplification cycles can produce false signals. Financial markets move for many non-geopolitical reasons (earnings, monetary policy, macro data), so an elevated Financial Markets signal in isolation is weaker evidence of geopolitical risk than an elevated Polymarket signal.</p>
    </div>

    <footer>Geopolitical Risk Dashboard v1.0 • Stage-Based Monitoring • Data: Polymarket + yfinance + GDELT BigQuery + Google Trends + Cloudflare Radar + ACLED CAST</footer>
</div>
{% endif %}

<script>
const _cd={{ score_history|tojson }};
const _rh={{ regional_history|tojson }};
const _rc={'europe':'#818cf8','middle_east':'#fbbf24','asia_pacific':'#34d399'};
Chart.defaults.color='#888';Chart.defaults.font.family="-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif";Chart.defaults.font.size=11;
let _gc=null,_gtf='48h',_rcs={},_rtfs={};

function toggleDD(id){const el=document.getElementById(id);if(el)el.classList.toggle('open');}
function toggleStage(id){const el=document.getElementById(id);if(el)el.classList.toggle('open');}

function filterTF(data,tf,dk){
    if(tf==='all')return data;const now=new Date();let co;
    if(tf==='48h')co=new Date(now-48*3600000);else if(tf==='1w')co=new Date(now-7*86400000);else if(tf==='1m')co=new Date(now-30*86400000);else return data;
    return data.filter(d=>new Date(d[dk])>=co);
}
function fmtL(ds){const d=new Date(ds);return d.toLocaleDateString('en-GB',{day:'numeric',month:'short'})+' '+d.toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'});}
function fmtS(ds){return new Date(ds).toLocaleDateString('en-GB',{day:'numeric',month:'short'});}

function setGlobalTF(tf){_gtf=tf;document.querySelectorAll('#dd-global-history .tf-btn').forEach(b=>b.classList.remove('active'));event.target.classList.add('active');redrawGlobalChart();}
function setRTF(rk,tf){_rtfs[rk]=tf;const v=document.getElementById('dd-'+rk+'-history');if(v)v.querySelectorAll('.tf-btn').forEach(b=>b.classList.remove('active'));event.target.classList.add('active');drawRC(rk);}

function redrawGlobalChart(){
    const cv=document.getElementById('globalChart');if(!cv)return;if(_gc){_gc.destroy();_gc=null;}
    const fd=filterTF(_cd,_gtf,'recorded_at');if(!fd.length)return;
    const labels=fd.map(d=>fmtS(d.recorded_at)),ds=[];
    if(document.getElementById('tgl-composite')&&document.getElementById('tgl-composite').checked)
        ds.push({label:'Global',data:fd.map(d=>d.composite_score),borderColor:'#eab308',borderWidth:2.5,pointRadius:0,pointHitRadius:8,tension:0.3,fill:false});
    ['europe','middle_east','asia_pacific'].forEach(rk=>{
        const tg=document.getElementById('tgl-'+rk);if(!tg||!tg.checked)return;
        const rd=filterTF(_rh[rk]||[],_gtf,'recorded_at');if(!rd.length)return;
        const gt=fd.map(d=>new Date(d.recorded_at).getTime());
        const al=gt.map(g=>{let cl=null,md=Infinity;for(const r of rd){const df=Math.abs(new Date(r.recorded_at).getTime()-g);if(df<md){md=df;cl=r;}}return cl&&md<7200000?cl.score:null;});
        ds.push({label:rk.replace('_',' '),data:al,borderColor:_rc[rk],borderWidth:1.5,pointRadius:0,pointHitRadius:8,tension:0.3,spanGaps:true,fill:false});
    });
    _gc=new Chart(cv,{type:'line',data:{labels,datasets:ds},options:{responsive:true,maintainAspectRatio:false,animation:{duration:300},interaction:{mode:'index',intersect:false},plugins:{legend:{display:false},tooltip:{backgroundColor:'#1a1a2e',borderColor:'#333',borderWidth:1,titleColor:'#eee',bodyColor:'#ccc',callbacks:{title:i=>i[0]?fmtL(fd[i[0].dataIndex].recorded_at):'',label:i=>' '+i.dataset.label+': '+(i.parsed.y!==null?i.parsed.y.toFixed(1):'—')}}},scales:{x:{grid:{color:'#1a1a2e'},ticks:{maxTicksLimit:6,color:'#555',font:{size:9}}},y:{min:0,max:100,grid:{color:ctx=>ctx.tick.value===50?'#333':'#111118'},ticks:{stepSize:25,color:'#555',callback:v=>v===50?'50':v}}}}});
}

function drawRC(rk){
    const cv=document.getElementById('chart-'+rk);if(!cv)return;if(_rcs[rk]){_rcs[rk].destroy();_rcs[rk]=null;}
    const tf=_rtfs[rk]||'48h',rd=filterTF(_rh[rk]||[],tf,'recorded_at');if(!rd.length)return;
    const labels=rd.map(d=>fmtS(d.recorded_at)),ds=[],rc=_rc[rk]||'#eab308';
    [{id:'comp',key:'score',colour:rc,w:2.5,l:'Composite'},{id:'pm',key:'pm',colour:'#3b82f6',w:1.5,l:'PM'},{id:'mkt',key:'mkt',colour:'#8b5cf6',w:1.5,l:'Mkt'},{id:'gdelt',key:'gdelt',colour:'#22c55e',w:1.5,l:'GDELT'},{id:'panic',key:'gt_panic',colour:'#f97316',w:1.5,l:'Panic'},{id:'cf',key:'cloudflare',colour:'#06b6d4',w:1.5,l:'Internet'}].forEach(t=>{
        const el=document.getElementById('tgl-'+rk+'-'+t.id);if(!el||!el.checked)return;
        const vals=rd.map(d=>d[t.key]);if(vals.every(v=>v===null||v===undefined))return;
        ds.push({label:t.l,data:vals,borderColor:t.colour,borderWidth:t.w,pointRadius:0,pointHitRadius:8,tension:0.3,spanGaps:true,fill:false});
    });
    _rcs[rk]=new Chart(cv,{type:'line',data:{labels,datasets:ds},options:{responsive:true,maintainAspectRatio:false,animation:{duration:300},interaction:{mode:'index',intersect:false},plugins:{legend:{display:false},tooltip:{backgroundColor:'#1a1a2e',borderColor:'#333',borderWidth:1,titleColor:'#eee',bodyColor:'#ccc',callbacks:{title:i=>i[0]?fmtL(rd[i[0].dataIndex].recorded_at):'',label:i=>' '+i.dataset.label+': '+(i.parsed.y!==null?i.parsed.y.toFixed(1):'—')}}},scales:{x:{grid:{color:'#1a1a2e'},ticks:{maxTicksLimit:6,color:'#555',font:{size:9}}},y:{min:0,max:100,grid:{color:ctx=>ctx.tick.value===50?'#333':'#111118'},ticks:{stepSize:25,color:'#555',callback:v=>v===50?'50':v}}}}});
}

function switchView(vi){
    document.querySelectorAll('.view').forEach(v=>v.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
    document.getElementById('view-'+vi).classList.add('active');
    const tabs=document.querySelectorAll('.tab'),keys=['global',{% for rk in regional_composites %}'{{rk}}',{% endfor %}];
    const idx=keys.indexOf(vi);if(idx>=0&&tabs[idx])tabs[idx].classList.add('active');
    setTimeout(()=>{if(vi==='global')redrawGlobalChart();else drawRC(vi);},100);
}

// ==================== LIVE UPDATES ====================
let _lastUp=Date.now();
function animateScore(el,nv){
    if(!el)return;const cur=parseFloat(el.textContent)||0,tgt=parseFloat(nv)||0,diff=tgt-cur;
    if(Math.abs(diff)<0.05){el.textContent=tgt.toFixed(1);return;}
    let step=0;const steps=20,timer=setInterval(()=>{step++;const p=step/steps,e=p<0.5?2*p*p:(1-Math.pow(-2*p+2,2)/2);
    el.textContent=(cur+diff*e).toFixed(1);if(step>=steps){el.textContent=tgt.toFixed(1);clearInterval(timer);}},40);
}
function applyLive(d){
    if(!d||d.error)return;
    const gse=document.querySelector('#view-global .sc-num');if(gse)animateScore(gse,d.composite);
    const gle=document.querySelector('#view-global .sc-label');if(gle){gle.textContent=d.risk_level;gle.style.color=d.risk_colour;}
    const gde=document.querySelector('#view-global .bar-dot');if(gde){gde.style.left=d.composite+'%';gde.style.background=d.risk_colour;}
    document.body.style.borderTopColor=d.risk_colour;
    const ts=document.querySelectorAll('.tab-score');if(ts[0]){animateScore(ts[0],d.composite);ts[0].style.color=d.risk_colour;}
    ['europe','middle_east','asia_pacific'].forEach((rk,ri)=>{
        const rd=d.regions[rk];if(!rd)return;
        if(ts[ri+1]){animateScore(ts[ri+1],rd.composite);ts[ri+1].style.color=rd.colour;}
        const rse=document.querySelector('#view-'+rk+' .sc-num');if(rse){animateScore(rse,rd.composite);rse.style.color=rd.colour;}
        const rce=document.querySelector('#view-'+rk+' .sc-conv');if(rce){rce.innerHTML=rd.convergence_label+(rd.convergence_detail?' <span>— '+rd.convergence_detail+'</span>':'');rce.style.color=rd.convergence_colour;}
        const rde=document.querySelector('#view-'+rk+' .bar-dot');if(rde){rde.style.left=rd.composite+'%';rde.style.background=rd.colour;}
        const rcard=document.getElementById('rcard-'+rk);
        if(rcard){const rs=rcard.querySelector('.rcard-score');if(rs){animateScore(rs,rd.composite);rs.style.color=rd.colour;}}
    });
    _lastUp=Date.now();
}
function fetchLive(){fetch('/api/scores').then(r=>r.json()).then(d=>applyLive(d)).catch(e=>console.log('Live err:',e));}
setInterval(fetchLive,60000);
setInterval(()=>{const c=document.getElementById('live-counter');if(!c)return;const a=Math.round((Date.now()-_lastUp)/1000);c.textContent=a<5?'Live — just updated':a<60?'Live — '+a+'s ago':'Updating — '+Math.round(a/60)+'m ago';c.style.color=a>300?'#f97316':'#555';},1000);

setTimeout(()=>redrawGlobalChart(),200);
</script>
</body>
</html>
"""


# ============================================================
# >>> PART 3 — Paste directly after Part 2
# ============================================================


# ============================================================
# DATA REFRESH
# ============================================================

def refresh_dashboard_data():
    global _dashboard_state
    if _dashboard_state["updating"]: return
    _dashboard_state["updating"] = True
    try:
        timestamps = {}; t0 = _time.time()

        # --- Polymarket ---
        events = fetch_polymarket_events()
        geo_markets = filter_geopolitical_markets(events)
        pm_baselines = fetch_contract_baselines()
        if pm_baselines:
            pm_score, pm_stats = calculate_polymarket_score_normalised(geo_markets, pm_baselines)
        else:
            avg = np.mean([m["risk_price"] for m in geo_markets]) if geo_markets else 0.5
            pm_score = round(avg * 100, 1)
            pm_stats = {"num_contracts": len(geo_markets), "total_volume": 0, "threat_count": 0, "deesc_count": 0, "avg_risk_pct": pm_score, "mature_count": 0, "immature_count": len(geo_markets), "method": "crude", "agg_z": 0.0, "contract_zscores": []}
        timestamps["polymarket"] = f"Live • {pm_stats.get('method','?')} • {pm_stats.get('mature_count',0)}/{pm_stats.get('num_contracts',0)} mature • {_time.time()-t0:.1f}s"

        pm_regional = calculate_polymarket_regional_scores(geo_markets, pm_baselines)
        global_pm_contracts = [m for m in geo_markets if m.get("region") == "global"]
        if pm_baselines:
            global_pm_score, global_pm_stats = calculate_polymarket_score_normalised(global_pm_contracts, pm_baselines)
        else:
            global_pm_score, global_pm_stats = 50.0, {"num_contracts": 0, "mature_count": 0, "agg_z": 0.0}
        global_pm_colour = get_score_colour(global_pm_score)

        # Build contribution lookup
        _contrib_lookup = {}
        for rk_l in list(REGIONS.keys()) + ["global"]:
            zl = (global_pm_stats if rk_l == "global" else pm_regional.get(rk_l, {}).get("stats", {})).get("contract_zscores", [])
            for cz in zl: _contrib_lookup[cz.get("q", "")] = {"pct": cz.get("contrib_pct"), "dir": cz.get("contrib_dir")}

        # Enrich contracts for display
        regional_contracts, global_contracts_list = {}, []
        for m in geo_markets:
            r, q = m.get("region", "global"), m.get("question", "")
            bl = pm_baselines.get(q, {})
            is_mature, dp_count = bl.get("mature", False), bl.get("count", 0)
            cz = round((m["risk_price"] - bl["mean"]) / max(bl["std"], PM_STD_FLOOR), 2) if is_mature and bl.get("std", 0) > 0.001 else None
            spark_svg = ""
            sp = bl.get("sparkline", [])
            if sp and len(sp) >= 2:
                if m["is_deescalation"]:
                    sp_d = [1.0 - v for v in sp]; sc = "#22c55e" if sp_d[-1] > sp_d[0] else "#ef4444" if sp_d[-1] < sp_d[0] else "#888"; spark_svg = make_sparkline_svg(sp_d, sc)
                else:
                    sc = "#ef4444" if sp[-1] > sp[0] else "#22c55e" if sp[-1] < sp[0] else "#888"; spark_svg = make_sparkline_svg(sp, sc)
            prob_change_24h, change_colour = None, "#888"
            if bl.get("price_24h_ago") is not None:
                rp_now, rp_24h = m["risk_price"], bl["price_24h_ago"]
                prob_now = (1.0 - rp_now) if m["is_deescalation"] else rp_now
                prob_24h = (1.0 - rp_24h) if m["is_deescalation"] else rp_24h
                prob_change_24h = round((prob_now - prob_24h) * 100, 1)
                if abs(prob_change_24h) > 0.5:
                    if m["is_deescalation"]: change_colour = "#22c55e" if prob_change_24h > 0 else "#ef4444"
                    else: change_colour = "#ef4444" if prob_change_24h > 0 else "#22c55e"
            contrib_pct, contrib_dir = None, None
            if is_mature:
                cl = _contrib_lookup.get(m["question"][:60])
                if cl: contrib_pct, contrib_dir = cl["pct"], cl["dir"]
            entry = {"question": m["question"], "probability_pct": m["probability_pct"], "risk_pct": m.get("risk_pct", m["probability_pct"]), "colour": get_prob_colour(100 - m["probability_pct"]) if m["is_deescalation"] else get_prob_colour(m["probability_pct"]), "is_deescalation": m["is_deescalation"], "is_mature": is_mature, "dp_count": dp_count, "z_score": cz, "change_24h": prob_change_24h, "change_colour": change_colour, "sparkline_svg": spark_svg, "contrib_pct": contrib_pct, "contrib_dir": contrib_dir}
            if r == "global": global_contracts_list.append(entry)
            else: regional_contracts.setdefault(r, []).append(entry)

        # --- Markets ---
        t1 = _time.time()
        market_data = fetch_market_data()
        mkt_score = calculate_market_score(market_data)
        timestamps["markets"] = f"Today vs 90d • {_time.time()-t1:.1f}s"
        for tk, d in market_data.items():
            if "daily_change_pct" in d:
                if d.get("inverted", False):
                    d["change_colour"] = get_change_colour(-d["daily_change_pct"])
                    sc = "#22c55e" if d["daily_change_pct"] > 0.5 else "#ef4444" if d["daily_change_pct"] < -0.5 else "#888"
                else:
                    d["change_colour"] = get_change_colour(d["daily_change_pct"])
                    sc = "#ef4444" if d["daily_change_pct"] > 0.5 else "#22c55e" if d["daily_change_pct"] < -0.5 else "#888"
                d["sparkline_svg"] = make_sparkline_svg(d.get("sparkline", []), sc)
            else: d["sparkline_svg"] = ""

        # --- ACLED (supplemental) ---
        acled_supplemental, acled_regional = {}, {}
        try:
            cached = get_cached("acled")
            if cached and cached.get("raw"):
                acled_supplemental = get_acled_supplemental(cached["raw"], cached["stats"].get("forecast_period", ""))
                acled_regional = get_acled_regional_supplemental(cached["raw"])
            else:
                cd, pd, fp = fetch_acled_cast()
                if cd:
                    acled_supplemental = get_acled_supplemental(cd, fp)
                    acled_regional = get_acled_regional_supplemental(cd)
                    set_cache("acled", data=cd, stats={"forecast_period": fp}, score=50, raw=cd, prev_raw=pd)
        except Exception as e: print(f"ACLED error: {e}")

        # --- Background signals ---
        gdelt_regional = _gdelt_state.get("regional", {})
        gdelt_fetching = _gdelt_state.get("fetching", False)
        gtrends_regional = _gtrends_state.get("regional", {})
        gtrends_fetching = _gtrends_state.get("fetching", False)
        cloudflare_regional = _cloudflare_state.get("regional", {})
        cloudflare_fetching = _cloudflare_state.get("fetching", False)

        # --- Regional composites ---
        mkt_regional = calculate_market_regional_scores(market_data)
        regional_composites = calculate_regional_composites(pm_regional, mkt_regional, gdelt_regional, gtrends_regional, cloudflare_regional)
        for rk, rd in regional_composites.items():
            rd["pm_colour"] = get_score_colour(rd["signals"]["polymarket"])
            rd["mkt_colour"] = get_score_colour(rd["signals"]["markets"])
            rd["gdelt_colour"] = get_score_colour(rd["signals"]["gdelt"])
            rd["gt_panic_colour"] = get_score_colour(rd["signals"]["gt_panic"])
            rd["gt_global_colour"] = get_score_colour(rd["signals"]["gt_global"])
            rd["cloudflare_colour"] = get_score_colour(rd["signals"]["cloudflare"])
            rd["fixed_colour"] = REGIONS[rk]["colour"]

        # --- Global score ---
        composite_score, regional_weights = calculate_dynamic_global_score(regional_composites, global_pm_score)
        risk_level, risk_colour = get_risk_level(composite_score)
        print(f"Regional: " + ", ".join(f"{r}={d['composite']}" for r, d in regional_composites.items()) + f" | Global PM={global_pm_score} | Final={composite_score}")

        # Cache scores for /api/scores endpoint
        api_alerts = []
        for rk_a, gt_a in gtrends_regional.items():
            for alert in gt_a.get("panic_alerts", []):
                api_alerts.append({"type": "panic", "region": rk_a, "text": f"{alert['term']}: z={alert['z']}"})
        for rk_a, cf_a in cloudflare_regional.items():
            for c in cf_a.get("country_details", []):
                if c["z"] < -1.5:
                    api_alerts.append({"type": "cloudflare", "region": rk_a, "text": f"{COUNTRY_CODE_NAMES.get(c['code'], c['code'])} z={c['z']}"})
        # Top contract movers (24h change > 5pp)
        for m in geo_markets:
            bl = pm_baselines.get(m.get("question", ""), {})
            if bl.get("price_24h_ago") is not None:
                rp_now, rp_24h = m["risk_price"], bl["price_24h_ago"]
                prob_now = (1.0 - rp_now) if m["is_deescalation"] else rp_now
                prob_24h = (1.0 - rp_24h) if m["is_deescalation"] else rp_24h
                change = round((prob_now - prob_24h) * 100, 1)
                if abs(change) >= 5:
                    q_lower = m['question'].lower()
                    dedup = re.sub(r'\s*by\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},?\s*\d{0,4}.*', '', q_lower).strip()
                    dedup = re.sub(r'\s*before\s+\d{4}.*', '', dedup).strip()
                    dedup = re.sub(r'\s*by\s+(end of\s+)?\d{4}.*', '', dedup).strip()
                    dedup = re.sub(r'\s*in\s+\d{4}.*', '', dedup).strip()
                    # Risk direction: for de-esc contracts, falling probability = rising risk
                    if m["is_deescalation"]:
                        risk_rising = change < 0  # peace odds dropping = bad
                    else:
                        risk_rising = change > 0  # threat odds rising = bad
                    api_alerts.append({"type": "contract", "region": m.get("region", "global"), "text": f"{m['question'][:50]} {'+'if change>0 else ''}{change}pp to {m['probability_pct']}%", "volume": m.get("volume", 0), "dedup_key": dedup, "risk_rising": risk_rising})
        # Deduplicate contract alerts
        seen_keys = {}
        deduped_alerts = []
        for a in api_alerts:
            if a["type"] != "contract":
                deduped_alerts.append(a)
            else:
                key = a.get("dedup_key", "")
                if key not in seen_keys or a.get("volume", 0) > seen_keys[key].get("volume", 0):
                    seen_keys[key] = a
        deduped_alerts.extend(seen_keys.values())
        api_alerts = [{"type": a["type"], "region": a.get("region", ""), "text": a["text"], "risk_rising": a.get("risk_rising", True)} for a in deduped_alerts]
        _dashboard_state["scores"] = {
            "composite": composite_score,
            "risk_level": risk_level,
            "risk_colour": risk_colour,
            "global_pm_score": global_pm_score,
            "regions": {
                rk: {
                    "name": REGIONS[rk]["name"],
                    "composite": rd["composite"],
                    "colour": rd["colour"],
                    "convergence_label": rd["convergence_label"],
                    "convergence_detail": rd["convergence_detail"],
                    "convergence_colour": rd["convergence_colour"],
                    "signals": rd["signals"],
                    "weight": round(regional_weights.get(rk, 0.333) * 100, 1),
                } for rk, rd in regional_composites.items()
            },
            "alerts": api_alerts,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        }

        # --- Save ---
        try:
            save_score_snapshot(composite_score, pm_score, mkt_score, None, None, pm_stats, {}, {}, risk_level)
            save_contract_snapshots(geo_markets)
            save_regional_scores(regional_composites, pm_regional, mkt_regional)
        except Exception as e: print(f"Save error: {e}")

        score_history = fetch_score_history(days=30)
        regional_history = fetch_regional_score_history(days=30)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        html = render_template_string(
            DASHBOARD_HTML,
            composite_score=composite_score, risk_level=risk_level, risk_colour=risk_colour,
            regional_composites=regional_composites, regions=REGIONS,
            global_contracts=global_contracts_list, regional_contracts=regional_contracts,
            market_data=market_data, score_history=score_history,
            global_pm_score=global_pm_score, global_pm_stats=global_pm_stats, global_pm_colour=global_pm_colour,
            acled_supplemental=acled_supplemental, acled_regional=acled_regional,
            timestamps=timestamps, updated_at=now, error=None,
            GLOBAL_REGIONAL_WEIGHT=GLOBAL_REGIONAL_WEIGHT, GLOBAL_PM_WEIGHT=GLOBAL_PM_WEIGHT,
            weights=WEIGHTS, regional_history=regional_history, regional_weights=regional_weights,
            gdelt_fetching=gdelt_fetching, gdelt_regional=gdelt_regional,
            gtrends_fetching=gtrends_fetching, gtrends_regional=gtrends_regional,
            cloudflare_fetching=cloudflare_fetching, cloudflare_regional=cloudflare_regional,
            market_ticker_count=sum(1 for t, i in MARKET_TICKERS.items() if i.get("region") != "global"),
            gtrends_expected={rk: {"l1": len(GTRENDS_GLOBAL_TERMS.get(rk, [])), "l2": len(GTRENDS_COUNTRY_TERMS.get(rk, [])), "total": len(GTRENDS_GLOBAL_TERMS.get(rk, [])) + len(GTRENDS_COUNTRY_TERMS.get(rk, []))} for rk in REGIONS},
            country_names=COUNTRY_CODE_NAMES,
            alerts=_dashboard_state.get("scores", {}).get("alerts", []),
        )
        _dashboard_state["html"] = html
        _dashboard_state["last_updated"] = _time.time()
    except Exception as e:
        print(f"Refresh error: {e}"); import traceback; traceback.print_exc()
        if _dashboard_state["html"] is None:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            _dashboard_state["html"] = render_template_string(
                DASHBOARD_HTML, composite_score=50, risk_level="NORMAL", risk_colour="#888888",
                regional_composites={}, regions=REGIONS, global_contracts=[], regional_contracts={},
                market_data={}, score_history=[], acled_supplemental={}, acled_regional={},
                timestamps={}, updated_at=now, error=f"Failed to load: {str(e)}",
                GLOBAL_REGIONAL_WEIGHT=GLOBAL_REGIONAL_WEIGHT, GLOBAL_PM_WEIGHT=GLOBAL_PM_WEIGHT,
                weights=WEIGHTS, regional_history={}, regional_weights={},
                gdelt_fetching=False, gdelt_regional={}, gtrends_fetching=False, gtrends_regional={},
                cloudflare_fetching=False, cloudflare_regional={},
                global_pm_score=50.0, global_pm_stats={"num_contracts": 0, "mature_count": 0, "agg_z": 0.0},
                global_pm_colour="#b0b0b0", market_ticker_count=0, gtrends_expected={},
                country_names=COUNTRY_CODE_NAMES, alerts=[],
            )
    finally:
        _dashboard_state["updating"] = False


# ============================================================
# ROUTES
# ============================================================

@app.route("/health")
def health():
    return "ok", 200


@app.route("/api/scores")
def api_scores():
    """JSON endpoint for live score updates. Returns latest cached scores instantly."""
    import json as _json
    if _time.time() - _dashboard_state["last_updated"] > 120:
        ctx = app.app_context()
        def bg():
            with ctx: refresh_dashboard_data()
        threading.Thread(target=bg).start()
    scores = _dashboard_state.get("scores")
    if not scores:
        return _json.dumps({"error": "No data yet"}), 200, {"Content-Type": "application/json"}
    return _json.dumps(scores), 200, {"Content-Type": "application/json"}


@app.route("/")
def dashboard():
    if _dashboard_state["html"] is None:
        ctx = app.app_context()
        def first_refresh():
            with ctx: refresh_dashboard_data()
        threading.Thread(target=first_refresh).start()
        return (
            '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta http-equiv="refresh" content="10">'
            '<title>Loading</title><style>'
            'body{background:#08080d;color:#f0f0f0;display:flex;justify-content:center;'
            'align-items:center;min-height:100vh;font-family:-apple-system,sans-serif;}'
            '.loading{text-align:center;}'
            '.loading h1{font-size:1.3em;letter-spacing:2px;margin-bottom:12px;}'
            '.loading p{color:#888;font-size:0.85em;}'
            '.spinner{width:40px;height:40px;border:3px solid #333;border-top:3px solid #eab308;'
            'border-radius:50%;animation:spin 1s linear infinite;margin:20px auto;}'
            '@keyframes spin{to{transform:rotate(360deg);}}'
            '</style></head><body><div class="loading">'
            '<h1>GEOPOLITICAL RISK INDEX</h1><div class="spinner"></div>'
            '<p>Fetching signals — first load takes up to 2 minutes</p>'
            '<p>Auto-refreshes in 10 seconds</p>'
            '</div></body></html>'
        ), 200
    if _time.time() - _dashboard_state["last_updated"] > 180:
        ctx = app.app_context()
        def bg():
            with ctx: refresh_dashboard_data()
        threading.Thread(target=bg).start()
    return _dashboard_state["html"]


# ============================================================
# STARTUP
# ============================================================

_threads_started = False

def _keepalive_loop():
    """Forces periodic data refreshes regardless of visitor traffic.
    Replit aggressively sleeps published apps — UptimeRobot hitting /health
    keeps the process alive but doesn't trigger data fetches. This loop
    ensures refresh_dashboard_data() runs at least every 3 minutes."""
    _time.sleep(60)  # Wait for initial startup and first thread stagger
    while True:
        try:
            if _time.time() - _dashboard_state["last_updated"] > 120:
                with app.app_context():
                    refresh_dashboard_data()
        except Exception as e:
            print(f"Keepalive refresh error: {e}")
        _time.sleep(180)  # 3-minute cycle

def start_background_threads():
    global _threads_started
    if _threads_started: return
    _threads_started = True
    # Stagger thread launches to avoid OOM from simultaneous memory spikes
    def _staggered_start():
        _time.sleep(5)
        threading.Thread(target=_cloudflare_background_loop, daemon=True).start()
        print("Background thread started: Cloudflare Radar")
        _time.sleep(30)
        threading.Thread(target=_gdelt_background_loop, daemon=True).start()
        print("Background thread started: GDELT BigQuery")
        _time.sleep(30)
        threading.Thread(target=_gtrends_background_loop, daemon=True).start()
        print("Background thread started: Google Trends")
        _time.sleep(10)
        threading.Thread(target=_keepalive_loop, daemon=True).start()
        print("Background thread started: Keepalive")
    threading.Thread(target=_staggered_start, daemon=True).start()
    print("Background threads starting (staggered)...")

start_background_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
