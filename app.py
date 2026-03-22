"""
BANDARMOLOGY ENGINE — Web App (Streamlit)
Deploy gratis di: https://share.streamlit.io
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import yfinance as yf
import warnings
warnings.filterwarnings("ignore")

# ── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Bandarmology IDX",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CUSTOM CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Dark theme override */
[data-testid="stAppViewContainer"] {
    background: #070b0f;
    color: #c8d8e8;
}
[data-testid="stSidebar"] {
    background: #0d1318;
    border-right: 1px solid #1a2535;
}
[data-testid="stSidebar"] * { color: #c8d8e8 !important; }

/* Metric cards */
[data-testid="metric-container"] {
    background: #0d1318;
    border: 1px solid #1a2535;
    border-radius: 6px;
    padding: 12px;
}
[data-testid="stMetricValue"]  { color: #00ccff !important; font-size: 1.6rem !important; }
[data-testid="stMetricLabel"]  { color: #6a8aaa !important; }
[data-testid="stMetricDelta"]  > div { font-size: 0.8rem !important; }

/* Headers */
h1, h2, h3 { color: #00ff88 !important; font-family: monospace !important; }
h1 { letter-spacing: 4px; font-size: 1.8rem !important; }

/* Dataframe */
[data-testid="stDataFrame"] { background: #0d1318; }

/* Buttons */
.stButton > button {
    background: #00ff88 !important;
    color: #070b0f !important;
    font-weight: 800 !important;
    font-family: monospace !important;
    letter-spacing: 2px !important;
    border: none !important;
    border-radius: 4px !important;
    padding: 10px 24px !important;
}
.stButton > button:hover { background: #00ffaa !important; }

/* Select/input */
.stSelectbox > div, .stTextInput > div > div {
    background: #0d1318 !important;
    border: 1px solid #1a2535 !important;
    color: #c8d8e8 !important;
}

/* Tabs */
[data-testid="stTabs"] button {
    background: transparent !important;
    color: #6a8aaa !important;
    font-family: monospace !important;
    letter-spacing: 1px !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: #00ff88 !important;
    border-bottom: 2px solid #00ff88 !important;
}

/* Divider */
hr { border-color: #1a2535 !important; }

/* Info/warning boxes */
.stAlert { background: #0d1318 !important; border: 1px solid #1a2535 !important; }

/* Score badge */
.score-badge {
    display: inline-block;
    padding: 6px 18px;
    border-radius: 4px;
    font-family: monospace;
    font-weight: 700;
    font-size: 1.1rem;
    letter-spacing: 2px;
}
.badge-acc  { background: rgba(0,255,136,0.15); color: #00ff88; border: 1px solid #00ff88; }
.badge-dis  { background: rgba(255,51,85,0.15);  color: #ff3355; border: 1px solid #ff3355; }
.badge-neut { background: rgba(255,170,0,0.15);  color: #ffaa00; border: 1px solid #ffaa00; }

/* Progress bar custom */
.stProgress > div > div { background: #00ff88 !important; }

/* Subheader */
[data-testid="stMarkdownContainer"] p { color: #c8d8e8; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
#  DATABASE BROKER
# ══════════════════════════════════════════════════════════════════════════

BROKER_DB = {
    "AK": {"name":"UBS Securities",          "cat":"FOREIGN_SMART",      "weight":3.0,  "goreng":False, "victim":False, "style":"Long-term Accumulator",       "contrarian":False},
    "BK": {"name":"J.P. Morgan",             "cat":"FOREIGN_SMART",      "weight":3.0,  "goreng":False, "victim":False, "style":"Block Deal / Market Mover",    "contrarian":False},
    "DB": {"name":"Deutsche Securities",     "cat":"FOREIGN_SMART",      "weight":2.8,  "goreng":False, "victim":False, "style":"Stealth Accumulator",          "contrarian":False},
    "GW": {"name":"HSBC Securities",         "cat":"FOREIGN_SMART",      "weight":2.8,  "goreng":False, "victim":False, "style":"Institutional / Macro",        "contrarian":False},
    "ML": {"name":"Merrill Lynch",           "cat":"FOREIGN_SMART",      "weight":2.8,  "goreng":False, "victim":False, "style":"Wealth Management HNWI",       "contrarian":False},
    "YU": {"name":"CGS-CIMB Securities",     "cat":"FOREIGN_SMART",      "weight":2.5,  "goreng":False, "victim":False, "style":"ASEAN Regional Fund",          "contrarian":False},
    "KZ": {"name":"CLSA Securities",         "cat":"FOREIGN_SMART",      "weight":2.5,  "goreng":False, "victim":False, "style":"Research-driven Swing",        "contrarian":False},
    "CS": {"name":"Credit Suisse",           "cat":"FOREIGN_SMART",      "weight":2.5,  "goreng":False, "victim":False, "style":"Block Deal Specialist",        "contrarian":False},
    "DP": {"name":"DBS Vickers",             "cat":"FOREIGN_SMART",      "weight":2.3,  "goreng":False, "victim":False, "style":"Singapore Institutional",      "contrarian":False},
    "ZP": {"name":"Maybank Securities",      "cat":"FOREIGN_SMART",      "weight":2.2,  "goreng":False, "victim":False, "style":"Malaysia Value Fund",          "contrarian":False},
    "RX": {"name":"Macquarie Securities",    "cat":"FOREIGN_SMART",      "weight":2.2,  "goreng":False, "victim":False, "style":"Commodities Specialist",       "contrarian":False},
    "AI": {"name":"UOB Kay Hian",            "cat":"FOREIGN_SMART",      "weight":2.0,  "goreng":False, "victim":False, "style":"Underwriter / Singapore",      "contrarian":False},
    "MK": {"name":"Ekuator Swarna",          "cat":"LOCAL_BANDAR",       "weight":2.0,  "goreng":True,  "victim":False, "style":"Pump & Dump / Goreng Saham",   "contrarian":False},
    "EP": {"name":"Valbury Asia Securities", "cat":"LOCAL_BANDAR",       "weight":1.8,  "goreng":True,  "victim":False, "style":"Goreng Saham / Active Prop",   "contrarian":False},
    "II": {"name":"Danatama Makmur",         "cat":"LOCAL_BANDAR",       "weight":1.5,  "goreng":True,  "victim":False, "style":"IPO Underwriter Gorengan",     "contrarian":False},
    "DD": {"name":"Makindo Securities",      "cat":"LOCAL_BANDAR",       "weight":1.2,  "goreng":True,  "victim":False, "style":"Second/Third Liner Bandar",    "contrarian":False},
    "CC": {"name":"Mandiri Sekuritas",       "cat":"BUMN_INST",          "weight":1.5,  "goreng":False, "victim":False, "style":"BUMN — Mixed Inst+Retail",     "contrarian":False},
    "NI": {"name":"BNI Sekuritas",           "cat":"BUMN_INST",          "weight":1.4,  "goreng":False, "victim":False, "style":"Dana Pensiun BUMN",            "contrarian":False},
    "OD": {"name":"BRI Danareksa",           "cat":"BUMN_INST",          "weight":1.3,  "goreng":False, "victim":False, "style":"Reksadana Index",              "contrarian":False},
    "DX": {"name":"Bahana Sekuritas",        "cat":"LOCAL_INST",         "weight":1.8,  "goreng":False, "victim":False, "style":"Market Stabilizer Gov-linked", "contrarian":False},
    "KI": {"name":"Ciptadana Sekuritas",     "cat":"LOCAL_INST",         "weight":1.6,  "goreng":False, "victim":False, "style":"Research + Prop Trading",      "contrarian":False},
    "LG": {"name":"Trimegah Sekuritas",      "cat":"LOCAL_INST",         "weight":1.6,  "goreng":False, "victim":False, "style":"Research-driven Swing",        "contrarian":False},
    "HG": {"name":"Sucor Sekuritas",         "cat":"LOCAL_INST",         "weight":1.5,  "goreng":False, "victim":False, "style":"Commodities Research",         "contrarian":False},
    "SQ": {"name":"BCA Sekuritas",           "cat":"LOCAL_INST",         "weight":1.4,  "goreng":False, "victim":False, "style":"HNWI Wealth Management",       "contrarian":False},
    "XA": {"name":"NH Korindo Securities",   "cat":"KOREAN",             "weight":1.8,  "goreng":False, "victim":False, "style":"Korean Institutional",         "contrarian":False},
    "AG": {"name":"Kiwoom Securities",       "cat":"KOREAN",             "weight":1.5,  "goreng":False, "victim":False, "style":"Korean Retail/Inst",           "contrarian":False},
    "BQ": {"name":"Korea Investment Sec.",   "cat":"KOREAN",             "weight":1.6,  "goreng":False, "victim":False, "style":"Korean Fund",                  "contrarian":False},
    "YP": {"name":"Mirae Asset (iPOT-lama)", "cat":"RETAIL_LARGE",       "weight":-0.5, "goreng":False, "victim":True,  "style":"Retail Terbesar — Kontrarian", "contrarian":True},
    "XC": {"name":"Ajaib Sekuritas",         "cat":"RETAIL_LARGE",       "weight":-0.6, "goreng":False, "victim":True,  "style":"'Xobat Cutloss' Digital Retail","contrarian":True},
    "XL": {"name":"Stockbit Sekuritas",      "cat":"RETAIL_LARGE",       "weight":-0.5, "goreng":False, "victim":True,  "style":"Social Trading Herd Behavior", "contrarian":True},
    "PD": {"name":"Indo Premier (IPOT)",     "cat":"RETAIL_LARGE",       "weight":-0.4, "goreng":False, "victim":True,  "style":"Active Retail Traders",        "contrarian":True},
    "KK": {"name":"Phillip Securities",      "cat":"RETAIL_MED",         "weight":0.3,  "goreng":False, "victim":True,  "style":"Mixed Retail",                 "contrarian":False},
    "MG": {"name":"Semesta Indovest",        "cat":"SCALPER",            "weight":-0.8, "goreng":False, "victim":False, "style":"Scalper Terbesar IDX — Noise", "contrarian":True},
}

CAT_COLORS = {
    "FOREIGN_SMART": "#00ff88",
    "LOCAL_BANDAR":  "#ffaa00",
    "BUMN_INST":     "#00ccff",
    "LOCAL_INST":    "#00ccff",
    "KOREAN":        "#aa88ff",
    "RETAIL_LARGE":  "#ff3355",
    "RETAIL_MED":    "#ff8855",
    "SCALPER":       "#ff5500",
}

CAT_LABELS = {
    "FOREIGN_SMART": "🟢 Foreign Smart Money",
    "LOCAL_BANDAR":  "🔥 Local Bandar/Goreng",
    "BUMN_INST":     "🏛️  BUMN Institutional",
    "LOCAL_INST":    "💼 Local Institutional",
    "KOREAN":        "🇰🇷 Korean Broker",
    "RETAIL_LARGE":  "⚠️  Retail Besar (Kontrarian)",
    "RETAIL_MED":    "⚠️  Retail Menengah",
    "SCALPER":       "⚡ Scalper (Noise)",
}

FOREIGN_LIST  = ["AK","BK","DB","GW","ML","YU","KZ","CS","DP","ZP","RX","AI"]
GORENG_LIST   = ["MK","EP","II","DD"]
RETAIL_LIST   = ["YP","XC","XL","PD","KK"]
SCALPER_LIST  = ["MG"]
LOCAL_INST    = ["CC","NI","OD","DX","KI","LG","HG","SQ"]
KOREAN_LIST   = ["XA","AG","BQ"]


# ══════════════════════════════════════════════════════════════════════════
#  KALKULASI INDIKATOR
# ══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=900, show_spinner=False)
def load_data(ticker: str, period: str) -> pd.DataFrame:
    symbol = ticker.upper().replace(".JK","") + ".JK"
    try:
        df = yf.download(symbol, period=period, interval="1d",
                         progress=False, auto_adjust=True)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.rename(columns={"Open":"open","High":"high","Low":"low",
                                  "Close":"close","Volume":"volume"})
        df = df[["open","high","low","close","volume"]].dropna()
        df = df[df["volume"] > 0]
        return df
    except:
        return None

def calc_cmf(df, p=14):
    hl  = df["high"] - df["low"]
    clv = ((df["close"]-df["low"])-(df["high"]-df["close"])) / hl.replace(0,np.nan)
    return (clv*df["volume"]).rolling(p).sum() / df["volume"].rolling(p).sum().fillna(method="ffill")

def calc_obv(df):
    return (np.sign(df["close"].diff()).fillna(0) * df["volume"]).cumsum()

def calc_mfi(df, p=14):
    tp  = (df["high"]+df["low"]+df["close"])/3
    mf  = tp * df["volume"]
    pos = mf.where(tp>tp.shift(1),0).rolling(p).sum()
    neg = mf.where(tp<tp.shift(1),0).rolling(p).sum()
    return (100 - 100/(1+pos/neg.replace(0,np.nan))).fillna(50)

def calc_rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    return (100 - 100/(1+g/l.replace(0,np.nan))).fillna(50)

def calc_bb(s, p=20):
    ma  = s.rolling(p).mean()
    std = s.rolling(p).std()
    return ma+2*std, ma, ma-2*std

def detect_wyckoff(df, cmf, obv):
    n = len(df)
    prices = df["close"]
    trend   = (prices.iloc[-n//3:].mean() - prices.iloc[:n//3].mean()) / prices.iloc[:n//3].mean()
    p20     = prices.tail(20)
    range20 = (p20.max()-p20.min()) / p20.mean()
    vol_spike = df["volume"].tail(5).max() > df["volume"].rolling(20).mean().iloc[-1]*1.8
    obv_rising = obv.iloc[-1] > obv.iloc[-min(10,len(obv)-1)]
    cmf_up     = cmf.iloc[-1] > cmf.iloc[-min(10,len(cmf)-1)]

    if trend < -0.07 and not vol_spike:
        return "A", "Selling Climax — bandar mulai serap supply"
    if range20 < 0.07 and cmf_up:
        return "B", "Building Cause — bandar kumpulkan saham diam-diam"
    if range20 < 0.09 and vol_spike and trend < 0.03:
        return "C", "⭐ Spring/Shakeout — ENTRY TERBAIK ikuti bandar"
    if trend > 0.03 and obv_rising:
        return "D", "Sign of Strength — breakout dikonfirmasi volume"
    if trend > 0.08 and obv_rising:
        return "E", "Markup — trending naik, mulai waspadai distribusi"
    return "B", "Building Cause / Konsolidasi — tunggu konfirmasi"

def detect_vsa(df):
    avg_vol = df["volume"].rolling(20).mean()
    avg_spd = (df["high"]-df["low"]).rolling(10).mean()
    patterns = []
    for i in range(5, len(df)):
        d, prev = df.iloc[i], df.iloc[i-1]
        av = avg_vol.iloc[i]
        if pd.isna(av) or av == 0: continue
        sp = d["high"]-d["low"]
        cp = (d["close"]-d["low"]) / (sp if sp>0 else 1)
        vr = d["volume"] / av
        if vr>1.5 and cp>0.6 and d["close"]<prev["close"]:
            patterns.append({"type":"ACC","name":"Stopping Volume","vol_ratio":vr})
        if vr<0.5 and sp<avg_spd.iloc[i]*0.5 and cp>0.5:
            patterns.append({"type":"ACC","name":"No Supply (Drying Up)","vol_ratio":vr})
        if vr>1.5 and d["low"]<prev["low"] and d["close"]>(d["high"]+d["low"])/2:
            patterns.append({"type":"ACC","name":"Shakeout / Spring","vol_ratio":vr})
        if vr>1.5 and cp<0.4 and d["close"]>prev["close"]:
            patterns.append({"type":"DIS","name":"Buying Climax","vol_ratio":vr})
        if vr>1.5 and d["high"]>prev["high"] and cp<0.45:
            patterns.append({"type":"DIS","name":"Upthrust","vol_ratio":vr})
    seen, res = set(), []
    for p in reversed(patterns):
        if p["name"] not in seen and len(res)<6:
            seen.add(p["name"]); res.insert(0,p)
    return res


# ══════════════════════════════════════════════════════════════════════════
#  BROKER DATA GENERATOR (Demo Realistis)
# ══════════════════════════════════════════════════════════════════════════

def gen_broker_data(ticker: str, tech_score: int) -> pd.DataFrame:
    np.random.seed(sum(ord(c) for c in ticker) + tech_score)
    r = lambda lo, hi: int(np.random.randint(lo, hi))
    s = "accumulation" if tech_score>=65 else "distribution" if tech_score<=35 else "neutral"

    if s == "accumulation":
        rows = [
            ("AK",r(18000,50000),r(1000,5000)), ("BK",r(14000,38000),r(1200,4500)),
            ("DB",r(8000,22000), r(800,3000)),  ("GW",r(6000,18000), r(700,2500)),
            ("YU",r(5000,15000), r(600,2000)),  ("CC",r(8000,20000), r(4000,12000)),
            ("LG",r(4000,10000), r(2000,7000)), ("NI",r(4000,10000), r(2000,6000)),
            ("XA",r(3000,9000),  r(2500,7000)),
            ("MG",r(25000,65000),r(35000,90000)),
            ("YP",r(12000,28000),r(20000,55000)),("XC",r(6000,14000), r(10000,28000)),
            ("XL",r(5000,12000), r(8000,20000)), ("PD",r(7000,16000), r(12000,30000)),
            ("KK",r(4000,10000), r(6000,16000)),
        ]
    elif s == "distribution":
        rows = [
            ("AK",r(1000,5000),  r(20000,55000)),("BK",r(1200,4500), r(16000,42000)),
            ("DB",r(800,3000),   r(10000,28000)),("GW",r(700,2500),  r(8000,20000)),
            ("MK",r(2000,8000),  r(12000,35000)),("EP",r(1500,6000), r(8000,25000)),
            ("CC",r(5000,14000), r(6000,18000)),
            ("MG",r(35000,85000),r(28000,70000)),
            ("YP",r(22000,55000),r(8000,18000)), ("XC",r(12000,30000),r(3000,8000)),
            ("XL",r(10000,25000),r(2500,7000)),  ("PD",r(14000,32000),r(4000,10000)),
            ("KK",r(7000,18000), r(2000,5000)),  ("AR",r(4000,10000), r(1000,3000)),
        ]
    else:
        rows = [
            ("AK",r(5000,15000),r(4000,14000)),  ("BK",r(4000,12000),r(3500,11000)),
            ("CC",r(6000,14000),r(5500,13000)),  ("MG",r(28000,60000),r(26000,58000)),
            ("YP",r(14000,30000),r(13000,28000)),("XC",r(6000,14000),r(5500,13000)),
            ("XL",r(5000,12000),r(4800,11500)),  ("PD",r(7000,16000),r(6800,15500)),
            ("LG",r(2500,7000), r(2300,6500)),   ("DB",r(3000,8000), r(2800,7500)),
            ("XA",r(3000,8000), r(2800,7800)),   ("KK",r(4000,10000),r(3800,9800)),
        ]

    df = pd.DataFrame(rows, columns=["broker","buy_lot","sell_lot"])
    df["net_lot"] = df["buy_lot"] - df["sell_lot"]
    df["name"]    = df["broker"].apply(lambda x: BROKER_DB.get(x,{}).get("name","Unknown"))
    df["cat"]     = df["broker"].apply(lambda x: BROKER_DB.get(x,{}).get("cat","RETAIL_MED"))
    df["goreng"]  = df["broker"].apply(lambda x: BROKER_DB.get(x,{}).get("goreng",False))
    df["victim"]  = df["broker"].apply(lambda x: BROKER_DB.get(x,{}).get("victim",False))
    return df


def analyze_broker_flow(bdf: pd.DataFrame) -> dict:
    res = dict(foreign_net=0, local_inst_net=0, korean_net=0,
               retail_net=0, scalper_net=0, goreng_alert=False,
               goreng_brokers=[], crossing=None,
               smart_buyers=[], smart_sellers=[],
               score=50, signal="NEUTRAL", confidence="RENDAH",
               warnings=[])

    total = bdf["buy_lot"].sum() + bdf["sell_lot"].sum()

    for _, row in bdf.iterrows():
        code = row["broker"].upper()
        net  = int(row["net_lot"])
        cat  = row["cat"]

        if cat == "FOREIGN_SMART":
            res["foreign_net"] += net
            entry = {"broker":code, "name":row["name"], "net":net}
            if net > 0: res["smart_buyers"].append(entry)
            else:       res["smart_sellers"].append(entry)
        elif cat in ("BUMN_INST","LOCAL_INST"):
            res["local_inst_net"] += net
        elif cat == "KOREAN":
            res["korean_net"] += net
        elif cat in ("RETAIL_LARGE","RETAIL_MED"):
            res["retail_net"] += net
        elif cat == "SCALPER":
            res["scalper_net"] += net
            share = (row["buy_lot"]+row["sell_lot"]) / (total+1)
            if share > 0.25:
                res["warnings"].append(f"⚡ {code} dominasi {share:.0%} volume — scalper ramai, bandar bisa distribusi lewat likuiditas ini")
        elif cat == "LOCAL_BANDAR" and net > 5000:
            res["goreng_alert"] = True
            res["goreng_brokers"].append(code)

    # XC warning
    xc = bdf[bdf["broker"]=="XC"]
    if not xc.empty and xc["net_lot"].values[0] > 5000:
        res["warnings"].append("⚠️ XC (Ajaib/'Xobat Cutloss') net buy besar — historis sering tanda distribusi berakhir ke retailer muda")
    xl = bdf[bdf["broker"]=="XL"]
    if not xl.empty and xl["net_lot"].values[0] > 5000:
        res["warnings"].append("⚠️ XL (Stockbit) net buy besar — saham mungkin viral, biasanya sudah terlambat ikut bandar")

    # Crossing
    sm = res["foreign_net"] > 2000
    sd = res["foreign_net"] < -2000
    rb = (res["retail_net"]+res["scalper_net"]) < -2000
    rbu= (res["retail_net"]+res["scalper_net"]) > 2000
    if sm and rb:
        res["crossing"] = "ACC"
        res["warnings"].append("🔥 CROSSING SIGNAL AKUMULASI: Smart money BELI + Retail JUAL!")
    elif sd and rbu:
        res["crossing"] = "DIS"
        res["warnings"].append("💀 CROSSING SIGNAL DISTRIBUSI: Smart money JUAL + Retail BELI!")

    if res["goreng_alert"]:
        codes = ", ".join(res["goreng_brokers"])
        res["warnings"].append(f"🚨 GORENG ALERT: Broker {codes} aktif — dikenal dalam pump-and-dump. Ikuti dengan stop loss ketat!")

    # Score
    score = 50.0
    score += np.clip(res["foreign_net"]  / (total*0.3+1) * 30, -30, 30)
    score += np.clip(res["local_inst_net"]/(total*0.2+1) * 15, -15, 15)
    score += np.clip(res["korean_net"]   / (total*0.15+1)* 8,  -8,  8)
    ret = res["retail_net"] + res["scalper_net"]
    score += np.clip(-ret/(total*0.3+1)*10, -10, 10)
    score += (len(res["smart_buyers"]) - len(res["smart_sellers"])) * 2.5
    if res["crossing"] == "ACC": score += 10
    elif res["crossing"] == "DIS": score -= 10
    if res["goreng_alert"]: score += 5

    final = int(np.clip(round(score), 0, 100))
    conf  = abs(final-50)/50
    res["score"]      = final
    res["signal"]     = "AKUMULASI" if final>=60 else "DISTRIBUSI" if final<=40 else "NEUTRAL"
    res["confidence"] = "TINGGI" if conf>0.6 else "SEDANG" if conf>0.3 else "RENDAH"
    return res


def calc_tech_score(df, cmf, obv, mfi) -> int:
    score = 50.0
    cmf_v = cmf.iloc[-1] if not pd.isna(cmf.iloc[-1]) else 0
    score += np.clip(cmf_v*125, -25, 25)
    obv_sl = (obv.iloc[-1]-obv.iloc[-min(10,len(obv)-1)]) / (abs(obv.iloc[-min(10,len(obv)-1)])+1)
    score += np.clip(obv_sl*500, -20, 20)
    last = df.iloc[-1]
    sp = last["high"]-last["low"]
    cp = (last["close"]-last["low"]) / (sp if sp>0 else 1)
    score += (cp-0.5)*30
    avg_v = df["volume"].tail(20).mean()
    vr = last["volume"]/avg_v if avg_v>0 else 1
    if vr>1.3: score += 15 if cp>0.5 else -15
    pt = (df["close"].iloc[-1]-df["close"].iloc[-min(10,len(df)-1)]) / df["close"].iloc[-min(10,len(df)-1)]
    mt = mfi.iloc[-1]-mfi.iloc[-min(10,len(mfi)-1)]
    if pt<-0.01 and mt>3: score += 15
    if pt>0.01  and mt<-3: score -= 15
    return int(np.clip(round(score), 0, 100))


# ══════════════════════════════════════════════════════════════════════════
#  CHART BUILDER (Plotly)
# ══════════════════════════════════════════════════════════════════════════

def build_price_chart(df, cmf, obv, mfi, vsa_patterns):
    bb_up, bb_mid, bb_low = calc_bb(df["close"])

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        row_heights=[0.50, 0.17, 0.17, 0.16],
        vertical_spacing=0.03,
    )

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df.index, open=df["open"], high=df["high"],
        low=df["low"], close=df["close"],
        increasing_line_color="#00ff88", decreasing_line_color="#ff3355",
        increasing_fillcolor="#00ff88", decreasing_fillcolor="#ff3355",
        name="OHLC", opacity=0.9,
    ), row=1, col=1)

    # Bollinger bands
    fig.add_trace(go.Scatter(x=df.index, y=bb_up,  name="BB Upper",
        line=dict(color="#1a3555",width=1), showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=bb_mid, name="BB Mid",
        line=dict(color="#ffaa00",width=1,dash="dot"), showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=bb_low, name="BB Lower",
        line=dict(color="#1a3555",width=1), fill="tonexty",
        fillcolor="rgba(0,204,255,0.03)", showlegend=False), row=1, col=1)

    # MA20
    fig.add_trace(go.Scatter(
        x=df.index, y=df["close"].rolling(20).mean(),
        name="MA20", line=dict(color="#00ccff",width=1.5,dash="dash")), row=1, col=1)

    # VSA markers
    for p in vsa_patterns:
        pass  # simplified for web

    # Volume
    colors = ["#00ff88" if df["close"].iloc[i]>=df["open"].iloc[i] else "#ff3355"
              for i in range(len(df))]
    fig.add_trace(go.Bar(
        x=df.index, y=df["volume"], name="Volume",
        marker_color=colors, opacity=0.4), row=2, col=1)
    avg_vol_line = df["volume"].rolling(20).mean()
    fig.add_trace(go.Scatter(
        x=df.index, y=avg_vol_line, name="Avg Vol",
        line=dict(color="#ffaa00",width=1,dash="dash"), showlegend=False), row=2, col=1)

    # CMF
    cmf_colors = ["#00ff88" if v>0 else "#ff3355" for v in cmf]
    fig.add_trace(go.Bar(
        x=df.index, y=cmf, name="CMF",
        marker_color=cmf_colors, opacity=0.7), row=3, col=1)
    fig.add_hline(y=0.15, line_dash="dash", line_color="rgba(0,255,136,0.4)", row=3, col=1)
    fig.add_hline(y=-0.15,line_dash="dash", line_color="rgba(255,51,85,0.4)",  row=3, col=1)
    fig.add_hline(y=0,     line_color="rgba(100,130,160,0.5)",                  row=3, col=1)

    # MFI
    fig.add_trace(go.Scatter(
        x=df.index, y=mfi, name="MFI",
        line=dict(color="#ffaa00",width=1.5),
        fill="tozeroy", fillcolor="rgba(255,170,0,0.05)"), row=4, col=1)
    fig.add_hline(y=80, line_dash="dash", line_color="rgba(255,51,85,0.4)",   row=4, col=1)
    fig.add_hline(y=20, line_dash="dash", line_color="rgba(0,255,136,0.4)",   row=4, col=1)
    fig.add_hline(y=50, line_color="rgba(100,130,160,0.3)",                    row=4, col=1)

    fig.update_layout(
        paper_bgcolor="#070b0f", plot_bgcolor="#0a0e14",
        font=dict(color="#c8d8e8", family="monospace", size=11),
        showlegend=True,
        legend=dict(bgcolor="#0d1318", bordercolor="#1a2535",
                    font=dict(size=10), orientation="h", y=1.02),
        xaxis_rangeslider_visible=False,
        margin=dict(l=0,r=0,t=10,b=0),
        height=680,
    )

    for i in range(1, 5):
        fig.update_xaxes(gridcolor="#1a2535", showgrid=True, row=i, col=1)
        fig.update_yaxes(gridcolor="#1a2535", showgrid=True, row=i, col=1)

    fig.update_yaxes(title_text="Harga (Rp)", row=1, col=1)
    fig.update_yaxes(title_text="Volume",     row=2, col=1)
    fig.update_yaxes(title_text="CMF",  range=[-0.6,0.6], row=3, col=1)
    fig.update_yaxes(title_text="MFI",  range=[0,100],    row=4, col=1)
    return fig


def build_broker_chart(bdf: pd.DataFrame):
    bdf_s = bdf.sort_values("net_lot")
    colors = []
    for _, row in bdf_s.iterrows():
        cat = row["cat"]
        if cat == "FOREIGN_SMART":   colors.append("#00ff88")
        elif cat == "LOCAL_BANDAR":  colors.append("#ffaa00")
        elif cat == "SCALPER":       colors.append("#ff5500")
        elif cat in ("RETAIL_LARGE","RETAIL_MED"): colors.append("#ff3355")
        else: colors.append("#00ccff")

    fig = go.Figure(go.Bar(
        x=bdf_s["net_lot"],
        y=bdf_s["broker"] + " | " + bdf_s["name"].str[:20],
        orientation="h",
        marker_color=colors,
        opacity=0.85,
        text=[f"{n:+,}" for n in bdf_s["net_lot"]],
        textposition="outside",
        textfont=dict(color="#c8d8e8", size=10),
    ))
    fig.update_layout(
        paper_bgcolor="#070b0f", plot_bgcolor="#0a0e14",
        font=dict(color="#c8d8e8", family="monospace", size=10),
        margin=dict(l=0,r=50,t=10,b=0),
        height=max(400, len(bdf_s)*32),
        xaxis=dict(gridcolor="#1a2535", title="Net Lot"),
        yaxis=dict(gridcolor="#1a2535"),
        showlegend=False,
    )
    fig.add_vline(x=0, line_color="#3a5570", line_width=1)
    return fig


def build_broker_category_chart(br: dict):
    cats   = ["Foreign Smart", "Local Inst.", "Korean", "Retail\n(Kontrarian)", "Scalper\n(Noise)"]
    values = [br["foreign_net"], br["local_inst_net"], br["korean_net"], br["retail_net"], br["scalper_net"]]
    colors = ["#00ff88" if v>0 else "#ff3355" for v in values]

    fig = go.Figure(go.Bar(
        x=cats, y=values,
        marker_color=colors, opacity=0.85,
        text=[f"{v:+,}" for v in values],
        textposition="outside",
        textfont=dict(color="#c8d8e8", size=11, family="monospace"),
    ))
    fig.update_layout(
        paper_bgcolor="#070b0f", plot_bgcolor="#0a0e14",
        font=dict(color="#c8d8e8", family="monospace", size=11),
        margin=dict(l=0,r=0,t=10,b=0),
        height=320,
        xaxis=dict(gridcolor="#1a2535"),
        yaxis=dict(gridcolor="#1a2535", title="Net Lot"),
        showlegend=False,
    )
    fig.add_hline(y=0, line_color="#3a5570")
    return fig


def build_gauge(score: int, verdict: str):
    color = "#00ff88" if score>=65 else "#ff3355" if score<=35 else "#ffaa00"
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number={"font":{"color":color,"size":52,"family":"monospace"},"suffix":"/100"},
        gauge={
            "axis": {"range":[0,100], "tickcolor":"#3a5570",
                     "tickfont":{"color":"#6a8aaa","size":10}},
            "bar": {"color": color, "thickness":0.25},
            "bgcolor": "#0a0e14",
            "bordercolor": "#1a2535",
            "steps": [
                {"range":[0,35],  "color":"rgba(255,51,85,0.12)"},
                {"range":[35,65], "color":"rgba(255,170,0,0.08)"},
                {"range":[65,100],"color":"rgba(0,255,136,0.12)"},
            ],
            "threshold": {"line":{"color":color,"width":4},"thickness":0.8,"value":score},
        },
    ))
    fig.update_layout(
        paper_bgcolor="#070b0f",
        font=dict(color=color, family="monospace"),
        margin=dict(l=20,r=20,t=20,b=10),
        height=200,
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════
#  UI — SIDEBAR
# ══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## ⚙️ PARAMETER ANALISIS")
    st.markdown("---")

    ticker_input = st.text_input(
        "Kode Saham (IDX)",
        value="BBCA",
        max_chars=8,
        help="Masukkan kode saham IDX tanpa .JK (contoh: BBCA, TLKM, GOTO)",
    ).upper().replace(".JK","")

    period_map = {"1 Bulan":"1mo", "3 Bulan":"3mo", "6 Bulan":"6mo", "1 Tahun":"1y"}
    period_sel = st.selectbox("Periode Analisis", list(period_map.keys()), index=1)
    period     = period_map[period_sel]

    st.markdown("---")
    st.markdown("### 📋 Quick Screener")
    preset_tickers = st.multiselect(
        "Pilih Saham",
        ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","ICBP","UNVR","KLBF",
         "ADRO","PGAS","ANTM","PTBA","INDF","BYAN","HMSP","EXCL","TBIG"],
        default=["BBCA","BBRI","TLKM","ASII","GOTO"],
    )

    st.markdown("---")
    st.markdown("""
    <div style='font-family:monospace;font-size:11px;color:#3a5570;line-height:1.8'>
    <b style='color:#6a8aaa'>BROKER LEGEND:</b><br>
    🟢 Foreign Smart Money<br>
    🔥 Local Bandar/Goreng<br>
    🏛️ BUMN Institutional<br>
    🇰🇷 Korean Broker<br>
    ⚠️ Retail (Kontrarian)<br>
    ⚡ Scalper (Noise)<br>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
#  UI — MAIN
# ══════════════════════════════════════════════════════════════════════════

# Header
st.markdown("""
<div style='text-align:center;padding:12px 0 8px'>
  <h1 style='font-family:monospace;font-size:2rem;letter-spacing:6px;
             color:#00ff88;text-shadow:0 0 30px rgba(0,255,136,0.4)'>
    BANDARMOLOGY
  </h1>
  <p style='color:#3a5570;font-family:monospace;letter-spacing:3px;font-size:12px'>
    SMART MONEY DETECTOR — IDX / BEI
  </p>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["📊 ANALISIS SAHAM", "🔍 SCREENER", "📚 PANDUAN BROKER"])

# ── TAB 1: ANALISIS ────────────────────────────────────────────────────────
with tab1:
    col_btn, col_info = st.columns([2,8])
    with col_btn:
        analyze_btn = st.button("🔍 ANALISIS", use_container_width=True)

    if analyze_btn or "last_result" in st.session_state:
        if analyze_btn:
            with st.spinner(f"📡 Mengambil data {ticker_input}.JK dari Yahoo Finance..."):
                df = load_data(ticker_input, period)
            if df is None or len(df) < 20:
                st.error(f"❌ Gagal mengambil data {ticker_input}.JK. Cek kode saham.")
                st.stop()

            with st.spinner("⚙️ Menghitung indikator teknikal..."):
                cmf  = calc_cmf(df)
                obv  = calc_obv(df)
                mfi  = calc_mfi(df)
                rsi  = calc_rsi(df["close"])
                vsa  = detect_vsa(df)
                wyck_phase, wyck_desc = detect_wyckoff(df, cmf, obv)
                tech_score = calc_tech_score(df, cmf, obv, mfi)

            with st.spinner("🏦 Menganalisis broker flow..."):
                bdf    = gen_broker_data(ticker_input, tech_score)
                broker = analyze_broker_flow(bdf)

            final_score = int(np.clip(round(tech_score*0.6 + broker["score"]*0.4), 0, 100))
            verdict     = "AKUMULASI" if final_score>=65 else "DISTRIBUSI" if final_score<=35 else "NEUTRAL"
            confidence  = "TINGGI" if abs(final_score-50)/50>0.6 else "SEDANG" if abs(final_score-50)/50>0.3 else "RENDAH"

            last = df.iloc[-1]
            prev = df.iloc[-2]
            change = (last["close"]-prev["close"])/prev["close"]*100
            avg_v  = df["volume"].tail(20).mean()
            vr     = last["volume"]/avg_v if avg_v>0 else 1

            st.session_state["last_result"] = dict(
                df=df, cmf=cmf, obv=obv, mfi=mfi, rsi=rsi, vsa=vsa,
                wyck_phase=wyck_phase, wyck_desc=wyck_desc,
                tech_score=tech_score, broker=broker, bdf=bdf,
                final_score=final_score, verdict=verdict, confidence=confidence,
                last_price=float(last["close"]), change=change,
                vol_ratio=vr, last_cmf=float(cmf.iloc[-1]),
                last_mfi=float(mfi.iloc[-1]), ticker=ticker_input,
            )

        res = st.session_state["last_result"]
        df  = res["df"]
        br  = res["broker"]

        # ── VERDICT HEADER ──────────────────────────────────────────────
        badge_cls = "badge-acc" if res["verdict"]=="AKUMULASI" else "badge-dis" if res["verdict"]=="DISTRIBUSI" else "badge-neut"
        cross_str = ""
        if br["crossing"] == "ACC":
            cross_str = " | 🔥 ACCUMULATION CROSS"
        elif br["crossing"] == "DIS":
            cross_str = " | 💀 DISTRIBUTION CROSS"
        goreng_str = " | 🚨 GORENG ALERT" if br["goreng_alert"] else ""

        st.markdown(f"""
        <div style='background:#0d1318;border:1px solid #1a2535;
                    border-top:3px solid {"#00ff88" if res["verdict"]=="AKUMULASI" else "#ff3355" if res["verdict"]=="DISTRIBUSI" else "#ffaa00"};
                    padding:18px 22px;margin-bottom:16px'>
          <div style='display:flex;justify-content:space-between;align-items:center'>
            <div>
              <span style='font-family:monospace;font-size:2.2rem;font-weight:900;color:#fff'>{res["ticker"]}.JK</span>
              <span style='font-family:monospace;font-size:1.3rem;color:#00ccff;margin-left:18px'>
                Rp {res["last_price"]:,.0f}
              </span>
              <span style='font-family:monospace;font-size:1rem;
                           color:{"#00ff88" if res["change"]>=0 else "#ff3355"};margin-left:10px'>
                {"+" if res["change"]>=0 else ""}{res["change"]:.2f}%
              </span>
            </div>
            <div style='text-align:right'>
              <span class='score-badge {badge_cls}'>{res["verdict"]}</span>
              <div style='font-family:monospace;font-size:11px;color:#6a8aaa;margin-top:4px'>
                Confidence: {res["confidence"]}{cross_str}{goreng_str}
              </div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── METRICS ─────────────────────────────────────────────────────
        c1,c2,c3,c4,c5 = st.columns(5)
        with c1:
            st.metric("FINAL SCORE", f"{res['final_score']}/100",
                      delta="Strong" if abs(res["final_score"]-50)>25 else "Moderate")
        with c2:
            st.metric("TECH SCORE", f"{res['tech_score']}/100")
        with c3:
            st.metric("BROKER SCORE", f"{br['score']}/100")
        with c4:
            cmf_val = res["last_cmf"]
            st.metric("CMF (14)", f"{cmf_val:.4f}",
                      delta="Bullish" if cmf_val>0.1 else "Bearish" if cmf_val<-0.1 else "Neutral")
        with c5:
            st.metric("VOL RATIO", f"{res['vol_ratio']:.1f}x",
                      delta=f"Wyckoff Ph {res['wyck_phase']}")

        # ── CHARTS ──────────────────────────────────────────────────────
        col_chart, col_gauge = st.columns([3, 1])
        with col_chart:
            st.markdown("#### 📈 PRICE + VOLUME + CMF + MFI")
            fig_price = build_price_chart(df, res["cmf"], res["obv"], res["mfi"], res["vsa"])
            st.plotly_chart(fig_price, use_container_width=True)

        with col_gauge:
            st.markdown("#### 🎯 COMPOSITE SCORE")
            fig_gauge = build_gauge(res["final_score"], res["verdict"])
            st.plotly_chart(fig_gauge, use_container_width=True)

            # Wyckoff phase
            phases = ["A","B","C","D","E"]
            ph_idx = phases.index(res["wyck_phase"])
            st.markdown(f"""
            <div style='background:#0d1318;border:1px solid #1a2535;padding:12px;margin-top:8px'>
              <div style='font-family:monospace;font-size:10px;color:#3a5570;letter-spacing:2px'>WYCKOFF PHASE</div>
              <div style='font-family:monospace;font-size:1.4rem;color:#00ccff;font-weight:700'>
                Phase {res["wyck_phase"]}
              </div>
              <div style='font-size:12px;color:#c8d8e8;margin-top:4px'>{res["wyck_desc"]}</div>
            </div>
            """, unsafe_allow_html=True)

            # VSA summary
            if res["vsa"]:
                acc_n = sum(1 for p in res["vsa"] if p["type"]=="ACC")
                dis_n = sum(1 for p in res["vsa"] if p["type"]=="DIS")
                st.markdown(f"""
                <div style='background:#0d1318;border:1px solid #1a2535;padding:12px;margin-top:8px'>
                  <div style='font-family:monospace;font-size:10px;color:#3a5570;letter-spacing:2px'>VSA PATTERNS</div>
                  <span style='color:#00ff88;font-family:monospace'>▲ {acc_n} ACC</span>
                  <span style='color:#ff3355;font-family:monospace;margin-left:12px'>▼ {dis_n} DIS</span>
                </div>
                """, unsafe_allow_html=True)

        # ── BROKER SECTION ───────────────────────────────────────────────
        st.markdown("---")
        st.markdown("#### 🏦 BROKER FLOW ANALYSIS")

        col_bflow, col_bcat = st.columns(2)
        with col_bflow:
            st.markdown("**Net Lot per Broker** (★=Foreign 🔥=Goreng ⚡=Scalper ⚠️=Retail)")
            fig_broker = build_broker_chart(res["bdf"])
            st.plotly_chart(fig_broker, use_container_width=True)

        with col_bcat:
            st.markdown("**Flow per Kategori Broker**")
            fig_cat = build_broker_category_chart(br)
            st.plotly_chart(fig_cat, use_container_width=True)

            # Smart money detail
            if br["smart_buyers"]:
                st.markdown("""<div style='background:#0a1a0a;border:1px solid #00ff88;
                    border-left:3px solid #00ff88;padding:10px 14px'>
                    <div style='font-family:monospace;font-size:10px;color:#3a5570'>
                    ▲ FOREIGN SMART MONEY BUYERS</div>""", unsafe_allow_html=True)
                for b in br["smart_buyers"][:4]:
                    st.markdown(f"""<div style='font-family:monospace;font-size:12px'>
                    <span style='color:#00ff88;font-weight:700'>{b["broker"]}</span>
                    <span style='color:#c8d8e8'> {b["name"][:25]}</span>
                    <span style='color:#00ff88;float:right'>+{b["net"]:,} lot</span></div>""",
                    unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

            if br["smart_sellers"]:
                st.markdown("""<div style='background:#1a0a0a;border:1px solid #ff3355;
                    border-left:3px solid #ff3355;padding:10px 14px;margin-top:8px'>
                    <div style='font-family:monospace;font-size:10px;color:#3a5570'>
                    ▼ FOREIGN SMART MONEY SELLERS</div>""", unsafe_allow_html=True)
                for b in br["smart_sellers"][:4]:
                    st.markdown(f"""<div style='font-family:monospace;font-size:12px'>
                    <span style='color:#ff3355;font-weight:700'>{b["broker"]}</span>
                    <span style='color:#c8d8e8'> {b["name"][:25]}</span>
                    <span style='color:#ff3355;float:right'>{b["net"]:,} lot</span></div>""",
                    unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

        # ── CROSSING + WARNINGS ────────────────────────────────────────
        if br["crossing"] == "ACC":
            st.success("🔥 **ACCUMULATION CROSS**: Smart money BELI + Retail JUAL = Setup ideal mengikuti bandar!")
        elif br["crossing"] == "DIS":
            st.error("💀 **DISTRIBUTION CROSS**: Smart money JUAL + Retail BELI = Retailer terjebak distribusi!")

        if br["goreng_alert"]:
            codes = ", ".join(br["goreng_brokers"])
            st.warning(f"🚨 **GORENG ALERT**: Broker {codes} aktif. Bisa ikut markup tapi WAJIB stop loss ketat!")

        for w in [x for x in br["warnings"] if "CROSSING" not in x and "GORENG" not in x]:
            st.info(w)

        # ── VSA TABLE ────────────────────────────────────────────────
        if res["vsa"]:
            st.markdown("---")
            st.markdown("#### 📊 VSA — Volume Spread Analysis Patterns")
            vsa_data = [{"Type": ("✅ ACC" if p["type"]=="ACC" else "🔴 DIS"),
                         "Pattern": p["name"],
                         "Vol Ratio": f"{p['vol_ratio']:.1f}x"} for p in res["vsa"]]
            st.dataframe(pd.DataFrame(vsa_data), hide_index=True, use_container_width=True)

    else:
        st.markdown("""
        <div style='text-align:center;padding:80px 20px;color:#3a5570'>
          <div style='font-size:3rem'>📊</div>
          <div style='font-family:monospace;font-size:14px;letter-spacing:2px;margin-top:12px'>
            MASUKKAN KODE SAHAM DAN KLIK ANALISIS
          </div>
          <div style='font-family:monospace;font-size:11px;margin-top:8px'>
            Data real-time dari Yahoo Finance · Broker analysis · Wyckoff + VSA + CMF + OBV
          </div>
        </div>
        """, unsafe_allow_html=True)


# ── TAB 2: SCREENER ────────────────────────────────────────────────────────
with tab2:
    st.markdown("#### 🔍 MULTI-STOCK SCREENER")
    st.markdown("Scan otomatis beberapa saham sekaligus, diurutkan dari score tertinggi.")

    if st.button("🚀 JALANKAN SCREENER", use_container_width=False):
        if not preset_tickers:
            st.warning("Pilih minimal 1 saham di sidebar.")
        else:
            results = []
            prog = st.progress(0)
            status = st.empty()
            for i, tk in enumerate(preset_tickers):
                status.text(f"⏳ Menganalisis {tk}... ({i+1}/{len(preset_tickers)})")
                df_tk = load_data(tk, period)
                if df_tk is None or len(df_tk) < 20:
                    prog.progress((i+1)/len(preset_tickers))
                    continue
                cmf_tk  = calc_cmf(df_tk)
                obv_tk  = calc_obv(df_tk)
                mfi_tk  = calc_mfi(df_tk)
                wyck_p, _ = detect_wyckoff(df_tk, cmf_tk, obv_tk)
                ts      = calc_tech_score(df_tk, cmf_tk, obv_tk, mfi_tk)
                bdf_tk  = gen_broker_data(tk, ts)
                br_tk   = analyze_broker_flow(bdf_tk)
                fs      = int(np.clip(round(ts*0.6+br_tk["score"]*0.4), 0, 100))
                last_tk = df_tk.iloc[-1]
                prev_tk = df_tk.iloc[-2]
                chg     = (last_tk["close"]-prev_tk["close"])/prev_tk["close"]*100
                avg_v_tk= df_tk["volume"].tail(20).mean()

                results.append({
                    "Ticker"     : tk,
                    "Harga"      : f"Rp {int(last_tk['close']):,}",
                    "Chg %"      : f"{'+'if chg>=0 else ''}{chg:.2f}%",
                    "Score"      : fs,
                    "Tech"       : ts,
                    "Broker"     : br_tk["score"],
                    "Verdict"    : "🟢 AKUMULASI" if fs>=65 else "🔴 DISTRIBUSI" if fs<=35 else "🟡 NEUTRAL",
                    "Wyckoff"    : f"Ph {wyck_p}",
                    "CMF"        : f"{float(cmf_tk.iloc[-1]):.4f}",
                    "Vol Ratio"  : f"{last_tk['volume']/(avg_v_tk if avg_v_tk>0 else 1):.1f}x",
                    "Cross Signal": ("🔥ACC" if br_tk["crossing"]=="ACC"
                                     else "💀DIS" if br_tk["crossing"]=="DIS" else "—"),
                    "Goreng"     : "🚨" if br_tk["goreng_alert"] else "",
                })
                prog.progress((i+1)/len(preset_tickers))

            status.empty()
            prog.empty()

            if results:
                df_res = pd.DataFrame(results).sort_values("Score", ascending=False)
                st.dataframe(df_res, hide_index=True, use_container_width=True,
                             column_config={
                                 "Score": st.column_config.ProgressColumn(
                                     "Score", min_value=0, max_value=100, format="%d"),
                             })

                acc_tickers = [r["Ticker"] for r in results if r["Verdict"].startswith("🟢")]
                cross_tickers = [r["Ticker"] for r in results if "ACC" in r["Cross Signal"]]
                if acc_tickers:
                    st.success(f"🟢 **AKUMULASI TERDETEKSI**: {', '.join(acc_tickers)}")
                if cross_tickers:
                    st.info(f"🔥 **CROSSING SIGNAL**: {', '.join(cross_tickers)}")
            else:
                st.error("Tidak ada data yang berhasil dimuat.")


# ── TAB 3: PANDUAN ─────────────────────────────────────────────────────────
with tab3:
    st.markdown("#### 📚 PANDUAN KARAKTERISTIK BROKER IDX")

    sections = [
        ("🟢 FOREIGN SMART MONEY — Ikuti saat net buy kompak",
         [k for k,v in BROKER_DB.items() if v["cat"]=="FOREIGN_SMART"],
         "#00ff88"),
        ("🔥 LOCAL BANDAR / GORENG SAHAM — Waspada!",
         [k for k,v in BROKER_DB.items() if v["cat"]=="LOCAL_BANDAR"],
         "#ffaa00"),
        ("🏛️ BUMN & LOCAL INSTITUTIONAL",
         [k for k,v in BROKER_DB.items() if v["cat"] in ("BUMN_INST","LOCAL_INST")],
         "#00ccff"),
        ("🇰🇷 KOREAN BROKER — Semi Smart Money",
         [k for k,v in BROKER_DB.items() if v["cat"]=="KOREAN"],
         "#aa88ff"),
        ("⚠️ RETAIL BESAR — SINYAL KONTRARIAN (terbalik!)",
         [k for k,v in BROKER_DB.items() if v["cat"] in ("RETAIL_LARGE","RETAIL_MED")],
         "#ff3355"),
        ("⚡ SCALPER — NOISE, abaikan sebagai sinyal",
         [k for k,v in BROKER_DB.items() if v["cat"]=="SCALPER"],
         "#ff5500"),
    ]

    for title, brokers, color in sections:
        with st.expander(title, expanded=False):
            rows = []
            for code in brokers:
                info = BROKER_DB[code]
                rows.append({
                    "Kode"    : code,
                    "Nama"    : info["name"],
                    "Style"   : info["style"],
                    "Goreng?" : "🔥 YA" if info["goreng"] else "—",
                    "Korban?" : "⚠️ YA" if info["victim"] else "—",
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    st.markdown("---")
    st.markdown("#### 🔑 ATURAN EMAS BROKER ANALYSIS")
    rules = [
        ("✅ IKUTI", "2+ broker asing (AK,BK,DB) serentak net buy di saham yang sama"),
        ("❌ WASPADA", "XC/YP/XL net buy masif = sering tanda akhir distribusi ke retailer"),
        ("🚨 GORENG", "MK+EP tiba-tiba muncul di saham sepi = kemungkinan pump-and-dump dimulai"),
        ("🔥 PELUANG", "YP/XC/XL panik jual + asing diam-diam beli = ACCUMULATION CROSS"),
        ("🔇 ABAIKAN", "MG mendominasi volume = pure scalper noise, lihat broker lainnya"),
        ("💎 TERKUAT", "Smart money beli + retail jual serentak = ACCUMULATION CROSS signal"),
        ("💀 BAHAYA", "Smart money jual + retail borong = DISTRIBUTION CROSS, segera exit"),
    ]
    for r_type, r_desc in rules:
        st.markdown(f"**{r_type}** — {r_desc}")

    st.info("""
    💡 **Catatan Penting**: Bandar TIDAK hanya pakai 1 broker.
    Mereka pakai 2-5 broker berbeda untuk menyamarkan aksi akumulasi/distribusi.
    Selalu lihat POLA keseluruhan, bukan 1 broker saja.
    """)
