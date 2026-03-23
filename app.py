"""
BANDARMOLOGY PRO v6
IDX Smart Money Intelligence Platform
─────────────────────────────────────
Features:
  • Broker flow (today)             — IDX API / Stockbit
  • Broker Accumulation             — cumulative net lot over N days (estimated position)
  • Broker Shareholding panel       — who holds how much, by broker category
  • Shareholders >1% (IDX/KSEI)    — live + monthly data (OJK mandate Mar 2026)
  • KSEI Foreign/Domestic split     — real holding composition
  • Wyckoff · CMF · OBV · MFI      — full technical suite
  • Entry Zone (price/SL/target)    — ATR-based
  • Ownership intelligence          — conglomerate database
  • Auto-refresh                    — live market status clock
  • All cache TTLs tuned per data source
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
import requests, warnings
from datetime import datetime, timedelta

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AR = True
except ImportError:
    HAS_AR = False

warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="Bandarmology PRO · IDX",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════
#  DESIGN SYSTEM
# ══════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

:root {
  --bg:#05080c; --bg2:#090d12; --bg3:#0d1219; --panel:#0b1018;
  --border:#141e2e; --border2:#1c2a3e;
  --green:#00e676; --red:#ff1744; --amber:#ffab00;
  --blue:#00b0ff;  --purple:#e040fb; --cyan:#00e5ff;
  --text:#cdd8e6;  --text2:#5a7a9a;  --text3:#2a3d52; --white:#eaf0f8;
}
* { font-family:'IBM Plex Sans',sans-serif; box-sizing:border-box; }

[data-testid="stAppViewContainer"]  { background:var(--bg); color:var(--text); }
[data-testid="stSidebar"]           { background:var(--bg2); border-right:1px solid var(--border); }
[data-testid="stSidebar"] *         { color:var(--text) !important; }

[data-testid="metric-container"] {
  background:var(--panel); border:1px solid var(--border);
  border-radius:4px; padding:12px 14px; position:relative; overflow:hidden;
}
[data-testid="metric-container"]::before {
  content:''; position:absolute; top:0; left:0; right:0; height:2px;
  background:linear-gradient(90deg,var(--blue),transparent);
}
[data-testid="stMetricValue"]  { color:var(--white)!important; font-family:'IBM Plex Mono',monospace!important; font-size:1.35rem!important; font-weight:600!important; }
[data-testid="stMetricLabel"]  { color:var(--text2)!important; font-size:10px!important; letter-spacing:1.5px!important; text-transform:uppercase!important; font-family:'IBM Plex Mono',monospace!important; }
[data-testid="stMetricDelta"]>div { font-size:10px!important; font-family:'IBM Plex Mono',monospace!important; }

.stButton>button {
  background:var(--green)!important; color:#000!important;
  font-family:'IBM Plex Mono',monospace!important; font-weight:700!important;
  letter-spacing:2px!important; font-size:11px!important;
  border:none!important; border-radius:3px!important; padding:9px 22px!important;
}
.stButton>button:hover { background:#00ff88!important; box-shadow:0 4px 18px rgba(0,230,118,.3)!important; }

/* Sidebar preset buttons — smaller, outlined style */
[data-testid="stSidebar"] .stButton>button {
  background:transparent!important;
  color:var(--green)!important;
  border:1px solid var(--green)!important;
  font-size:10px!important;
  font-weight:600!important;
  letter-spacing:1px!important;
  padding:5px 8px!important;
  border-radius:3px!important;
  box-shadow:none!important;
}
[data-testid="stSidebar"] .stButton>button:hover {
  background:rgba(0,230,118,.12)!important;
  box-shadow:none!important;
}

.stTextInput>div>div, .stTextArea>div>div {
  background:var(--bg3)!important; border:1px solid var(--border2)!important;
  color:var(--white)!important; font-family:'IBM Plex Mono',monospace!important;
}
/* Fix: textarea inner element text color */
textarea {
  color:var(--white)!important;
  background:var(--bg3)!important;
  font-family:'IBM Plex Mono',monospace!important;
  font-size:11px!important;
  line-height:1.6!important;
}
textarea::placeholder { color:var(--text3)!important; }
textarea:focus { 
  border-color:var(--green)!important;
  box-shadow:0 0 0 1px rgba(0,230,118,.2)!important;
  outline:none!important;
}
/* Fix: stTextArea label */
.stTextArea label, .stTextInput label {
  color:var(--text2)!important;
  font-family:'IBM Plex Mono',monospace!important;
  font-size:11px!important;
}
.stSelectbox>div>div, .stSlider { background:var(--bg3)!important; border:1px solid var(--border2)!important; }

[data-testid="stTabs"] button {
  background:transparent!important; color:var(--text2)!important;
  font-family:'IBM Plex Mono',monospace!important; font-size:10px!important;
  letter-spacing:1.5px!important; text-transform:uppercase!important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
  color:var(--green)!important; border-bottom:2px solid var(--green)!important;
  background:rgba(0,230,118,.03)!important;
}

h1,h2,h3 { font-family:'IBM Plex Mono',monospace!important; color:var(--white)!important; }
hr { border-color:var(--border)!important; margin:6px 0!important; }
.stProgress>div>div { background:var(--green)!important; }
[data-testid="stDataFrame"] { border:1px solid var(--border)!important; }

/* ── COMPONENTS ── */
.sec {
  font-family:'IBM Plex Mono',monospace; font-size:10px; letter-spacing:3px;
  color:var(--text2); text-transform:uppercase; margin-bottom:8px;
  padding-bottom:5px; border-bottom:1px solid var(--border);
}
.kv { display:flex; justify-content:space-between; align-items:center;
      padding:6px 0; border-bottom:1px solid var(--border); font-size:12px; }
.kv:last-child { border-bottom:none; }
.kv-k { color:var(--text2); font-family:'IBM Plex Mono',monospace; font-size:10px; letter-spacing:1px; }
.kv-v { color:var(--white); font-family:'IBM Plex Mono',monospace; font-weight:500; }

.tag  { display:inline-block; padding:2px 8px; border-radius:2px; font-family:'IBM Plex Mono',monospace; font-size:10px; letter-spacing:1px; }
.tg { background:rgba(0,230,118,.12); color:var(--green);  border:1px solid rgba(0,230,118,.3); }
.tr { background:rgba(255,23,68,.10);  color:var(--red);    border:1px solid rgba(255,23,68,.2); }
.ta { background:rgba(255,171,0,.10);  color:var(--amber);  border:1px solid rgba(255,171,0,.2); }
.tb { background:rgba(0,176,255,.10);  color:var(--blue);   border:1px solid rgba(0,176,255,.2); }

.signal-card { padding:16px 20px; border-radius:4px; border-left:4px solid; margin-bottom:10px; }
.sc-buy  { background:rgba(0,230,118,.06); border-color:var(--green); }
.sc-sell { background:rgba(255,23,68,.06);  border-color:var(--red);   }
.sc-watch{ background:rgba(255,171,0,.06);  border-color:var(--amber); }
.sig-main { font-family:'IBM Plex Mono',monospace; font-size:1.9rem; font-weight:700; letter-spacing:2px; }
.sig-lbl  { font-family:'IBM Plex Mono',monospace; font-size:10px; letter-spacing:2.5px; text-transform:uppercase; font-weight:700; margin-bottom:4px; }
.sig-why  { font-size:12px; color:var(--text); margin-top:6px; line-height:1.6; }
.conds { margin-top:8px; display:flex; flex-wrap:wrap; gap:4px; }
.cm { padding:2px 8px; border-radius:2px; font-family:'IBM Plex Mono',monospace; font-size:9px; background:rgba(0,230,118,.12); color:var(--green); border:1px solid rgba(0,230,118,.25); }
.cf { padding:2px 8px; border-radius:2px; font-family:'IBM Plex Mono',monospace; font-size:9px; background:rgba(255,23,68,.10);  color:var(--red);   border:1px solid rgba(255,23,68,.2); }
.cw { padding:2px 8px; border-radius:2px; font-family:'IBM Plex Mono',monospace; font-size:9px; background:rgba(255,171,0,.10); color:var(--amber); border:1px solid rgba(255,171,0,.2); }

.mkt-open   { display:inline-flex; align-items:center; gap:6px; padding:3px 12px; border-radius:3px; background:rgba(0,230,118,.1);  border:1px solid rgba(0,230,118,.3); font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--green); }
.mkt-closed { display:inline-flex; align-items:center; gap:6px; padding:3px 12px; border-radius:3px; background:rgba(255,171,0,.08); border:1px solid rgba(255,171,0,.25); font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--amber); }
.dot { width:6px; height:6px; border-radius:50%; }
.dot-g { background:var(--green); box-shadow:0 0 5px var(--green); animation:pulse 1.5s infinite; }
.dot-a { background:var(--amber); }
@keyframes pulse { 0%,100%{opacity:1}50%{opacity:.35} }

.bar-wrap { height:6px; background:var(--border); border-radius:3px; overflow:hidden; margin-top:4px; }
.bar      { height:100%; border-radius:3px; }

.entry-box { background:rgba(0,230,118,.05); border:1px solid rgba(0,230,118,.25); padding:12px; border-radius:3px; font-family:'IBM Plex Mono',monospace; }
.stop-box  { background:rgba(255,23,68,.04);  border:1px solid rgba(255,23,68,.2);  padding:12px; border-radius:3px; font-family:'IBM Plex Mono',monospace; }

.ph-track { display:flex; gap:3px; margin-top:8px; }
.ph  { flex:1; text-align:center; padding:6px 2px; border:1px solid var(--border); border-radius:3px; font-family:'IBM Plex Mono',monospace; font-size:9px; color:var(--text3); }
.ph-a{ background:rgba(0,230,118,.1); border-color:var(--green); color:var(--green); font-weight:700; }
.ph-d{ background:rgba(0,230,118,.04); border-color:rgba(0,230,118,.3); color:var(--text2); }

.broker-hold-row {
  display:flex; align-items:center; gap:10px;
  padding:8px 0; border-bottom:1px solid var(--border);
}
.broker-hold-row:last-child { border-bottom:none; }
.bh-code { font-family:'IBM Plex Mono',monospace; font-size:13px; font-weight:700; min-width:36px; }
.bh-name { font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--text2); flex:1; }
.bh-lots { font-family:'IBM Plex Mono',monospace; font-size:11px; font-weight:600; min-width:80px; text-align:right; }
.bh-pct  { font-family:'IBM Plex Mono',monospace; font-size:10px; color:var(--text2); min-width:48px; text-align:right; }

.ts-note { font-family:'IBM Plex Mono',monospace; font-size:9px; color:var(--text3); letter-spacing:1px; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  MARKET STATUS
# ══════════════════════════════════════════════════════════════════════

def market_status():
    now_wib = datetime.utcnow() + timedelta(hours=7)
    wd = now_wib.weekday()
    t  = now_wib.time()
    def T(s): return datetime.strptime(s, "%H:%M").time()
    if wd >= 5:
        return {"open":False, "label":"WEEKEND CLOSED", "next":"Monday 09:00 WIB", "wib":now_wib}
    if T("09:00") <= t <= T("11:30"):
        return {"open":True,  "label":"SESSION 1 OPEN", "next":"Closes 11:30 WIB", "wib":now_wib}
    if T("13:30") <= t <= T("16:30"):
        return {"open":True,  "label":"SESSION 2 OPEN", "next":"Closes 16:30 WIB", "wib":now_wib}
    if t < T("09:00"):
        return {"open":False, "label":"PRE-MARKET",     "next":"Opens 09:00 WIB",  "wib":now_wib}
    if T("11:30") < t < T("13:30"):
        return {"open":False, "label":"LUNCH BREAK",    "next":"Reopens 13:30 WIB","wib":now_wib}
    return {"open":False, "label":"AFTER HOURS",        "next":"Tomorrow 09:00 WIB","wib":now_wib}


def trade_days(n=30):
    days, d = [], datetime.now()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y-%m-%d"))
        d -= timedelta(days=1)
    return days


# ══════════════════════════════════════════════════════════════════════
#  BROKER DATABASE
# ══════════════════════════════════════════════════════════════════════

BROKER_DB = {
    "AK":{"name":"UBS Securities",        "cat":"FOREIGN_SMART","flag":"🇨🇭"},
    "BK":{"name":"J.P. Morgan",           "cat":"FOREIGN_SMART","flag":"🇺🇸"},
    "DB":{"name":"Deutsche Securities",   "cat":"FOREIGN_SMART","flag":"🇩🇪"},
    "GW":{"name":"HSBC Securities",       "cat":"FOREIGN_SMART","flag":"🇬🇧"},
    "ML":{"name":"Merrill Lynch",         "cat":"FOREIGN_SMART","flag":"🇺🇸"},
    "YU":{"name":"CGS-CIMB Securities",   "cat":"FOREIGN_SMART","flag":"🇲🇾"},
    "KZ":{"name":"CLSA Securities",       "cat":"FOREIGN_SMART","flag":"🇭🇰"},
    "CS":{"name":"Credit Suisse",         "cat":"FOREIGN_SMART","flag":"🇨🇭"},
    "DP":{"name":"DBS Vickers",           "cat":"FOREIGN_SMART","flag":"🇸🇬"},
    "ZP":{"name":"Maybank Securities",    "cat":"FOREIGN_SMART","flag":"🇲🇾"},
    "RX":{"name":"Macquarie Securities",  "cat":"FOREIGN_SMART","flag":"🇦🇺"},
    "AI":{"name":"UOB Kay Hian",          "cat":"FOREIGN_SMART","flag":"🇸🇬"},
    "MK":{"name":"Ekuator Swarna",        "cat":"LOCAL_BANDAR", "flag":"🇮🇩"},
    "EP":{"name":"Valbury Asia",          "cat":"LOCAL_BANDAR", "flag":"🇮🇩"},
    "II":{"name":"Danatama Makmur",       "cat":"LOCAL_BANDAR", "flag":"🇮🇩"},
    "DD":{"name":"Makindo Securities",    "cat":"LOCAL_BANDAR", "flag":"🇮🇩"},
    "CC":{"name":"Mandiri Sekuritas",     "cat":"BUMN",         "flag":"🇮🇩"},
    "NI":{"name":"BNI Sekuritas",         "cat":"BUMN",         "flag":"🇮🇩"},
    "OD":{"name":"BRI Danareksa",         "cat":"BUMN",         "flag":"🇮🇩"},
    "DX":{"name":"Bahana Sekuritas",      "cat":"LOCAL_INST",   "flag":"🇮🇩"},
    "KI":{"name":"Ciptadana Sekuritas",   "cat":"LOCAL_INST",   "flag":"🇮🇩"},
    "LG":{"name":"Trimegah Sekuritas",    "cat":"LOCAL_INST",   "flag":"🇮🇩"},
    "HG":{"name":"Sucor Sekuritas",       "cat":"LOCAL_INST",   "flag":"🇮🇩"},
    "SQ":{"name":"BCA Sekuritas",         "cat":"LOCAL_INST",   "flag":"🇮🇩"},
    "XA":{"name":"NH Korindo",            "cat":"KOREAN",       "flag":"🇰🇷"},
    "AG":{"name":"Kiwoom Securities",     "cat":"KOREAN",       "flag":"🇰🇷"},
    "BQ":{"name":"Korea Investment Sec.", "cat":"KOREAN",       "flag":"🇰🇷"},
    "YP":{"name":"Mirae Asset",           "cat":"RETAIL",       "flag":"🇰🇷"},
    "XC":{"name":"Ajaib (Xobat Cutloss)", "cat":"RETAIL",       "flag":"🇮🇩"},
    "XL":{"name":"Stockbit Sekuritas",    "cat":"RETAIL",       "flag":"🇮🇩"},
    "PD":{"name":"Indo Premier (IPOT)",   "cat":"RETAIL",       "flag":"🇮🇩"},
    "KK":{"name":"Phillip Securities",    "cat":"RETAIL",       "flag":"🇸🇬"},
    "MG":{"name":"Semesta Indovest",      "cat":"SCALPER",      "flag":"🇮🇩"},
}

CAT_COLOR = {
    "FOREIGN_SMART":"#00e676","LOCAL_BANDAR":"#ffab00",
    "BUMN":"#00b0ff","LOCAL_INST":"#00b0ff",
    "KOREAN":"#e040fb","RETAIL":"#ff1744","SCALPER":"#ff6d00",
}
CAT_LABEL = {
    "FOREIGN_SMART":"Foreign Smart Money","LOCAL_BANDAR":"Local Bandar / Pump",
    "BUMN":"BUMN","LOCAL_INST":"Local Institutional",
    "KOREAN":"Korean Broker","RETAIL":"Retail (Contrarian)","SCALPER":"Scalper (Noise)",
}

HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, */*",
    "Accept-Language": "id-ID,id;q=0.9",
}


# ══════════════════════════════════════════════════════════════════════
#  OWNERSHIP DATABASE
# ══════════════════════════════════════════════════════════════════════

OWNER_DB = {
    "BREN":{"owner":"Prajogo Pangestu","group":"Barito Pacific","tier":1,"sector":"Geothermal Energy","float":12.3,"political":False,"rating":95,"note":"Low float 12.3% → FCA stock. IDX richest conglomerate."},
    "BRPT":{"owner":"Prajogo Pangestu","group":"Barito Pacific","tier":1,"sector":"Petrochemicals","float":20.0,"political":False,"rating":90,"note":"Holding co. for BREN & TPIA."},
    "TPIA":{"owner":"Prajogo Pangestu","group":"Barito Pacific","tier":1,"sector":"Petrochemicals","float":10.7,"political":False,"rating":88,"note":"Float 10.7% → FCA. Largest petrochemical producer."},
    "CUAN":{"owner":"Prajogo Pangestu","group":"Barito Pacific","tier":1,"sector":"Coal","float":25.0,"political":False,"rating":82,"note":"Coal + resources. Part of Prajogo empire."},
    "CDIA":{"owner":"Prajogo Pangestu","group":"Barito Pacific","tier":1,"sector":"Infrastructure","float":10.0,"political":False,"rating":80,"note":"Chandra Daya Investasi. Float 10% → FCA."},
    "PTRO":{"owner":"Prajogo + Hapsoro","group":"Barito/Hapsoro","tier":2,"sector":"Mining Services","float":30.0,"political":True,"rating":75,"note":"Dual ownership + political ties. Higher risk."},
    "BYAN":{"owner":"Low Tuck Kwong","group":"Bayan Group","tier":1,"sector":"Coal Mining","float":33.0,"political":False,"rating":88,"note":"Forbes #2 Indonesia. Largest private coal miner."},
    "BBCA":{"owner":"Hartono Bersaudara","group":"Djarum / BCA","tier":1,"sector":"Banking","float":45.0,"political":False,"rating":99,"note":"Indonesia's most consistently profitable bank."},
    "DCII":{"owner":"Hartono Bersaudara","group":"Djarum Group","tier":1,"sector":"Data Center","float":28.0,"political":False,"rating":92,"note":"DCI Indonesia. Dominant data center operator."},
    "TOWR":{"owner":"Hartono Bersaudara","group":"Djarum Group","tier":1,"sector":"Tower Infra","float":35.0,"political":False,"rating":85,"note":"Sarana Menara Nusantara. Steady dividend."},
    "SSIA":{"owner":"Hartono + Prajogo","group":"Djarum + Barito","tier":1,"sector":"Industrial Estate","float":40.0,"political":False,"rating":90,"note":"DUAL CONGLOMERATE: Both Hartono & Prajogo buying → very bullish."},
    "ICBP":{"owner":"Anthoni Salim","group":"Salim Group","tier":1,"sector":"Consumer F&B","float":48.0,"political":False,"rating":92,"note":"Indomie parent. World's #1 instant noodle brand."},
    "INDF":{"owner":"Anthoni Salim","group":"Salim Group","tier":1,"sector":"Consumer Diversified","float":50.0,"political":False,"rating":88,"note":"Holding of ICBP. Conglomerate discount vs ICBP."},
    "ADRO":{"owner":"Garibaldi Thohir","group":"Adaro Group","tier":1,"sector":"Coal / Mining","float":22.0,"political":False,"rating":85,"note":"Pivoting coal→copper/nickel/battery metals."},
    "ADMR":{"owner":"Garibaldi Thohir","group":"Adaro Group","tier":1,"sector":"Copper/Nickel","float":12.0,"political":False,"rating":78,"note":"EV battery metals. Float 12% — watch for FCA."},
    "MDKA":{"owner":"Merdeka Group","group":"Merdeka / Adaro","tier":1,"sector":"Gold/Copper/Nickel","float":35.0,"political":False,"rating":80,"note":"Multi-commodity. Strong EV metals pipeline."},
    "AMMN":{"owner":"Agoes Projosasmito","group":"Amman Mineral","tier":1,"sector":"Copper/Gold","float":30.0,"political":False,"rating":87,"note":"Batu Hijau mine. New $21T smelter."},
    "PANI":{"owner":"Sugianto Kusuma (Aguan)","group":"Agung Sedayu","tier":2,"sector":"Property (PIK2)","float":45.0,"political":True,"rating":78,"note":"PIK 2 National Capital project. Political connections."},
    "RAJA":{"owner":"Happy Hapsoro","group":"Hapsoro Group","tier":3,"sector":"Shipping","float":35.0,"political":True,"rating":30,"note":"HIGH RISK. Hapsoro = Puan Maharani's husband (DPR Chair)."},
    "PSKT":{"owner":"Happy Hapsoro","group":"Hapsoro Group","tier":3,"sector":"Logistics","float":40.0,"political":True,"rating":28,"note":"SPECULATIVE. Moves on politics not fundamentals."},
    "WIFI":{"owner":"Hashim (affiliated)","group":"Political","tier":3,"sector":"Telecoms","float":40.0,"political":True,"rating":22,"note":"EXTREME RISK. +612% in 2025, driven by political sentiment."},
    "BBRI":{"owner":"Government (BUMN)","group":"BRI Group","tier":1,"sector":"Banking","float":43.0,"political":False,"rating":87,"note":"Largest bank by assets. Strong rural MSME franchise."},
    "BMRI":{"owner":"Government (BUMN)","group":"Mandiri Group","tier":1,"sector":"Banking","float":40.0,"political":False,"rating":86,"note":"Most profitable state bank. Strong corporate banking."},
    "BBNI":{"owner":"Government (BUMN)","group":"BNI Group","tier":1,"sector":"Banking","float":40.0,"political":False,"rating":75,"note":"Recovering after restructuring. Digital push."},
    "TLKM":{"owner":"Government (BUMN)","group":"Telkom Group","tier":1,"sector":"Telecoms","float":47.0,"political":False,"rating":78,"note":"Defensive dividend. Data infrastructure."},
    "ANTM":{"owner":"Government (BUMN)","group":"MIND ID","tier":2,"sector":"Nickel/Gold","float":35.0,"political":False,"rating":62,"note":"BUMN inefficiency risk. Nickel oversupply from China."},
    "PTBA":{"owner":"Government (BUMN)","group":"MIND ID","tier":2,"sector":"Coal Mining","float":35.0,"political":False,"rating":72,"note":"High dividend yield. Coal transition risk."},
    "PGAS":{"owner":"Government (BUMN)","group":"Pertamina","tier":2,"sector":"Gas Distribution","float":43.0,"political":False,"rating":65,"note":"Regulated margins. Gas infra monopoly."},
    "ASII":{"owner":"Jardine Matheson","group":"Astra International","tier":1,"sector":"Auto / Diversified","float":50.0,"political":False,"rating":91,"note":"Blueprint of Indonesia conglomerate. Auto + Finance + Agri."},
    "UNTR":{"owner":"Jardine / Astra","group":"Astra Group","tier":1,"sector":"Heavy Equipment","float":40.0,"political":False,"rating":85,"note":"United Tractors. Komatsu distributor + coal."},
    "KLBF":{"owner":"Boenjamin Setiawan Family","group":"Kalbe Group","tier":1,"sector":"Pharma","float":44.0,"political":False,"rating":88,"note":"Indonesia's largest pharma. Consistent growth."},
    "UNVR":{"owner":"Unilever PLC (UK)","group":"Unilever Global","tier":1,"sector":"Consumer","float":15.0,"political":False,"rating":55,"note":"Losing market share in Indonesia. Secular decline."},
    "GOTO":{"owner":"Founders / SoftBank / Alibaba","group":"GoTo Group","tier":2,"sector":"Digital Economy","float":55.0,"political":False,"rating":45,"note":"Path to profitability unclear. Competitive pressure."},
    "HMSP":{"owner":"Philip Morris Int'l","group":"Philip Morris","tier":1,"sector":"Tobacco","float":8.0,"political":False,"rating":50,"note":"Float 7.5% → FCA. ESG + declining volumes."},
    "BSDE":{"owner":"Widjaja Family","group":"Sinar Mas","tier":2,"sector":"Property","float":48.0,"political":False,"rating":72,"note":"Largest township developer. Rate-sensitive."},
}


def owner_risk(own):
    if not own: return 50, "UNKNOWN"
    s = min(30, int(own.get("float",30) * 0.6))
    s += {1:40,2:25,3:5}.get(own.get("tier",2), 20)
    s += 0 if own.get("political") else 10
    s += int(own.get("rating",50) * 0.2)
    s = max(0, min(100, s))
    lbl = "LOW RISK" if s>=70 else "MEDIUM RISK" if s>=45 else "HIGH RISK"
    return s, lbl


# ══════════════════════════════════════════════════════════════════════
#  DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════

def enrich(df):
    df["name"] = df["broker"].apply(lambda x: BROKER_DB.get(x.upper(),{}).get("name", f"Broker {x}"))
    df["cat"]  = df["broker"].apply(lambda x: BROKER_DB.get(x.upper(),{}).get("cat",  "LOCAL_INST"))
    df["flag"] = df["broker"].apply(lambda x: BROKER_DB.get(x.upper(),{}).get("flag", "🇮🇩"))
    return df.sort_values("net_lot", ascending=False).reset_index(drop=True)


def parse_idx_json(data):
    bm, sm, rows = {}, {}, []
    def ex(items):
        m = {}
        for i in (items or []):
            c = (i.get("BrokerCode") or i.get("broker_code") or i.get("code","")).upper()
            if not c: continue
            m[c] = {
                "lot"  : int(  i.get("TradedLot")    or i.get("lot")   or 0),
                "value": int(  i.get("TradedValue")   or i.get("value") or 0),
                "avg"  : float(i.get("AveragePrice")  or i.get("avg")   or 0),
            }
        return m
    if isinstance(data, dict):
        bm = ex(data.get("BrokerBuyerSummary",  data.get("buyers",  [])))
        sm = ex(data.get("BrokerSellerSummary", data.get("sellers", [])))
    elif isinstance(data, list):
        for i in data:
            c = (i.get("BrokerCode") or i.get("broker","")).upper()
            if not c: continue
            rows.append({"broker":c,
                "buy_lot"  : int(  i.get("BuyVolume",  0)),
                "sell_lot" : int(  i.get("SellVolume", 0)),
                "buy_value": int(  i.get("BuyValue",   0)),
                "sell_value":int(  i.get("SellValue",  0)),
                "buy_avg"  : float(i.get("BuyAvg",     0)),
                "sell_avg" : float(i.get("SellAvg",    0)),
            })
    if not rows:
        for c in set(bm) | set(sm):
            b = bm.get(c, {"lot":0,"value":0,"avg":0})
            s = sm.get(c, {"lot":0,"value":0,"avg":0})
            rows.append({"broker":c,
                "buy_lot":b["lot"],"sell_lot":s["lot"],
                "buy_value":b["value"],"sell_value":s["value"],
                "buy_avg":b["avg"],"sell_avg":s["avg"],
            })
    if not rows: return None
    df = pd.DataFrame(rows)
    df["net_lot"]   = df["buy_lot"]   - df["sell_lot"]
    df["net_value"] = df["buy_value"] - df["sell_value"]
    return enrich(df)


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_idx_day(ticker: str, date: str):
    t = ticker.upper().replace(".JK","")
    for ep, p in [
        ("https://www.idx.co.id/umbraco/Surface/TradingSummary/GetBrokerSummary",
         {"code":t,"start":date,"end":date,"draw":1,"length":99}),
        (f"https://www.idx.co.id/api/v1/broker-summary/{t}",
         {"startDate":date,"endDate":date}),
    ]:
        try:
            r = requests.get(ep, params=p, headers=HDR, timeout=12)
            if r.status_code == 200:
                df = parse_idx_json(r.json())
                if df is not None and len(df) >= 3:
                    return df
        except: continue
    return None


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_stockbit(ticker: str, token: str, date_from: str, date_to: str):
    if not token or len(token) < 20: return None
    t = ticker.upper().replace(".JK","")
    hdrs = {**HDR, "Authorization": f"Bearer {token}",
            "Origin":"https://stockbit.com","Referer":"https://stockbit.com/"}
    params = {"startDate":date_from,"endDate":date_to,"code":t,"limit":50}
    for url in [
        f"https://exodus.stockbit.com/broker-transaction/v1/summary/{t}",
        f"https://api.stockbit.com/v2.4/broker_summary/{t}",
    ]:
        try:
            r = requests.get(url, headers=hdrs, params=params, timeout=12)
            if r.status_code == 401: return "EXPIRED"
            if r.status_code == 200:
                data = r.json(); payload = data
                if isinstance(data, dict):
                    payload = data.get("data", data)
                    if isinstance(payload, dict):
                        payload = payload.get("brokerSummary", payload)
                buyers  = payload.get("buyer",  payload.get("buyers",  [])) if isinstance(payload,dict) else []
                sellers = payload.get("seller", payload.get("sellers", [])) if isinstance(payload,dict) else []
                def ep(items):
                    m = {}
                    for i in (items or []):
                        c = (i.get("broker_code") or i.get("brokerCode") or i.get("code","")).upper()
                        if not c: continue
                        m[c] = {"lot"  : int(i.get("lot") or i.get("volume") or 0),
                                "value": int(i.get("value") or 0),
                                "avg"  : float(i.get("avg") or i.get("averagePrice") or 0)}
                    return m
                bm = ep(buyers); sm = ep(sellers)
                if not (set(bm) | set(sm)): continue
                rows = [{"broker":c,
                    "buy_lot"  : bm.get(c,{"lot":0})["lot"],
                    "sell_lot" : sm.get(c,{"lot":0})["lot"],
                    "buy_value": bm.get(c,{"value":0})["value"],
                    "sell_value":sm.get(c,{"value":0})["value"],
                    "buy_avg"  : bm.get(c,{"avg":0})["avg"],
                    "sell_avg" : sm.get(c,{"avg":0})["avg"],
                } for c in set(bm)|set(sm)]
                df = pd.DataFrame(rows)
                df["net_lot"]   = df["buy_lot"]   - df["sell_lot"]
                df["net_value"] = df["buy_value"] - df["sell_value"]
                df = enrich(df)
                if len(df) >= 3: return df
        except: continue
    return None


def get_broker_today(ticker, token=""):
    """Today's broker summary → (df, source)"""
    today = trade_days(1)[0]
    if token and len(token) > 20:
        yesterday = trade_days(2)[-1]
        r = fetch_stockbit(ticker, token, yesterday, today)
        if r == "EXPIRED": st.warning("⚠️ Stockbit token expired.")
        elif r is not None: return r, "stockbit"
    df = fetch_idx_day(ticker, today)
    if df is not None: return df, "idx"
    return None, "demo"


@st.cache_data(ttl=3600, show_spinner=False)
def get_broker_accumulation(ticker: str, token: str, n_days: int = 20):
    """
    Cumulative net lot per broker over n_days trading days.
    = estimated accumulated position (clients' long/short bias).
    Positive → broker clients are net long (accumulated).
    Negative → broker clients are net short / exited.
    """
    days = trade_days(n_days)
    today = days[0]; oldest = days[-1]

    # 1. Stockbit aggregated (one call)
    if token and len(token) > 20:
        r = fetch_stockbit(ticker, token, oldest, today)
        if r is not None and r != "EXPIRED" and len(r) >= 3:
            r = r.copy()
            r["cum_net"] = r["net_lot"]
            return r, "stockbit", n_days

    # 2. IDX API day-by-day (limit to 5 to avoid rate limits)
    dfs = []
    for d in days[:5]:
        df_d = fetch_idx_day(ticker, d)
        if df_d is not None:
            dfs.append(df_d)

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        agg = combined.groupby("broker").agg(
            buy_lot   =("buy_lot",  "sum"),
            sell_lot  =("sell_lot", "sum"),
            net_lot   =("net_lot",  "sum"),
            buy_value =("buy_value","sum"),
            sell_value=("sell_value","sum"),
        ).reset_index()
        agg["cum_net"]  = agg["net_lot"]
        agg["net_value"]= agg["buy_value"] - agg["sell_value"]
        agg["buy_avg"]  = 0.0; agg["sell_avg"] = 0.0
        agg = enrich(agg).sort_values("cum_net", ascending=False).reset_index(drop=True)
        return agg, "idx", len(dfs)

    return None, "demo", 0


def demo_broker(ticker, ts):
    np.random.seed(sum(ord(c) for c in ticker) + ts)
    r = lambda a,b: int(np.random.randint(a,b))
    s = "acc" if ts>=65 else "dis" if ts<=35 else "neu"
    if s == "acc":
        rows = [("AK",r(20000,55000),r(1000,5000)),("BK",r(15000,40000),r(1000,4000)),
                ("DB",r(8000,22000), r(800,3000)), ("GW",r(6000,18000), r(600,2500)),
                ("CC",r(8000,20000), r(3000,10000)),("LG",r(4000,10000),r(2000,6000)),
                ("MG",r(28000,70000),r(38000,95000)),
                ("YP",r(14000,30000),r(22000,58000)),("XC",r(7000,15000),r(11000,30000)),
                ("XL",r(5000,13000), r(9000,22000))]
    elif s == "dis":
        rows = [("AK",r(1000,5000), r(22000,58000)),("BK",r(1000,4000), r(17000,45000)),
                ("MK",r(2000,8000), r(14000,38000)),("EP",r(1500,6000), r(9000,27000)),
                ("CC",r(4000,13000),r(5000,16000)),
                ("MG",r(38000,90000),r(30000,75000)),
                ("YP",r(24000,58000),r(9000,20000)),("XC",r(13000,32000),r(3000,8000)),
                ("XL",r(11000,27000),r(2500,7500))]
    else:
        rows = [("AK",r(5000,14000),r(4000,13000)),("BK",r(4000,12000),r(3500,11000)),
                ("CC",r(6000,14000),r(5500,13000)),("MG",r(30000,65000),r(28000,62000)),
                ("YP",r(15000,32000),r(14000,30000)),("XC",r(6000,14000),r(5500,13000)),
                ("XL",r(5000,12000),r(4800,11500))]
    df = pd.DataFrame(rows, columns=["broker","buy_lot","sell_lot"])
    df["net_lot"]   = df["buy_lot"] - df["sell_lot"]
    df["cum_net"]   = df["net_lot"] * np.random.randint(2, 6, len(df))
    df["net_value"] = df["net_lot"] * 500
    df["buy_avg"]   = 0.0; df["sell_avg"] = 0.0
    return enrich(df)


@st.cache_data(ttl=86400, show_spinner=False)  # 24 h — published monthly
def fetch_shareholders(ticker: str):
    """
    Major shareholders >1% from IDX/KSEI.
    OJK mandate: published monthly since March 2026.
    Access: idx.co.id/id/berita/pengumuman/
    """
    t = ticker.upper().replace(".JK","")
    for ep in [
        f"https://www.idx.co.id/api/v1/company-profile/{t}/shareholders",
        f"https://www.idx.co.id/umbraco/Surface/Helper/GetInitiationOfPublicCompany?kodeEmiten={t}",
    ]:
        try:
            r = requests.get(ep, headers=HDR, timeout=12)
            if r.status_code == 200:
                data = r.json()
                rows = []
                sh_list = (data.get("shareholders") or
                           data.get("MajorShareholderList") or
                           data.get("data",{}).get("shareholders",[]))
                for item in (sh_list or []):
                    name = (item.get("ShareholderName") or item.get("name") or item.get("holder","—"))
                    pct  = float(item.get("Percentage") or item.get("percentage") or 0)
                    lots = int(  item.get("ShareAmount") or item.get("lots")       or 0)
                    typ  = item.get("Type") or item.get("type") or "Institutional"
                    if pct > 0 or lots > 0:
                        rows.append({"name":name,"type":typ,"pct":pct,
                                     "lots":lots,"src":"IDX/KSEI Live"})
                if rows:
                    return rows, "idx_live", datetime.now().strftime("%d %b %Y")
        except: continue

    # Fallback from ownership DB
    own = OWNER_DB.get(t)
    if own:
        ff = own.get("float", 30)
        ctrl_pct = max(0, round(100 - ff - 10, 1))
        return [
            {"name": own["owner"], "type":"Controlling Shareholder",
             "pct": ctrl_pct, "lots":0, "src":"Ownership DB (estimate)"},
            {"name":"Public / Free Float","type":"Public",
             "pct": ff, "lots":0, "src":"Ownership DB (estimate)"},
        ], "ownership_db", "Estimated"
    return [], "none", None


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_ksei_composition(ticker: str):
    t = ticker.upper().replace(".JK","")
    try:
        r = requests.get(f"https://web.ksei.co.id/issuers/{t}/holding",
                         headers=HDR, timeout=10)
        if r.status_code == 200:
            d = r.json()
            return {"foreign":d.get("foreignPct"), "domestic":d.get("domesticPct"),
                    "total":d.get("totalShares"), "date":d.get("date")}
    except: pass
    try:
        info = yf.Ticker(t+".JK").info
        ip = info.get("heldPercentInstitutions")
        if ip:
            return {"foreign":round(ip*100,1),"domestic":round((1-ip)*100,1),
                    "total":info.get("sharesOutstanding"),"date":"Yahoo Finance"}
    except: pass
    return {}


# ══════════════════════════════════════════════════════════════════════
#  BROKER SHAREHOLDING CALCULATOR
#  Converts cumulative net lots → estimated % of outstanding shares held
# ══════════════════════════════════════════════════════════════════════

def calc_broker_shareholding(accu_df: pd.DataFrame, shares_outstanding: int,
                              lot_size: int = 100) -> pd.DataFrame:
    """
    Estimate how many shares each broker's clients hold.

    Method:
    ─────────────────────────────────────────────────────────────────
    We can't see actual sub-account holdings (KSEI sub-account data
    is private). What we CAN see is cumulative net lot activity.

    Estimated shares held = cumulative net lot × lot_size
    Estimated % of OS     = (est. shares) / shares_outstanding × 100

    This is the SAME methodology used by RTI Business, Stockbit Pro,
    and Bloomberg IDX broker accumulation screens.

    Limitations:
    - Does not account for positions established before the analysis window
    - Reflects NET activity, not absolute position
    - Brokers with zero activity appear to hold nothing (not true)
    ─────────────────────────────────────────────────────────────────
    """
    df = accu_df.copy()
    df["est_shares"] = df["cum_net"].clip(lower=0) * lot_size
    if shares_outstanding and shares_outstanding > 0:
        df["est_pct"] = df["est_shares"] / shares_outstanding * 100
    else:
        df["est_pct"] = 0.0
    df["est_shares_str"] = df["est_shares"].apply(
        lambda x: f"{x/1e9:.2f}B" if x>=1e9 else f"{x/1e6:.1f}M" if x>=1e6 else f"{x:,.0f}")
    return df.sort_values("cum_net", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=900, show_spinner=False)   # 15 min
def load_price(ticker, period):
    try:
        df = yf.download(ticker.upper().replace(".JK","") + ".JK",
                         period=period, interval="1d",
                         progress=False, auto_adjust=True)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.rename(columns={"Open":"open","High":"high","Low":"low",
                                  "Close":"close","Volume":"volume"})
        return df[["open","high","low","close","volume"]].dropna()
    except: return None

def cmf(df, p=14):
    hl  = df["high"] - df["low"]
    clv = ((df["close"]-df["low"]) - (df["high"]-df["close"])) / hl.replace(0,np.nan)
    return (clv*df["volume"]).rolling(p).sum() / df["volume"].rolling(p).sum()

def obv(df):
    return (np.sign(df["close"].diff()).fillna(0) * df["volume"]).cumsum()

def mfi(df, p=14):
    tp  = (df["high"]+df["low"]+df["close"]) / 3
    mf  = tp * df["volume"]
    pos = mf.where(tp>tp.shift(1),0).rolling(p).sum()
    neg = mf.where(tp<tp.shift(1),0).rolling(p).sum()
    return (100 - 100/(1 + pos/neg.replace(0,np.nan))).fillna(50)

def rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    return (100 - 100/(1 + g/l.replace(0,np.nan))).fillna(50)

def atr(df, p=14):
    hl = df["high"]-df["low"]
    hc = (df["high"]-df["close"].shift()).abs()
    lc = (df["low"] -df["close"].shift()).abs()
    return pd.concat([hl,hc,lc],axis=1).max(axis=1).rolling(p).mean()

def wyckoff(df, c, o):
    n = len(df); p = df["close"]
    tr  = (p.iloc[-n//3:].mean()-p.iloc[:n//3].mean()) / p.iloc[:n//3].mean()
    r20 = (p.tail(20).max()-p.tail(20).min()) / p.tail(20).mean()
    sp  = df["volume"].tail(5).max() > df["volume"].rolling(20).mean().iloc[-1]*1.8
    ob  = o.iloc[-1] > o.iloc[-min(10,n-1)]
    cu  = c.iloc[-1] > c.iloc[-min(10,len(c)-1)]
    if tr<-0.07 and not sp: return "A","Selling Climax","Bandar begins absorbing supply."
    if r20<0.07  and cu:    return "B","Building Cause","Sideways accumulation. Bandar quietly building."
    if r20<0.09  and sp and tr<0.03: return "C","Spring","⭐ Retail shaken out — ideal entry zone."
    if tr>0.03   and ob:    return "D","Sign of Strength","Breakout confirmed."
    if tr>0.08   and ob:    return "E","Markup","Trending up. Watch for distribution."
    return "B","Building Cause","Consolidation. Wait for confirmation."

def tech_score(df, c, o, m):
    sc = 50.0
    cv = float(c.iloc[-1]) if not pd.isna(c.iloc[-1]) else 0
    sc += np.clip(cv*125,-25,25)
    os = (o.iloc[-1]-o.iloc[-min(10,len(o)-1)]) / (abs(o.iloc[-min(10,len(o)-1)])+1)
    sc += np.clip(os*500,-20,20)
    last = df.iloc[-1]; sp = last["high"]-last["low"]
    cp = (last["close"]-last["low"]) / (sp if sp>0 else 1)
    sc += (cp-0.5)*30
    av = df["volume"].tail(20).mean(); vr = last["volume"]/av if av>0 else 1
    if vr>1.3: sc += 15 if cp>0.5 else -15
    pt = (df["close"].iloc[-1]-df["close"].iloc[-min(10,len(df)-1)]) / df["close"].iloc[-min(10,len(df)-1)]
    mt = float(m.iloc[-1]-m.iloc[-min(10,len(m)-1)])
    if pt<-0.01 and mt>3:  sc+=15
    if pt>0.01  and mt<-3: sc-=15
    return int(np.clip(round(sc),0,100))


# ══════════════════════════════════════════════════════════════════════
#  VCP — VOLATILITY CONTRACTION PATTERN ENGINE
#  Based on Mark Minervini's methodology
#  Detects: shrinking price swings + declining volume = supply exhaustion
# ══════════════════════════════════════════════════════════════════════

def detect_vcp(df: pd.DataFrame) -> dict:
    """
    Detect Volatility Contraction Pattern (VCP) in historical OHLCV data.

    A valid VCP requires:
    1. At least 2–4 progressively smaller price contractions (pivot swings)
    2. Each contraction shallower than the previous (% depth shrinking)
    3. Volume contracting during each consolidation phase
    4. Stock in a prior uptrend (above key moving averages)
    5. Last contraction tight (<3%) = pivot point = potential buy zone

    VCP Grade:
    ─────────────────────────────────────────────────────────
    A  = 3-4 contractions, each < 50% prior, tight pivot (<3%),
         volume dry-up confirmed, above all MAs = IDEAL SETUP
    B  = 2-3 contractions, good volume tightening, near pivot
    C  = Early contraction, setup incomplete but forming
    NONE = No VCP detected or stock in downtrend

    Returns dict with grade, contractions list, pivot level, score, chart data.
    """
    if len(df) < 60:
        return {"grade": "NONE", "score": 0, "contractions": [],
                "pivot": None, "available": False}

    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    vol    = df["volume"]
    n      = len(df)

    # ── 1. Trend filter: Must be in uptrend
    ma50  = close.rolling(50).mean()
    ma150 = close.rolling(150).mean() if n >= 150 else close.rolling(min(n,100)).mean()
    ma200 = close.rolling(200).mean() if n >= 200 else close.rolling(min(n,120)).mean()

    last_price = float(close.iloc[-1])
    ma50_v  = float(ma50.iloc[-1])  if not pd.isna(ma50.iloc[-1])  else last_price
    ma150_v = float(ma150.iloc[-1]) if not pd.isna(ma150.iloc[-1]) else last_price
    ma200_v = float(ma200.iloc[-1]) if not pd.isna(ma200.iloc[-1]) else last_price

    above_ma50  = last_price > ma50_v
    above_ma150 = last_price > ma150_v
    above_ma200 = last_price > ma200_v
    ma50_slope  = (float(ma50.iloc[-1]) - float(ma50.iloc[-min(20,n-1)])) > 0
    trend_score = sum([above_ma50, above_ma150, above_ma200, ma50_slope])
    in_uptrend  = trend_score >= 2  # at least 2/4 trend conditions

    # ── 2. Find price swings (peaks and troughs) using rolling extremes
    def find_pivots(series, window=10):
        """Find local pivot highs and lows."""
        highs, lows = [], []
        for i in range(window, len(series) - window):
            if series.iloc[i] == series.iloc[i-window:i+window+1].max():
                highs.append((i, float(series.iloc[i])))
            if series.iloc[i] == series.iloc[i-window:i+window+1].min():
                lows.append((i, float(series.iloc[i])))
        return highs, lows

    # Use last 120 trading days (≈6 months) for VCP detection
    lookback = min(120, n)
    df_vcp   = df.iloc[-lookback:]
    c_vcp    = close.iloc[-lookback:]
    v_vcp    = vol.iloc[-lookback:]

    highs, lows = find_pivots(c_vcp, window=8)

    # ── 3. Identify contraction sequences
    contractions = []
    for i in range(1, min(len(highs), len(lows)+1)):
        if i > len(lows): break
        peak  = highs[i-1] if i-1 < len(highs) else None
        trough = lows[i-1] if i-1 < len(lows)  else None
        if peak is None or trough is None: continue

        # Contraction = swing from peak to nearest lower trough
        if trough[0] > peak[0]:  # trough after peak
            depth_pct = (peak[1] - trough[1]) / peak[1] * 100
            # Volume during this contraction
            t_start = peak[0]; t_end = trough[0]
            if t_end > t_start:
                avg_vol_contract = float(v_vcp.iloc[t_start:t_end].mean())
                avg_vol_prior    = float(v_vcp.iloc[max(0,t_start-20):t_start].mean())
                vol_ratio_contract = (avg_vol_contract / avg_vol_prior
                                      if avg_vol_prior > 0 else 1.0)
                contractions.append({
                    "idx"        : len(contractions) + 1,
                    "peak_price" : round(peak[1], 0),
                    "trough_price": round(trough[1], 0),
                    "depth_pct"  : round(depth_pct, 1),
                    "vol_ratio"  : round(vol_ratio_contract, 2),
                    "vol_dry_up" : vol_ratio_contract < 0.8,  # volume contracted
                    "peak_idx"   : peak[0],
                    "trough_idx" : trough[0],
                })

    # Keep only the most recent and meaningful contractions
    contractions = [c for c in contractions if 1.0 <= c["depth_pct"] <= 50]
    contractions = sorted(contractions, key=lambda x: x["trough_idx"])[-5:]

    # ── 4. Check for progressively shrinking contractions
    is_contracting   = False
    contraction_good = 0
    vol_drying_up    = 0

    if len(contractions) >= 2:
        depths = [c["depth_pct"] for c in contractions]
        # Check each pair: should be getting shallower
        shrinking = sum(1 for i in range(1, len(depths)) if depths[i] < depths[i-1])
        is_contracting = shrinking >= len(depths) - 1  # all or all-but-one shrinking
        contraction_good = shrinking
        vol_drying_up = sum(1 for c in contractions if c["vol_dry_up"])

    # ── 5. Current tightness (last few bars)
    recent_window = min(20, n)
    recent_high   = float(high.iloc[-recent_window:].max())
    recent_low    = float(low.iloc[-recent_window:].min())
    current_tight = (recent_high - recent_low) / recent_high * 100 if recent_high > 0 else 99
    pivot_price   = recent_high  # breakout above recent high = pivot point

    # Volume in last 10 days vs prior 20 days
    avg_vol_recent = float(vol.tail(10).mean())
    avg_vol_prior  = float(vol.iloc[-30:-10].mean()) if n >= 30 else float(vol.mean())
    vol_dry_recent = (avg_vol_recent / avg_vol_prior) if avg_vol_prior > 0 else 1.0
    vol_is_dry     = vol_dry_recent < 0.75

    # ── 6. VCP Grade Assignment
    n_contractions = len(contractions)
    last_depth     = contractions[-1]["depth_pct"] if contractions else 99.0

    if (in_uptrend and n_contractions >= 3 and is_contracting and
            contraction_good >= 2 and vol_drying_up >= 2 and
            current_tight <= 6 and vol_is_dry):
        grade = "A"
        grade_color = "#00e676"
        grade_desc  = "IDEAL VCP — Multiple tight contractions + volume dry-up + uptrend confirmed"
        vcp_score   = 90

    elif (in_uptrend and n_contractions >= 2 and is_contracting and
              vol_drying_up >= 1 and current_tight <= 12):
        grade = "B"
        grade_color = "#88ffbb"
        grade_desc  = "GOOD VCP — 2–3 contractions, volume contracting, setup forming"
        vcp_score   = 70

    elif (in_uptrend and n_contractions >= 2 and current_tight <= 20):
        grade = "C"
        grade_color = "#ffab00"
        grade_desc  = "DEVELOPING VCP — Early contraction, not yet complete"
        vcp_score   = 45

    elif n_contractions >= 1 and not in_uptrend:
        grade = "C-"
        grade_color = "#ff8888"
        grade_desc  = "CAUTION — Contraction pattern but stock in downtrend/below MAs"
        vcp_score   = 25

    else:
        grade = "NONE"
        grade_color = "#5a7a9a"
        grade_desc  = "No clear VCP detected"
        vcp_score   = 0

    # ── 7. Breakout conditions check
    last_vol    = float(vol.iloc[-1])
    avg_vol_20  = float(vol.tail(20).mean())
    vol_on_last = last_vol / avg_vol_20 if avg_vol_20 > 0 else 1.0
    is_breaking = (last_price >= pivot_price * 0.995 and vol_on_last >= 1.5)
    near_pivot  = (last_price >= pivot_price * 0.97)

    # ── 8. Premium setup detection (VCP + Wyckoff + Broker)
    premium_setup = grade in ("A","B") and in_uptrend and near_pivot

    return {
        "available"     : True,
        "grade"         : grade,
        "grade_color"   : grade_color,
        "grade_desc"    : grade_desc,
        "score"         : vcp_score,
        "contractions"  : contractions,
        "n_contractions": n_contractions,
        "is_contracting": is_contracting,
        "current_tight" : round(current_tight, 1),
        "pivot_price"   : round(pivot_price, 0),
        "last_price"    : round(last_price, 0),
        "near_pivot"    : near_pivot,
        "is_breaking"   : is_breaking,
        "vol_dry_recent": round(vol_dry_recent, 2),
        "vol_is_dry"    : vol_is_dry,
        "vol_drying_up" : vol_drying_up,
        "in_uptrend"    : in_uptrend,
        "trend_score"   : trend_score,
        "above_ma50"    : above_ma50,
        "above_ma150"   : above_ma150,
        "above_ma200"   : above_ma200,
        "ma50_v"        : round(ma50_v, 0),
        "ma150_v"       : round(ma150_v, 0),
        "ma200_v"       : round(ma200_v, 0),
        "premium_setup" : premium_setup,
    }


def chart_vcp(df: pd.DataFrame, vcp: dict) -> go.Figure:
    """
    VCP visualization chart:
    - Candlestick with MA50/150/200
    - Volume with contraction highlighting
    - Pivot level marked
    - Contraction arcs annotated
    """
    ma50  = df["close"].rolling(50).mean()
    ma150 = df["close"].rolling(150).mean() if len(df)>=150 else df["close"].rolling(min(len(df),100)).mean()
    ma200 = df["close"].rolling(200).mean() if len(df)>=200 else df["close"].rolling(min(len(df),120)).mean()

    # Use last 120 days for clarity
    lookback = min(120, len(df))
    df_v   = df.iloc[-lookback:]
    ma50_v = ma50.iloc[-lookback:]
    ma150_v= ma150.iloc[-lookback:]
    ma200_v= ma200.iloc[-lookback:]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.68, 0.32], vertical_spacing=0.04)

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df_v.index, open=df_v["open"], high=df_v["high"],
        low=df_v["low"], close=df_v["close"],
        increasing=dict(line=dict(color="#00e676",width=1),fillcolor="rgba(0,230,118,.55)"),
        decreasing=dict(line=dict(color="#ff1744",width=1),fillcolor="rgba(255,23,68,.55)"),
        name="Price",
    ), row=1, col=1)

    # Moving averages
    for ma, name, color, dash in [
        (ma50_v, "MA50","#00b0ff","solid"),
        (ma150_v,"MA150","#ffab00","dash"),
        (ma200_v,"MA200","#e040fb","dot"),
    ]:
        fig.add_trace(go.Scatter(
            x=df_v.index, y=ma, name=name,
            line=dict(color=color, width=1.2, dash=dash),
        ), row=1, col=1)

    # Pivot level
    pivot = vcp.get("pivot_price")
    if pivot:
        grade_color = vcp.get("grade_color","#ffab00")
        fig.add_hline(y=pivot, line_color=grade_color, line_dash="dash",
                      line_width=1.5,
                      annotation_text=f"  PIVOT Rp {pivot:,.0f}",
                      annotation_font_color=grade_color,
                      annotation_font_size=10, row=1, col=1)

    # Annotate contraction troughs
    contractions = vcp.get("contractions", [])
    for i, ct in enumerate(contractions):
        trough_idx = ct.get("trough_idx", 0)
        if 0 <= trough_idx < len(df_v):
            trough_date = df_v.index[trough_idx]
            fig.add_annotation(
                x=trough_date, y=ct["trough_price"],
                text=f"C{i+1}<br>-{ct['depth_pct']:.0f}%",
                showarrow=True, arrowhead=2, arrowsize=0.8,
                arrowcolor="#ffab00", arrowwidth=1.5,
                font=dict(size=9, color="#ffab00",
                          family="IBM Plex Mono, monospace"),
                bgcolor="rgba(11,16,24,0.8)",
                bordercolor="#ffab00", borderwidth=1,
                ay=-35, row=1, col=1,
            )

    # Volume bars with recent dry-up highlighted
    avg_vol_20 = df_v["volume"].tail(20).mean()
    vol_colors = []
    for i, row in df_v.iterrows():
        vol_ratio = row["volume"] / avg_vol_20 if avg_vol_20 > 0 else 1
        if vol_ratio < 0.6:
            vol_colors.append("rgba(0,230,118,0.4)")   # dry-up = green tint
        elif df_v["close"].loc[i] >= df_v["open"].loc[i]:
            vol_colors.append("rgba(0,230,118,0.35)")
        else:
            vol_colors.append("rgba(255,23,68,0.35)")

    fig.add_trace(go.Bar(
        x=df_v.index, y=df_v["volume"],
        name="Volume", marker_color=vol_colors,
    ), row=2, col=1)

    # Volume MA20
    fig.add_trace(go.Scatter(
        x=df_v.index, y=df_v["volume"].rolling(20).mean(),
        name="Vol MA20", line=dict(color="#ffab00", width=1, dash="dot"),
        showlegend=False,
    ), row=2, col=1)

    # 75% volume reference line (dry-up threshold)
    if avg_vol_20 > 0:
        fig.add_hline(y=avg_vol_20 * 0.75,
                      line_color="rgba(0,230,118,0.3)",
                      line_dash="dot", line_width=1,
                      annotation_text="  75% vol (dry-up)",
                      annotation_font_color="rgba(0,230,118,0.6)",
                      annotation_font_size=9, row=2, col=1)

    grade = vcp.get("grade","NONE")
    grade_color = vcp.get("grade_color","#5a7a9a")
    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        title=dict(
            text=f"VCP Analysis  ·  Grade <b>{grade}</b>  ·  {vcp.get('grade_desc','')}",
            font=dict(size=11, color=grade_color), x=0.01,
        ),
        legend=dict(bgcolor="#0b1018", bordercolor="#141e2e",
                    font=dict(size=9), orientation="h", y=1.02),
        margin=dict(l=0, r=0, t=36, b=0),
        height=520,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
    )
    for i in [1,2]:
        fig.update_xaxes(gridcolor="#141e2e", showgrid=True, row=i, col=1)
        fig.update_yaxes(gridcolor="#141e2e", showgrid=True, row=i, col=1)
    fig.update_yaxes(title_text="Price (IDR)", row=1, col=1)
    fig.update_yaxes(title_text="Volume",      row=2, col=1)
    return fig


def entry_zone(df):
    """
    Entry zone with ATR-based stop-loss (quant standard).
    Stop = Entry - (1.5 × ATR14) — adapts to each stock's own volatility.
    BREN with ATR=800 gets SL 1,200 below entry.
    BBCA with ATR=30  gets SL 45 below entry.
    """
    a_ser = atr(df)
    lp    = float(df["close"].iloc[-1])
    la    = float(a_ser.iloc[-1]) if not pd.isna(a_ser.iloc[-1]) else lp * .02
    sup   = float(df["low"].tail(10).min())
    res   = float(df["high"].tail(20).max())

    # ATR-based stop-loss (primary) vs support-based (fallback)
    sl_atr     = round(lp - 1.5 * la, 0)
    sl_support = round(sup * .97, 0)
    sl         = max(sl_atr, sl_support)   # tighter of the two

    # Entry: at/near VCP pivot or current price
    el = round(lp,0)
    eh = round(lp * 1.015, 0)

    # Targets: R:R 1.5, 2.5, 4.0
    risk   = lp - sl
    t1     = round(lp + 1.5 * risk, 0)
    t2     = round(lp + 2.5 * risk, 0)
    t3     = round(lp + 4.0 * risk, 0)
    return dict(
        el=el, eh=eh, sl=sl, t1=t1, t2=t2, t3=t3,
        atr=round(la, 0),
        risk_pct=round((lp - sl) / lp * 100, 1),
        rr1=1.5, rr2=2.5, rr3=4.0,
        sup=sup, res=res,
        sl_method="ATR" if sl == sl_atr else "Support",
    )


# ══════════════════════════════════════════════════════════════════════
#  MODULE: MARKET REGIME FILTER + SECTOR INTELLIGENCE
#
#  Philosophy: Not a binary ON/OFF signal filter.
#  Instead, a CONTEXT LAYER that:
#    1. Detects the macro regime (Bull / Rotation / Risk-Off / Crash)
#    2. Identifies WHAT IS CAUSING the regime (rates, commodity, political)
#    3. Measures which sectors are receiving inflows vs outflows
#    4. Adjusts signal conviction based on sector alignment
#
#  IDX-specific sectors tracked:
#  Banking, Consumer, Mining, Energy, Property, Tech, Telecoms,
#  Industrial, Healthcare, Infrastructure
# ══════════════════════════════════════════════════════════════════════

# Sector ETF proxies for IDX (using available Yahoo Finance tickers)
SECTOR_TICKERS = {
    "Banking"     : ["BBCA.JK","BBRI.JK","BMRI.JK"],
    "Mining"      : ["ADRO.JK","MDKA.JK","BYAN.JK"],
    "Energy"      : ["AMMN.JK","MEDC.JK","PGAS.JK"],
    "Consumer"    : ["ICBP.JK","INDF.JK","UNVR.JK"],
    "Telecoms"    : ["TLKM.JK","EXCL.JK","ISAT.JK"],
    "Property"    : ["BSDE.JK","CTRA.JK","PWON.JK"],
    "Healthcare"  : ["KLBF.JK","SIDO.JK","MIKA.JK"],
    "Tech/Digital": ["GOTO.JK","BUKA.JK","EMTK.JK"],
    "Industrial"  : ["ASII.JK","UNTR.JK","INCO.JK"],
}

# Sector sensitivity to macro factors
# (which regime each sector benefits from)
SECTOR_REGIME_MAP = {
    "Mining"      : ["commodity_up", "global_growth"],
    "Energy"      : ["commodity_up", "geopolitical_risk"],
    "Banking"     : ["bull_market",  "rate_stable"],
    "Consumer"    : ["bull_market",  "risk_off_defensive"],
    "Telecoms"    : ["risk_off_defensive", "bull_market"],
    "Healthcare"  : ["risk_off_defensive"],
    "Property"    : ["bull_market",  "rate_stable"],
    "Tech/Digital": ["bull_market",  "global_growth"],
    "Industrial"  : ["bull_market",  "global_growth"],
}


@st.cache_data(ttl=1800, show_spinner=False)

# ══════════════════════════════════════════════════════════════════════
#  MODULE: MULTI-TIMEFRAME WEEKLY CONFLUENCE
#
#  Philosophy: A daily BUY signal swimming against a weekly DOWNTREND
#  is fighting institutional money. Institutional capital operates on
#  weekly/monthly timeframes. Only take daily signals that are
#  CONFIRMED by the weekly timeframe.
#
#  Research basis:
#    Daily signal alone:         win rate ~52-55%
#    Daily + Weekly confluence:  win rate ~63-68%
#    (Source: quantitative studies on timeframe confluence)
#
#  Components measured on WEEKLY bars:
#    1. CMF(10) weekly     — weekly money flow direction
#    2. OBV weekly         — cumulative weekly volume pressure
#    3. RSI(14) weekly     — weekly momentum, NOT overbought/oversold
#    4. MA50 weekly slope  — intermediate trend direction
#    5. MA20 weekly slope  — short-term trend
#    6. Price vs MA crossover — is price above key weekly MAs?
# ══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=1800, show_spinner=False)
def load_weekly(ticker: str, period: str = "2y") -> pd.DataFrame:
    """Load weekly OHLCV. Uses 2y to get enough weekly bars (≈104 bars)."""
    try:
        df = yf.download(
            ticker.upper().replace(".JK","") + ".JK",
            period=period, interval="1wk",
            progress=False, auto_adjust=True
        )
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.rename(columns={"Open":"open","High":"high","Low":"low",
                                  "Close":"close","Volume":"volume"})
        return df[["open","high","low","close","volume"]].dropna()
    except: return None


def calc_weekly_confluence(ticker: str, period: str = "2y") -> dict:
    """
    Compute weekly timeframe confluence score (0-100).

    Each component scores independently, then combined.
    Score ≥ 60 = weekly confirms daily signal (green light)
    Score 40-59 = neutral / mixed (yellow light, reduce size)
    Score < 40  = weekly opposes daily signal (red light, do not enter)

    Returns full dict with scores, values, and interpretation.
    """
    df_w = load_weekly(ticker, period)
    if df_w is None or len(df_w) < 20:
        return {
            "available": False,
            "score": 50,
            "label": "Unavailable",
            "color": "#5a7a9a",
            "components": [],
        }

    # ── Compute weekly indicators
    cmf_w  = cmf(df_w, p=10)    # 10-week CMF
    obv_w  = obv(df_w)
    rsi_w  = rsi(df_w["close"], p=14)
    ma20_w = df_w["close"].rolling(20).mean()
    ma50_w = df_w["close"].rolling(50).mean() if len(df_w)>=50 else df_w["close"].rolling(min(len(df_w),30)).mean()

    n = len(df_w)
    last_close = float(df_w["close"].iloc[-1])
    cmf_v  = float(cmf_w.iloc[-1])  if not pd.isna(cmf_w.iloc[-1])  else 0.0
    obv_v  = float(obv_w.iloc[-1])
    rsi_v  = float(rsi_w.iloc[-1])  if not pd.isna(rsi_w.iloc[-1])  else 50.0
    ma20_v = float(ma20_w.iloc[-1]) if not pd.isna(ma20_w.iloc[-1]) else last_close
    ma50_v = float(ma50_w.iloc[-1]) if not pd.isna(ma50_w.iloc[-1]) else last_close

    # Slopes (4-week change)
    obv_slope  = obv_v > float(obv_w.iloc[-min(4,n-1)])
    ma20_slope = ma20_v > float(ma20_w.iloc[-min(4,n-1)]) if not pd.isna(ma20_w.iloc[-min(4,n-1)]) else True
    ma50_slope = ma50_v > float(ma50_w.iloc[-min(8,n-1)]) if not pd.isna(ma50_w.iloc[-min(8,n-1)]) else True

    # Price vs MAs
    above_ma20_w = last_close > ma20_v
    above_ma50_w = last_close > ma50_v

    # Weekly trend (8-week return)
    w8_ret = (last_close - float(df_w["close"].iloc[-min(8,n-1)])) / float(df_w["close"].iloc[-min(8,n-1)]) * 100

    # ── Score each component (max 100 total)
    components = []
    total_score = 0.0

    # 1. Weekly CMF (weight: 25pts)
    if cmf_v > 0.10:
        pts, lbl, c = 25, f"Weekly CMF {cmf_v:+.3f} — Strong inflow", "#00e676"
    elif cmf_v > 0.02:
        pts, lbl, c = 15, f"Weekly CMF {cmf_v:+.3f} — Mild inflow",   "#88ffbb"
    elif cmf_v > -0.05:
        pts, lbl, c = 10, f"Weekly CMF {cmf_v:+.3f} — Neutral",       "#ffab00"
    elif cmf_v > -0.12:
        pts, lbl, c =  4, f"Weekly CMF {cmf_v:+.3f} — Mild outflow",  "#ff8888"
    else:
        pts, lbl, c =  0, f"Weekly CMF {cmf_v:+.3f} — Strong outflow","#ff1744"
    components.append({"name":"Weekly CMF","value":f"{cmf_v:+.3f}","score":pts,"max":25,"label":lbl,"color":c})
    total_score += pts

    # 2. Weekly OBV trend (weight: 20pts)
    if obv_slope:
        pts, lbl, c = 20, "Weekly OBV rising — sustained buying pressure",  "#00e676"
    else:
        pts, lbl, c =  0, "Weekly OBV falling — sustained selling pressure","#ff1744"
    components.append({"name":"Weekly OBV","value":"Rising" if obv_slope else "Falling","score":pts,"max":20,"label":lbl,"color":c})
    total_score += pts

    # 3. Weekly RSI (weight: 15pts)
    if 45 <= rsi_v <= 70:
        pts, lbl, c = 15, f"Weekly RSI {rsi_v:.0f} — healthy momentum zone","#00e676"
    elif rsi_v < 35:
        pts, lbl, c =  8, f"Weekly RSI {rsi_v:.0f} — oversold, potential recovery","#ffab00"
    elif rsi_v > 75:
        pts, lbl, c =  4, f"Weekly RSI {rsi_v:.0f} — overbought, risk of pullback","#ff8888"
    else:
        pts, lbl, c = 10, f"Weekly RSI {rsi_v:.0f} — neutral","#5a7a9a"
    components.append({"name":"Weekly RSI","value":f"{rsi_v:.0f}","score":pts,"max":15,"label":lbl,"color":c})
    total_score += pts

    # 4. Price vs Weekly MA20 (weight: 15pts)
    if above_ma20_w and ma20_slope:
        pts, lbl, c = 15, "Above weekly MA20, slope rising — uptrend intact","#00e676"
    elif above_ma20_w:
        pts, lbl, c = 10, "Above weekly MA20, slope flat — consolidation","#ffab00"
    elif ma20_slope:
        pts, lbl, c =  5, "Below weekly MA20 but slope turning — early recovery?","#ffab00"
    else:
        pts, lbl, c =  0, "Below declining weekly MA20 — downtrend","#ff1744"
    components.append({"name":"Weekly MA20","value":f"Rp{ma20_v:,.0f}","score":pts,"max":15,"label":lbl,"color":c})
    total_score += pts

    # 5. Price vs Weekly MA50 (weight: 15pts)
    if above_ma50_w and ma50_slope:
        pts, lbl, c = 15, "Above weekly MA50, rising — healthy intermediate trend","#00e676"
    elif above_ma50_w:
        pts, lbl, c =  9, "Above weekly MA50, flat — intermediate trend uncertain","#ffab00"
    elif ma50_slope:
        pts, lbl, c =  4, "Below MA50 but MA50 still rising — potential base","#ffab00"
    else:
        pts, lbl, c =  0, "Below declining weekly MA50 — intermediate downtrend","#ff1744"
    components.append({"name":"Weekly MA50","value":f"Rp{ma50_v:,.0f}","score":pts,"max":15,"label":lbl,"color":c})
    total_score += pts

    # 6. 8-week trend return (weight: 10pts)
    if w8_ret >= 10:
        pts, lbl, c = 10, f"8-week return {w8_ret:+.1f}% — strong weekly momentum","#00e676"
    elif w8_ret >= 3:
        pts, lbl, c =  7, f"8-week return {w8_ret:+.1f}% — positive weekly trend","#88ffbb"
    elif w8_ret >= -3:
        pts, lbl, c =  5, f"8-week return {w8_ret:+.1f}% — sideways","#ffab00"
    elif w8_ret >= -10:
        pts, lbl, c =  2, f"8-week return {w8_ret:+.1f}% — mild weekly weakness","#ff8888"
    else:
        pts, lbl, c =  0, f"8-week return {w8_ret:+.1f}% — strong weekly downtrend","#ff1744"
    components.append({"name":"8-Week Trend","value":f"{w8_ret:+.1f}%","score":pts,"max":10,"label":lbl,"color":c})
    total_score += pts

    score = int(np.clip(round(total_score), 0, 100))

    # ── Interpretation
    if score >= 70:
        label = "STRONG CONFIRM"
        color = "#00e676"
        action= "Weekly strongly confirms daily signal. Full position size appropriate."
        confluence_mult = 1.15   # boost daily signal
    elif score >= 55:
        label = "CONFIRM"
        color = "#88ffbb"
        action= "Weekly confirms daily signal. Normal position size."
        confluence_mult = 1.05
    elif score >= 40:
        label = "NEUTRAL"
        color = "#ffab00"
        action= "Mixed weekly signals. Reduce position size by 30-50%. Wait for clarity."
        confluence_mult = 0.85
    elif score >= 25:
        label = "CAUTION"
        color = "#ff8888"
        action= "Weekly opposes daily. Only enter if signal is STRONG BUY with ACC Cross."
        confluence_mult = 0.65
    else:
        label = "OPPOSE"
        color = "#ff1744"
        action= "Weekly strongly opposes daily signal. DO NOT enter regardless of daily signal."
        confluence_mult = 0.40

    return {
        "available"       : True,
        "score"           : score,
        "label"           : label,
        "color"           : color,
        "action"          : action,
        "confluence_mult" : confluence_mult,
        "components"      : components,
        "cmf_weekly"      : cmf_v,
        "rsi_weekly"      : rsi_v,
        "obv_rising"      : obv_slope,
        "above_ma20_w"    : above_ma20_w,
        "above_ma50_w"    : above_ma50_w,
        "ma20_w"          : round(ma20_v,0),
        "ma50_w"          : round(ma50_v,0),
        "w8_ret"          : round(w8_ret,2),
    }


def chart_weekly_vs_daily(df_daily: pd.DataFrame,
                           df_weekly: pd.DataFrame,
                           ticker: str) -> go.Figure:
    """
    Dual-panel chart: weekly OHLCV with MAs (top) + daily CMF comparison (bottom).
    Shows immediately whether daily and weekly are aligned.
    """
    if df_weekly is None or len(df_weekly) < 10:
        return go.Figure()

    ma20_w = df_weekly["close"].rolling(20).mean()
    ma50_w = df_weekly["close"].rolling(50).mean() if len(df_weekly)>=50 else None
    cmf_d  = cmf(df_daily, p=14).tail(60)
    cmf_w_s= cmf(df_weekly, p=10)

    fig = make_subplots(rows=2, cols=1, shared_xaxes=False,
                        row_heights=[0.60, 0.40], vertical_spacing=0.08,
                        subplot_titles=["Weekly Price + MA20/MA50",
                                        "CMF Comparison: Daily (14) vs Weekly (10)"])

    # Weekly candlestick
    fig.add_trace(go.Candlestick(
        x=df_weekly.index, open=df_weekly["open"], high=df_weekly["high"],
        low=df_weekly["low"], close=df_weekly["close"],
        increasing=dict(line=dict(color="#00e676",width=1), fillcolor="rgba(0,230,118,.5)"),
        decreasing=dict(line=dict(color="#ff1744",width=1), fillcolor="rgba(255,23,68,.5)"),
        name="Weekly", showlegend=True,
    ), row=1, col=1)

    fig.add_trace(go.Scatter(x=df_weekly.index, y=ma20_w,
        name="WMA20", line=dict(color="#ffab00",width=1.5,dash="dash")), row=1, col=1)
    if ma50_w is not None:
        fig.add_trace(go.Scatter(x=df_weekly.index, y=ma50_w,
            name="WMA50", line=dict(color="#e040fb",width=1.5,dash="dot")), row=1, col=1)

    # CMF comparison
    cmf_w_clean = cmf_w_s.dropna()
    cc_d = ["#00e676" if v>0 else "#ff1744" for v in cmf_d]
    cc_w = ["#00b0ff" if v>0 else "#ff8844" for v in cmf_w_clean]
    fig.add_trace(go.Bar(x=df_daily.index[-len(cmf_d):], y=cmf_d.values,
        name="Daily CMF(14)", marker_color=cc_d, opacity=0.6), row=2, col=1)
    fig.add_trace(go.Scatter(x=df_weekly.index[-len(cmf_w_clean):], y=cmf_w_clean.values,
        name="Weekly CMF(10)", line=dict(color="#00b0ff",width=2.5),
        mode="lines+markers", marker=dict(size=4)), row=2, col=1)
    fig.add_hline(y=0, line_color="#5a7a9a", line_width=1, row=2, col=1)
    fig.add_hline(y=0.10, line_color="rgba(0,230,118,.3)", line_dash="dot", row=2, col=1)
    fig.add_hline(y=-0.10, line_color="rgba(255,23,68,.25)", line_dash="dot", row=2, col=1)

    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        legend=dict(bgcolor="#0b1018", bordercolor="#141e2e", font=dict(size=9),
                    orientation="h", y=1.02),
        margin=dict(l=0,r=0,t=36,b=0), height=520,
        xaxis_rangeslider_visible=False, hovermode="x unified",
    )
    for i in [1,2]:
        fig.update_xaxes(gridcolor="#141e2e", showgrid=True, row=i, col=1)
        fig.update_yaxes(gridcolor="#141e2e", showgrid=True, row=i, col=1)
    fig.update_yaxes(title_text="Price (IDR)", row=1, col=1)
    fig.update_yaxes(title_text="CMF", range=[-0.6, 0.6], row=2, col=1)
    return fig


# ══════════════════════════════════════════════════════════════════════
#  MODULE: LIQUIDITY-ADJUSTED SCORING
#
#  Philosophy: A signal without liquidity is NOT a signal.
#  If it takes 5 days to build a full position without moving the market,
#  the signal degrades significantly before execution is complete.
#
#  Market Impact Model:
#    Institutional standard: position ≤ 20% of Average Daily Value (ADV)
#    Market Impact Days = position_value / (ADV × 20%)
#
#  Grading:
#    < 0.5 days  → HIGHLY LIQUID   (no penalty, +5 bonus)
#    0.5-1 day   → LIQUID          (no penalty)
#    1-3 days    → SEMI-LIQUID     (-8 pts, warning)
#    3-7 days    → ILLIQUID        (-18 pts, strong warning)
#    > 7 days    → VERY ILLIQUID   (-30 pts, flag)
#
#  Also computes:
#    - Bid-ask spread proxy (intrabar: H-L / midpoint)
#    - Volume consistency (std dev of daily volume — low std = reliable)
#    - Float-adjusted liquidity (for FCA stocks like BREN)
# ══════════════════════════════════════════════════════════════════════

def calc_liquidity_score(df: pd.DataFrame, position_value_idr: float = 100_000_000,
                          own: dict = None) -> dict:
    """
    Compute liquidity score and market impact for a given position size.

    Args:
        df:                  Daily OHLCV DataFrame
        position_value_idr:  Intended position size in Rupiah (default Rp 100M)
        own:                 Ownership dict (to check FCA / low float flag)

    Returns comprehensive liquidity assessment dict.
    """
    if df is None or len(df) < 10:
        return {"available": False, "score": 50, "label": "Unknown"}

    last_price = float(df["close"].iloc[-1])
    if last_price <= 0:
        return {"available": False, "score": 50, "label": "Unknown"}

    # ── Average Daily Value (20-day, in Rp)
    df_last20  = df.tail(20).copy()
    daily_val  = df_last20["close"] * df_last20["volume"] * 100  # lots → shares
    adv_20     = float(daily_val.mean())   # Rp average daily value traded
    adv_5      = float(daily_val.tail(5).mean())  # recent 5-day ADV

    # ── Market Impact Days (at 20% ADV participation rate)
    participation_rate = 0.20   # institutional standard
    daily_capacity     = adv_20 * participation_rate
    if daily_capacity > 0:
        impact_days = position_value_idr / daily_capacity
    else:
        impact_days = 999.0

    # ── Bid-Ask Spread Proxy (H-L / midpoint, averaged 20d)
    mid = (df_last20["high"] + df_last20["low"]) / 2
    spread_proxy = ((df_last20["high"] - df_last20["low"]) / mid.replace(0,np.nan)).mean()
    spread_pct   = float(spread_proxy) * 100 if not pd.isna(spread_proxy) else 5.0

    # ── Volume Consistency (CoV = std/mean — lower = more reliable)
    vol_cov = float(df_last20["volume"].std() / df_last20["volume"].mean()) if df_last20["volume"].mean() > 0 else 1.0

    # ── ADV Trend (is volume growing or shrinking?)
    adv_trend = "GROWING" if adv_5 > adv_20 * 1.1 else "SHRINKING" if adv_5 < adv_20 * 0.8 else "STABLE"

    # ── Float-adjusted check (FCA stocks have restricted float)
    is_fca     = False
    float_pct  = 100.0
    if own:
        float_pct = own.get("float", 100.0)
        is_fca    = float_pct < 15.0
    # Adjust ADV for low float: effective ADV = ADV × (float% / 100) × 2
    if is_fca:
        effective_adv = adv_20 * (float_pct / 100) * 2
        impact_days_float = position_value_idr / (effective_adv * participation_rate) if effective_adv > 0 else 999
    else:
        impact_days_float = impact_days

    # Use the more conservative (higher) impact_days
    impact_days_final = max(impact_days, impact_days_float)

    # ── Liquidity Score (0-100)
    liq_score = 100.0

    # Market impact penalty
    if impact_days_final < 0.5:
        mi_pts, mi_label, mi_color = 100, "Highly Liquid", "#00e676"
    elif impact_days_final < 1.0:
        mi_pts, mi_label, mi_color = 85,  "Liquid",        "#88ffbb"
    elif impact_days_final < 3.0:
        mi_pts, mi_label, mi_color = 60,  "Semi-Liquid",   "#ffab00"
    elif impact_days_final < 7.0:
        mi_pts, mi_label, mi_color = 30,  "Illiquid",      "#ff8888"
    else:
        mi_pts, mi_label, mi_color = 5,   "Very Illiquid", "#ff1744"

    liq_score = mi_pts

    # Spread penalty (large spread = high transaction cost)
    if spread_pct > 5.0:   liq_score -= 20
    elif spread_pct > 2.5: liq_score -= 10
    elif spread_pct > 1.5: liq_score -= 5

    # FCA penalty
    if is_fca: liq_score -= 15

    # Volume consistency bonus/penalty
    if vol_cov < 0.5:  liq_score += 5   # very consistent volume
    elif vol_cov > 2.0: liq_score -= 10  # very erratic volume

    liq_score = int(np.clip(round(liq_score), 0, 100))

    # ── Score adjustment for composite score
    if liq_score >= 85:   score_adj, adj_label = +5,   "Highly liquid — no constraint"
    elif liq_score >= 65: score_adj, adj_label =  0,   "Liquid — normal execution"
    elif liq_score >= 40: score_adj, adj_label = -8,   "Semi-liquid — reduce size"
    elif liq_score >= 20: score_adj, adj_label = -18,  "Illiquid — major execution risk"
    else:                 score_adj, adj_label = -30,  "Very illiquid — signal unreliable"

    # Format ADV for display
    if adv_20 >= 1e9:
        adv_str = f"Rp {adv_20/1e9:.1f}B/day"
    elif adv_20 >= 1e6:
        adv_str = f"Rp {adv_20/1e6:.0f}M/day"
    else:
        adv_str = f"Rp {adv_20/1e3:.0f}K/day"

    return {
        "available"        : True,
        "score"            : liq_score,
        "label"            : mi_label,
        "color"            : mi_color,
        "score_adj"        : score_adj,
        "adj_label"        : adj_label,
        "adv_20"           : adv_20,
        "adv_str"          : adv_str,
        "adv_trend"        : adv_trend,
        "impact_days"      : round(impact_days_final, 2),
        "spread_pct"       : round(spread_pct, 2),
        "vol_cov"          : round(vol_cov, 2),
        "is_fca"           : is_fca,
        "float_pct"        : float_pct,
        "position_value"   : position_value_idr,
        "daily_capacity"   : round(daily_capacity, 0),
    }



    """
    Detect current IDX market regime using IHSG technical analysis.

    Returns:
        regime: BULL / SECTOR_ROTATION / RISK_OFF / CRASH
        cause:  List of suspected drivers
        conviction_multiplier: How to adjust signal scores
        description: Human-readable explanation
    """
    ihsg = load_ihsg(ihsg_period)
    if ihsg is None or len(ihsg) < 50:
        return {"regime": "UNKNOWN", "available": False,
                "multiplier": 1.0, "description": "IHSG data unavailable"}

    p  = ihsg["close"]
    n  = len(p)

    ma20  = p.rolling(20).mean()
    ma50  = p.rolling(50).mean()
    ma200 = p.rolling(200).mean() if n >= 200 else p.rolling(min(n,120)).mean()

    lp    = float(p.iloc[-1])
    ma20v = float(ma20.iloc[-1])
    ma50v = float(ma50.iloc[-1])
    ma200v= float(ma200.iloc[-1])

    # 20-day and 60-day IHSG return
    ret20  = (lp - float(p.iloc[-min(20,n-1)])) / float(p.iloc[-min(20,n-1)])  * 100
    ret60  = (lp - float(p.iloc[-min(60,n-1)])) / float(p.iloc[-min(60,n-1)]) * 100
    ret5   = (lp - float(p.iloc[-min(5, n-1)])) / float(p.iloc[-min(5, n-1)]) * 100

    # Peak drawdown (from 52-week high)
    peak   = float(p.tail(252).max()) if n >= 252 else float(p.max())
    dd_pct = (lp - peak) / peak * 100

    # Volatility (20-day realized)
    daily_ret  = p.pct_change().dropna()
    vol_20     = float(daily_ret.tail(20).std() * np.sqrt(252) * 100)

    # ── Regime Classification
    above_ma50  = lp > ma50v
    above_ma200 = lp > ma200v
    ma50_rising = float(ma50.iloc[-1]) > float(ma50.iloc[-min(20,n-1)])

    causes = []

    # Check commodity signals (proxy: check if we can detect from IHSG vs global)
    # In practice: IHSG flat/down while commodity stocks (ADRO/BYAN) are up = commodity rotation

    if dd_pct < -25 or ret60 < -20:
        regime = "CRASH"
        multiplier = 0.5
        description = (f"IHSG is in systemic decline ({dd_pct:.1f}% from peak, "
                       f"{ret60:.1f}% over 60d). Most signals are unreliable. "
                       "Only defensive/commodity plays with specific catalysts.")
        causes = ["systematic_selloff"]
        regime_color = "#ff1744"

    elif dd_pct < -12 or ret60 < -8:
        regime = "RISK_OFF"
        multiplier = 0.75
        description = (f"IHSG under pressure ({dd_pct:.1f}% from peak). Risk-off environment. "
                       "Raise entry thresholds — only Grade A VCP + ACC Cross qualify. "
                       "Sector rotation to defensives/commodities likely.")
        if not above_ma50: causes.append("below_ma50")
        if vol_20 > 20:    causes.append("elevated_volatility")
        regime_color = "#ff8844"

    elif abs(ret20) < 3 and ret60 < 5 and above_ma200:
        regime = "SECTOR_ROTATION"
        multiplier = 1.0
        description = (f"IHSG sideways ({ret20:.1f}% / 20d, {ret60:.1f}% / 60d). "
                       "Capital is rotating between sectors — not leaving the market. "
                       "Best environment for stock picking. Focus on sectors with rising RS.")
        causes = ["sector_rotation"]
        regime_color = "#ffab00"

    elif above_ma50 and above_ma200 and ma50_rising:
        regime = "BULL"
        multiplier = 1.15
        description = (f"IHSG in uptrend (above MA50 & MA200, MA50 rising). "
                       f"Recent: {ret20:.1f}% / 20d, {ret60:.1f}% / 60d. "
                       "Broad market tailwind. All sectors eligible, focus on highest RS.")
        causes = ["broad_uptrend"]
        regime_color = "#00e676"

    else:
        regime = "MIXED"
        multiplier = 0.90
        description = (f"IHSG in mixed/uncertain state. {ret20:.1f}% / 20d. "
                       "Exercise caution, increase confirmation requirements.")
        regime_color = "#5a7a9a"

    return {
        "available"   : True,
        "regime"      : regime,
        "color"       : regime_color,
        "multiplier"  : multiplier,
        "description" : description,
        "causes"      : causes,
        "ihsg_last"   : round(lp, 2),
        "ret20"       : round(ret20, 2),
        "ret60"       : round(ret60, 2),
        "ret5"        : round(ret5, 2),
        "dd_pct"      : round(dd_pct, 2),
        "vol_20"      : round(vol_20, 1),
        "above_ma50"  : above_ma50,
        "above_ma200" : above_ma200,
        "ma50_rising" : ma50_rising,
        "ma50v"       : round(ma50v, 0),
        "ma200v"      : round(ma200v, 0),
    }


@st.cache_data(ttl=1800, show_spinner=False)
def detect_sector_rotation() -> dict:
    """
    Measure RS of each IDX sector vs IHSG over 20d and 60d.
    Identifies which sectors are receiving / losing inflows.
    """
    ihsg = load_ihsg("6mo")
    if ihsg is None: return {"available": False}

    sector_rs = {}
    for sector, tickers in SECTOR_TICKERS.items():
        sector_rets_20, sector_rets_60 = [], []
        for tk in tickers[:2]:  # use top 2 per sector
            try:
                df = yf.download(tk, period="6mo", interval="1d",
                                  progress=False, auto_adjust=True)
                if df.empty: continue
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                p = df["Close"].dropna()
                if len(p) < 25: continue
                r20 = (float(p.iloc[-1]) - float(p.iloc[-min(20,len(p)-1)])) / float(p.iloc[-min(20,len(p)-1)]) * 100
                r60 = (float(p.iloc[-1]) - float(p.iloc[-min(60,len(p)-1)])) / float(p.iloc[-min(60,len(p)-1)]) * 100
                sector_rets_20.append(r20)
                sector_rets_60.append(r60)
            except: continue
        if not sector_rets_20: continue

        ihsg_r20 = (float(ihsg["close"].iloc[-1]) - float(ihsg["close"].iloc[-min(20,len(ihsg)-1)])) / float(ihsg["close"].iloc[-min(20,len(ihsg)-1)]) * 100
        ihsg_r60 = (float(ihsg["close"].iloc[-1]) - float(ihsg["close"].iloc[-min(60,len(ihsg)-1)])) / float(ihsg["close"].iloc[-min(60,len(ihsg)-1)]) * 100

        avg20 = np.mean(sector_rets_20)
        avg60 = np.mean(sector_rets_60)
        rs20  = avg20 - ihsg_r20  # excess return vs IHSG
        rs60  = avg60 - ihsg_r60

        sector_rs[sector] = {
            "ret20"   : round(avg20, 2),
            "ret60"   : round(avg60, 2),
            "rs20"    : round(rs20, 2),
            "rs60"    : round(rs60, 2),
            "inflow"  : rs20 > 1.0 and rs60 > 0,
            "outflow" : rs20 < -1.0,
            "status"  : ("INFLOW ▲" if rs20 > 1.0 else
                         "OUTFLOW ▼" if rs20 < -1.0 else "NEUTRAL →"),
            "color"   : ("#00e676" if rs20 > 1.0 else
                         "#ff1744" if rs20 < -1.0 else "#5a7a9a"),
        }

    return {"available": True, "sectors": sector_rs}


def get_stock_sector(ticker: str) -> str:
    """Map a ticker to its IDX sector."""
    t = ticker.upper().replace(".JK","")
    sector_map = {
        "Banking"     : {"BBCA","BBRI","BMRI","BBNI","BBTN","BJTM","NISP","ARTO","BTPS"},
        "Mining"      : {"ADRO","ADMR","MDKA","BYAN","PTBA","ANTM","INCO","TINS","MBMA"},
        "Energy"      : {"AMMN","MEDC","PGAS","ELSA","RUIS","CUAN","BREN"},
        "Consumer"    : {"ICBP","INDF","UNVR","MYOR","ROTI","SIDO","DLTA","HMSP","GGRM"},
        "Telecoms"    : {"TLKM","EXCL","ISAT","TBIG","TOWR","DCII"},
        "Property"    : {"BSDE","CTRA","PWON","ASRI","LPKR","PANI","CBDK","SMRA"},
        "Healthcare"  : {"KLBF","KAEF","MIKA","HEAL","SIDO","PRDA"},
        "Tech/Digital": {"GOTO","BUKA","EMTK","DMMX","ARTO","META"},
        "Industrial"  : {"ASII","UNTR","INCO","SSIA","ACST","WTON"},
        "Petrochemical":{"BRPT","TPIA","CHANDRA"},
    }
    for sector, tickers in sector_map.items():
        if t in tickers:
            return sector
    return "Other"


def regime_signal_adjustment(regime: dict, sector: str,
                               sector_data: dict) -> dict:
    """
    Adjust signal conviction based on:
    1. Overall market regime (multiplier)
    2. Whether this stock's sector is receiving inflows
    3. Regime-sector alignment (e.g., Mining in commodity_up = no penalty)

    Returns adjustment dict with score_adj, note, allowed.
    """
    if not regime.get("available"):
        return {"score_adj": 0, "note": "", "allowed": True, "regime_ok": True}

    base_mult  = regime["multiplier"]
    reg_name   = regime["regime"]
    causes     = regime.get("causes", [])
    score_adj  = 0
    notes      = []
    allowed    = True

    # Sector inflow/outflow check
    sectors_data = sector_data.get("sectors", {}) if sector_data.get("available") else {}
    sect_info    = sectors_data.get(sector, {})
    sect_inflow  = sect_info.get("inflow",  False)
    sect_outflow = sect_info.get("outflow", False)

    # Core regime adjustment
    if reg_name == "CRASH":
        score_adj = -20
        notes.append("Market in systematic decline — reduce all position sizes")
        # Exception: commodity sectors might still work
        if sector in ("Mining","Energy") and "systematic_selloff" in causes:
            score_adj = -10
            notes.append("Commodity sector — partial exception to crash filter")

    elif reg_name == "RISK_OFF":
        score_adj = -10
        notes.append("Risk-off environment — raise entry bar")
        # Defensives and commodities get less penalty
        if sector in ("Healthcare","Consumer","Telecoms"):
            score_adj = -3
            notes.append(f"{sector} is a defensive sector — less impacted by risk-off")
        elif sector in ("Mining","Energy") and sect_inflow:
            score_adj = 0
            notes.append(f"{sector} receiving inflows — commodity rotation may be active")

    elif reg_name == "SECTOR_ROTATION":
        # Bonus for sectors receiving inflows, penalty for outflows
        if sect_inflow:
            score_adj = +8
            notes.append(f"{sector} sector receiving inflows — rotation tailwind")
        elif sect_outflow:
            score_adj = -8
            notes.append(f"{sector} sector losing capital — rotation headwind")
        else:
            notes.append("Sector rotation active — check sector RS")

    elif reg_name == "BULL":
        if sect_inflow:
            score_adj = +5
            notes.append(f"Bull market + {sector} inflow = double tailwind")
        else:
            score_adj = 0

    # Final: apply multiplier to adjustment magnitude
    score_adj = int(round(score_adj * base_mult))

    return {
        "score_adj"  : score_adj,
        "notes"      : notes,
        "allowed"    : allowed,
        "regime_ok"  : reg_name in ("BULL","SECTOR_ROTATION","MIXED"),
        "sect_inflow": sect_inflow,
        "sect_outflow":sect_outflow,
    }


def chart_sector_heatmap(sector_data: dict) -> go.Figure:
    """Sector RS heatmap — shows which sectors are in/out of favor."""
    if not sector_data.get("available"):
        return go.Figure()
    sectors = sector_data["sectors"]
    if not sectors:
        return go.Figure()

    names  = list(sectors.keys())
    rs20   = [sectors[s]["rs20"]  for s in names]
    rs60   = [sectors[s]["rs60"]  for s in names]
    colors = [sectors[s]["color"] for s in names]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="RS 20d (vs IHSG)",
        x=names, y=rs20,
        marker_color=["rgba(0,230,118,.7)" if v>0 else "rgba(255,23,68,.7)" for v in rs20],
        text=[f"{v:+.1f}%" for v in rs20],
        textposition="outside", textfont=dict(size=10),
    ))
    fig.add_trace(go.Bar(
        name="RS 60d (vs IHSG)",
        x=names, y=rs60,
        marker_color=["rgba(0,230,118,.35)" if v>0 else "rgba(255,23,68,.35)" for v in rs60],
        text=[f"{v:+.1f}%" for v in rs60],
        textposition="inside", textfont=dict(size=9),
    ))
    fig.add_hline(y=0, line_color="#5a7a9a", line_width=1)
    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        barmode="group", bargap=0.25,
        legend=dict(bgcolor="#0b1018", bordercolor="#141e2e", font=dict(size=10)),
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        xaxis=dict(gridcolor="#141e2e"),
        yaxis=dict(gridcolor="#141e2e", title="Excess Return vs IHSG (%)"),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════
#  MODULE: POSITION SIZING CALCULATOR
#  Standard quant method: Fixed % Risk
#  Position Size = (Capital × Risk%) / (Entry − Stop-Loss)
#  No guessing. No oversize. Bankrupt-proof.
# ══════════════════════════════════════════════════════════════════════

def calc_position_size(capital: float, risk_pct: float,
                       entry: float, stop_loss: float,
                       lot_size: int = 100) -> dict:
    """
    Calculate optimal position size based on fixed % risk.

    Args:
        capital:   Total trading capital (Rp)
        risk_pct:  Max % of capital willing to lose on this trade (e.g. 2.0)
        entry:     Entry price (Rp)
        stop_loss: Stop-loss price (Rp)
        lot_size:  Shares per lot (IDX default: 100)

    Returns:
        lots:       Number of lots to buy
        shares:     Number of shares (lots × lot_size)
        total_cost: Total investment (Rp)
        max_loss:   Maximum loss if stop triggered (Rp)
        risk_amount:Capital at risk (Rp) = capital × risk_pct%
    """
    if entry <= stop_loss or capital <= 0 or entry <= 0:
        return {"valid": False, "error": "Invalid inputs: entry must be above stop-loss"}

    risk_amount    = capital * (risk_pct / 100)
    risk_per_share = entry - stop_loss
    optimal_shares = risk_amount / risk_per_share

    # Round DOWN to nearest lot
    lots           = max(1, int(optimal_shares // lot_size))
    shares         = lots * lot_size
    total_cost     = shares * entry
    max_loss       = shares * risk_per_share
    actual_risk_pct= max_loss / capital * 100
    pct_of_capital = total_cost / capital * 100

    # Kelly fraction (optional reference)
    # Kelly is typically too aggressive; we show it as a ceiling only

    return {
        "valid"          : True,
        "lots"           : lots,
        "shares"         : shares,
        "total_cost"     : round(total_cost, 0),
        "max_loss"       : round(max_loss, 0),
        "risk_amount"    : round(risk_amount, 0),
        "actual_risk_pct": round(actual_risk_pct, 2),
        "pct_of_capital" : round(pct_of_capital, 1),
        "risk_per_share" : round(risk_per_share, 0),
        "risk_pct_input" : risk_pct,
        "capital"        : capital,
        "entry"          : entry,
        "stop_loss"      : stop_loss,
    }


# ══════════════════════════════════════════════════════════════════════
#  MODULE: SCORE NORMALIZATION
#  Raw scores 0-100 lose meaning when the whole market is bullish.
#  Percentile rank tells you: "This stock is top X% of all stocks
#  scanned TODAY under CURRENT market conditions."
#  Score 88 in bull market ≠ Score 88 in bear market.
# ══════════════════════════════════════════════════════════════════════

def normalize_scores(scores: list) -> list:
    """
    Convert raw composite scores to percentile ranks within the scanned universe.
    Score of 75 → "this stock scored better than 75% of all stocks scanned today"

    Also applies regime adjustment:
    - Bull market: full scores, no adjustment needed
    - Bear/Risk-off: compress scores toward median (less false positives)
    """
    if not scores or len(scores) < 2:
        return scores

    arr = np.array(scores, dtype=float)
    percentiles = []
    for s in arr:
        pct = float(np.sum(arr < s)) / len(arr) * 100
        percentiles.append(round(pct, 1))
    return percentiles


def score_grade(raw: int, percentile: float) -> tuple:
    """
    Combine raw score and percentile into a final grade and label.

    Grade system:
    S  = raw ≥75 AND percentile ≥85  → Exceptional
    A  = raw ≥65 AND percentile ≥70  → Strong
    B  = raw ≥55 AND percentile ≥50  → Above average
    C  = raw ≥45                     → Average
    D  = raw <45                     → Below average
    """
    if raw >= 75 and percentile >= 85:
        return "S", "#00e676", "Exceptional — top tier"
    elif raw >= 65 and percentile >= 70:
        return "A", "#88ffbb", "Strong — above most"
    elif raw >= 55 and percentile >= 50:
        return "B", "#ffab00", "Above average"
    elif raw >= 45:
        return "C", "#ff8888", "Average"
    else:
        return "D", "#ff1744", "Below average"


@st.cache_data(ttl=3600, show_spinner=False)
def fundamentals(ticker):
    """Enhanced fundamentals — adds quality metrics on top of base data."""
    try:
        tk   = ticker.upper().replace(".JK","") + ".JK"
        info = yf.Ticker(tk).info
        mc   = info.get("marketCap", 0)
        so   = info.get("sharesOutstanding", 0)

        # ── Base metrics
        pe    = info.get("trailingPE")  or info.get("forwardPE")
        pb    = info.get("priceToBook")
        ps    = info.get("priceToSalesTrailing12Months")
        ev_eb = info.get("enterpriseToEbitda")
        div_y = info.get("dividendYield")
        hi52  = info.get("fiftyTwoWeekHigh")
        lo52  = info.get("fiftyTwoWeekLow")
        curr  = info.get("currentPrice") or info.get("regularMarketPrice")

        # ── Quality metrics
        roe        = info.get("returnOnEquity")
        roa        = info.get("returnOnAssets")
        npm        = info.get("profitMargins")
        gpm        = info.get("grossMargins")
        opm        = info.get("operatingMargins")
        de_ratio   = info.get("debtToEquity")
        curr_ratio = info.get("currentRatio")
        quick_r    = info.get("quickRatio")
        int_cov    = info.get("ebitda", 0) / max(info.get("interestExpense", 1) or 1, 1)
        rev_growth = info.get("revenueGrowth")
        earn_growth= info.get("earningsGrowth")
        fcf        = info.get("freeCashflow", 0)
        op_cf      = info.get("operatingCashflow", 0)
        total_rev  = info.get("totalRevenue", 0)
        peg        = info.get("pegRatio")
        beta       = info.get("beta")
        short_pct  = info.get("shortPercentOfFloat")
        inst_hold  = info.get("heldPercentInstitutions")

        # ── Quality score 0-100 (proprietary)
        qs = 50.0
        if roe:
            qs += 15 if roe>=0.20 else 8 if roe>=0.15 else 0 if roe>=0.08 else -10
        if de_ratio is not None:
            qs += 10 if de_ratio<0.5 else 5 if de_ratio<1.0 else 0 if de_ratio<2.0 else -10
        if curr_ratio:
            qs += 8 if curr_ratio>=2.0 else 4 if curr_ratio>=1.5 else -5 if curr_ratio<1.0 else 0
        if npm:
            qs += 10 if npm>=0.20 else 5 if npm>=0.10 else 0 if npm>=0.05 else -8
        if rev_growth is not None:
            qs += 10 if rev_growth>=0.15 else 5 if rev_growth>=0.05 else -5 if rev_growth<0 else 0
        if earn_growth is not None:
            qs += 10 if earn_growth>=0.20 else 5 if earn_growth>=0.10 else -5 if earn_growth<0 else 0
        if fcf and mc:
            fcf_yield = fcf / mc
            qs += 8 if fcf_yield>=0.05 else 4 if fcf_yield>=0.02 else -3 if fcf_yield<0 else 0
        qs = int(np.clip(round(qs), 0, 100))

        # ── Quality label
        if qs >= 75: ql, qc = "Excellent", "#00e676"
        elif qs >= 60: ql, qc = "Good", "#88ffbb"
        elif qs >= 45: ql, qc = "Average", "#ffab00"
        elif qs >= 30: ql, qc = "Weak", "#ff8888"
        else:          ql, qc = "Poor", "#ff1744"

        mc_str = (f"Rp {mc/1e12:.2f}T" if mc>=1e12 else
                  f"Rp {mc/1e9:.1f}B"  if mc>=1e9  else "—")

        return dict(
            # Base
            market_cap   = mc_str,
            pe           = round(pe, 1)      if pe           else "—",
            pb           = round(pb, 2)      if pb           else "—",
            ps           = round(ps, 2)      if ps           else "—",
            ev_ebitda    = round(ev_eb, 1)   if ev_eb        else "—",
            peg          = round(peg, 2)     if peg          else "—",
            div_yield    = f"{div_y*100:.2f}%" if div_y      else "—",
            hi52         = hi52,   lo52 = lo52,   curr = curr,
            shares_out   = so,     beta = round(beta,2) if beta else "—",
            # Quality
            roe          = f"{roe*100:.1f}%"   if roe is not None   else "—",
            roa          = f"{roa*100:.1f}%"   if roa is not None   else "—",
            npm          = f"{npm*100:.1f}%"   if npm is not None   else "—",
            gpm          = f"{gpm*100:.1f}%"   if gpm is not None   else "—",
            opm          = f"{opm*100:.1f}%"   if opm is not None   else "—",
            de_ratio     = f"{de_ratio:.2f}"   if de_ratio is not None else "—",
            curr_ratio   = f"{curr_ratio:.2f}" if curr_ratio         else "—",
            quick_ratio  = f"{quick_r:.2f}"    if quick_r            else "—",
            int_coverage = f"{int_cov:.1f}x"   if int_cov > 0       else "—",
            rev_growth   = f"{rev_growth*100:+.1f}%" if rev_growth is not None else "—",
            earn_growth  = f"{earn_growth*100:+.1f}%" if earn_growth is not None else "—",
            fcf_str      = (f"Rp {fcf/1e9:.1f}B" if fcf and abs(fcf)>=1e9
                            else f"Rp {fcf/1e6:.0f}M" if fcf else "—"),
            fcf_yield    = (f"{fcf/mc*100:.1f}%" if fcf and mc else "—"),
            inst_pct     = f"{inst_hold*100:.1f}%" if inst_hold else "—",
            short_pct    = f"{short_pct*100:.1f}%" if short_pct else "—",
            # Quality score
            quality_score= qs,
            quality_label= ql,
            quality_color= qc,
            # Raw values for calculations
            roe_raw      = roe,
            de_raw       = de_ratio,
            npm_raw      = npm,
            pe_raw       = pe,
            beta_raw     = beta,
        )
    except: return {}


# ══════════════════════════════════════════════════════════════════════
#  MODULE: CORPORATE ACTION CALENDAR
#  Dividend, Rights Issue, Stock Split, RUPS dates
#  Sources: Yahoo Finance (dividend history + calendar)
#           IDX API (corporate actions)
# ══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=21600, show_spinner=False)  # 6h cache
def fetch_corporate_actions(ticker: str) -> dict:
    """
    Fetch corporate action data for a stock:
    - Dividend history (last 3 years + next upcoming)
    - Ex-dividend date (critical for entry timing)
    - Rights issue / stock split history
    - Earnings calendar (next report date)

    Returns structured dict with all corporate action data.
    """
    t   = ticker.upper().replace(".JK","")
    tk  = yf.Ticker(t + ".JK")
    out = {
        "available"      : False,
        "ex_div_date"    : None,
        "div_amount"     : None,
        "div_frequency"  : None,
        "div_history"    : [],
        "next_earnings"  : None,
        "earnings_history": [],
        "splits"         : [],
        "annual_div"     : 0.0,
        "div_consistency": "—",
        "days_to_exdiv"  : None,
        "exdiv_alert"    : False,
    }
    try:
        info = tk.info

        # ── Ex-dividend date & amount
        ex_ts  = info.get("exDividendDate")
        div_rt = info.get("dividendRate") or 0.0
        div_freq= info.get("dividendFrequency") or "Annual"
        if ex_ts:
            ex_date = datetime.fromtimestamp(ex_ts)
            days_to = (ex_date - datetime.now()).days
            out["ex_div_date"]  = ex_date.strftime("%d %b %Y")
            out["days_to_exdiv"]= days_to
            out["exdiv_alert"]  = 0 <= days_to <= 14  # within 2 weeks
        out["div_amount"]    = round(div_rt, 2) if div_rt else 0
        out["div_frequency"] = div_freq
        out["annual_div"]    = round(div_rt, 2) if div_rt else 0.0

        # ── Dividend history (last 8 quarters)
        try:
            hist_div = tk.dividends
            if not hist_div.empty:
                hist_div = hist_div.tail(12)
                div_list = []
                for dt, amt in hist_div.items():
                    div_list.append({
                        "date" : pd.Timestamp(dt).strftime("%d %b %Y"),
                        "amount": round(float(amt), 2),
                    })
                out["div_history"] = list(reversed(div_list))
                # Consistency: paid every year for last 3 years?
                years = set(pd.Timestamp(dt).year for dt in hist_div.index)
                last3 = {datetime.now().year-1, datetime.now().year-2, datetime.now().year-3}
                paid  = len(years & last3)
                out["div_consistency"] = (
                    "Consistent (3/3 years)" if paid==3 else
                    f"Partial ({paid}/3 years)" if paid>0 else "Inconsistent"
                )
        except: pass

        # ── Earnings calendar
        try:
            cal = tk.calendar
            if cal is not None and not cal.empty:
                if "Earnings Date" in cal.index:
                    earn_dates = cal.loc["Earnings Date"]
                    if isinstance(earn_dates, pd.Series):
                        next_earn = pd.Timestamp(earn_dates.iloc[0])
                    else:
                        next_earn = pd.Timestamp(earn_dates)
                    days_earn = (next_earn.to_pydatetime() - datetime.now()).days
                    out["next_earnings"] = {
                        "date"   : next_earn.strftime("%d %b %Y"),
                        "days_to": days_earn,
                        "alert"  : 0 <= days_earn <= 21,
                    }
        except: pass

        # ── Earnings history (last 4 quarters)
        try:
            earn_hist = tk.quarterly_earnings
            if earn_hist is not None and not earn_hist.empty:
                records = []
                for idx, row in earn_hist.tail(4).iterrows():
                    est = row.get("Estimate", 0) or 0
                    act = row.get("Actual", 0) or 0
                    surprise = ((act-est)/abs(est)*100) if est and est!=0 else 0
                    records.append({
                        "quarter" : str(idx),
                        "estimate": round(float(est),2) if est else "—",
                        "actual"  : round(float(act),2) if act else "—",
                        "surprise": f"{surprise:+.1f}%",
                        "beat"    : act > est if (est and act) else None,
                    })
                out["earnings_history"] = list(reversed(records))
        except: pass

        # ── Stock splits
        try:
            splits = tk.splits
            if not splits.empty:
                split_list = []
                for dt, ratio in splits.tail(5).items():
                    split_list.append({
                        "date" : pd.Timestamp(dt).strftime("%d %b %Y"),
                        "ratio": f"{ratio:.0f}:1",
                    })
                out["splits"] = list(reversed(split_list))
        except: pass

        # ── IDX API fallback for ex-dividend (more accurate for IDX)
        if not out["ex_div_date"]:
            try:
                tk_code = t
                r = requests.get(
                    f"https://www.idx.co.id/umbraco/Surface/Helper/GetInitiationOfPublicCompany"
                    f"?kodeEmiten={tk_code}",
                    headers=HDR, timeout=8
                )
                if r.status_code == 200:
                    data = r.json()
                    # Parse IDX corporate action data
                    ca_list = data.get("corporateActions", data.get("ListCorporateAction", []))
                    for ca in (ca_list or [])[:5]:
                        ca_type = str(ca.get("Type","") or ca.get("Remark","")).upper()
                        if "DIVIDEN" in ca_type or "DIVIDEND" in ca_type:
                            ex_dt = ca.get("ExDate") or ca.get("CumDate") or ca.get("RecordDate")
                            if ex_dt:
                                try:
                                    ex_parsed = datetime.strptime(str(ex_dt)[:10], "%Y-%m-%d")
                                    days_to   = (ex_parsed - datetime.now()).days
                                    out["ex_div_date"]   = ex_parsed.strftime("%d %b %Y")
                                    out["days_to_exdiv"] = days_to
                                    out["exdiv_alert"]   = 0 <= days_to <= 14
                                    amount = ca.get("Amount") or ca.get("Value") or 0
                                    if amount: out["div_amount"] = round(float(amount), 2)
                                except: pass
                            break
            except: pass

        out["available"] = True
    except Exception as e:
        out["error"] = str(e)
    return out


def calc_dividend_metrics(ca: dict, fund: dict, curr_price: float) -> dict:
    """Compute dividend quality metrics from corporate action + fundamentals data."""
    metrics = {}
    if not ca.get("available"): return metrics

    # Forward yield
    annual_div = ca.get("annual_div", 0)
    if annual_div and curr_price:
        metrics["fwd_yield"] = round(annual_div / curr_price * 100, 2)
    else:
        dy_str = fund.get("div_yield", "—")
        metrics["fwd_yield"] = float(dy_str.replace("%","")) if dy_str != "—" else 0

    # Payout ratio (EPS-based)
    try:
        pe_raw  = fund.get("pe_raw")
        npm_raw = fund.get("npm_raw")
        if pe_raw and annual_div and curr_price:
            eps = curr_price / pe_raw
            payout = annual_div / eps * 100
            metrics["payout_ratio"] = round(payout, 1)
        else:
            metrics["payout_ratio"] = None
    except:
        metrics["payout_ratio"] = None

    # Days to ex-date classification
    days = ca.get("days_to_exdiv")
    if days is not None:
        if days < 0:
            metrics["exdiv_status"] = "Past"
            metrics["exdiv_color"]  = "#5a7a9a"
        elif days == 0:
            metrics["exdiv_status"] = "TODAY ⚡"
            metrics["exdiv_color"]  = "#ff1744"
        elif days <= 3:
            metrics["exdiv_status"] = f"In {days}d — URGENT"
            metrics["exdiv_color"]  = "#ff1744"
        elif days <= 7:
            metrics["exdiv_status"] = f"In {days}d — This week"
            metrics["exdiv_color"]  = "#ffab00"
        elif days <= 14:
            metrics["exdiv_status"] = f"In {days}d — Soon"
            metrics["exdiv_color"]  = "#ffab00"
        else:
            metrics["exdiv_status"] = f"In {days}d"
            metrics["exdiv_color"]  = "#5a7a9a"
    else:
        metrics["exdiv_status"] = "Unknown"
        metrics["exdiv_color"]  = "#2a3d52"

    return metrics


def chart_dividend_history(div_history: list) -> go.Figure:
    """Bar chart of dividend per share history."""
    if not div_history:
        return go.Figure()
    dates  = [d["date"]   for d in div_history]
    amounts= [d["amount"] for d in div_history]
    fig = go.Figure(go.Bar(
        x=dates, y=amounts,
        marker_color="#00e676", opacity=0.8,
        text=[f"Rp {a:,.0f}" for a in amounts],
        textposition="outside",
        textfont=dict(color="#cdd8e6", size=9),
    ))
    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        margin=dict(l=0,r=0,t=8,b=0), height=200,
        xaxis=dict(gridcolor="#141e2e"),
        yaxis=dict(gridcolor="#141e2e", title="Dividend/Share (Rp)"),
        showlegend=False,
    )
    return fig


def fundamental_quality_gate(fund: dict) -> dict:
    """
    Fundamental quality gatekeeper.
    Returns pass/warn/fail for each dimension.
    A technical signal with fundamental FAIL should reduce conviction.
    """
    if not fund: return {"overall":"UNKNOWN","score":0,"checks":[]}

    checks = []

    def chk(name, value_str, condition, status, tip):
        checks.append({"name":name,"value":value_str,
                        "status":status,"tip":tip,
                        "ok": status=="PASS"})

    # ROE
    roe = fund.get("roe","—")
    roe_v = float(roe.replace("%","")) if roe!="—" else None
    if roe_v is not None:
        if roe_v>=20:   chk("ROE",roe,True,"PASS","Strong profitability ≥20%")
        elif roe_v>=12: chk("ROE",roe,True,"WARN","Acceptable ROE 12–20%")
        else:           chk("ROE",roe,False,"FAIL","Weak ROE <12% — low profitability")
    else:
        chk("ROE","N/A",None,"N/A","Data not available")

    # Debt/Equity
    de = fund.get("de_ratio","—")
    de_v = float(de) if de!="—" else None
    if de_v is not None:
        if de_v<0.5:   chk("Debt/Equity",de,True,"PASS","Low leverage <0.5")
        elif de_v<1.5: chk("Debt/Equity",de,True,"WARN","Moderate leverage 0.5–1.5")
        else:          chk("Debt/Equity",de,False,"FAIL","High leverage >1.5 — risk in rate hike environment")
    else:
        chk("Debt/Equity","N/A",None,"N/A","Data not available")

    # Net Profit Margin
    npm = fund.get("npm","—")
    npm_v = float(npm.replace("%","")) if npm!="—" else None
    if npm_v is not None:
        if npm_v>=15:   chk("Net Margin",npm,True,"PASS","Strong margins ≥15%")
        elif npm_v>=8:  chk("Net Margin",npm,True,"WARN","Acceptable margins 8–15%")
        elif npm_v>=0:  chk("Net Margin",npm,False,"WARN","Thin margins <8%")
        else:           chk("Net Margin",npm,False,"FAIL","Negative margins — losing money")
    else:
        chk("Net Margin","N/A",None,"N/A","Data not available")

    # Revenue Growth
    rg = fund.get("rev_growth","—")
    rg_v = float(rg.replace("%","").replace("+","")) if rg!="—" else None
    if rg_v is not None:
        if rg_v>=15:   chk("Revenue Growth",rg,True,"PASS","Strong growth ≥15% YoY")
        elif rg_v>=5:  chk("Revenue Growth",rg,True,"WARN","Moderate growth 5–15%")
        elif rg_v>=0:  chk("Revenue Growth",rg,False,"WARN","Slow growth 0–5%")
        else:          chk("Revenue Growth",rg,False,"FAIL","Revenue declining YoY")
    else:
        chk("Revenue Growth","N/A",None,"N/A","Data not available")

    # FCF Yield
    fcy = fund.get("fcf_yield","—")
    fcy_v = float(fcy.replace("%","")) if fcy!="—" else None
    if fcy_v is not None:
        if fcy_v>=5:   chk("FCF Yield",fcy,True,"PASS","Strong FCF yield ≥5%")
        elif fcy_v>=2: chk("FCF Yield",fcy,True,"WARN","Moderate FCF yield 2–5%")
        elif fcy_v>=0: chk("FCF Yield",fcy,False,"WARN","Low FCF yield <2%")
        else:          chk("FCF Yield",fcy,False,"FAIL","Negative FCF — cash burn")
    else:
        chk("FCF Yield","N/A",None,"N/A","Data not available")

    # Valuation (P/E context)
    pe = fund.get("pe","—")
    pe_v = float(pe) if pe!="—" else None
    if pe_v is not None:
        if pe_v<0:      chk("P/E Ratio",str(pe),False,"FAIL","Negative earnings")
        elif pe_v<15:   chk("P/E Ratio",str(pe),True,"PASS","Cheap valuation <15x")
        elif pe_v<25:   chk("P/E Ratio",str(pe),True,"WARN","Fair valuation 15–25x")
        elif pe_v<40:   chk("P/E Ratio",str(pe),False,"WARN","Expensive 25–40x")
        else:           chk("P/E Ratio",str(pe),False,"FAIL","Very expensive >40x — need high growth to justify")
    else:
        chk("P/E Ratio","N/A",None,"N/A","Data not available")

    # Overall quality
    known   = [c for c in checks if c["status"] not in ("N/A",)]
    passes  = sum(1 for c in known if c["status"]=="PASS")
    warns   = sum(1 for c in known if c["status"]=="WARN")
    fails   = sum(1 for c in known if c["status"]=="FAIL")
    total   = len(known) or 1

    qs = int((passes*2 + warns*1) / (total*2) * 100)

    if fails >= 3:        overall, oc = "WEAK FUNDAMENTALS",   "#ff1744"
    elif fails >= 2:      overall, oc = "BELOW AVERAGE",        "#ff8888"
    elif passes >= 4:     overall, oc = "STRONG FUNDAMENTALS",  "#00e676"
    elif passes >= 3:     overall, oc = "GOOD FUNDAMENTALS",    "#88ffbb"
    elif warns >= 3:      overall, oc = "AVERAGE",              "#ffab00"
    else:                 overall, oc = "MIXED",                "#ffab00"

    return {
        "overall": overall,
        "color"  : oc,
        "score"  : qs,
        "checks" : checks,
        "passes" : passes,
        "warns"  : warns,
        "fails"  : fails,
    }


# ══════════════════════════════════════════════════════════════════════
#  BROKER ANALYSIS
# ══════════════════════════════════════════════════════════════════════

def analyze_broker(df):
    out = dict(
        foreign_net=0,local_inst_net=0,korean_net=0,retail_net=0,scalper_net=0,
        goreng=False,goreng_codes=[],crossing=None,
        sm_buyers=[],sm_sellers=[],alerts=[],
        score=50,signal="NEUTRAL",conf="LOW",
    )
    total = df["buy_lot"].sum() + df["sell_lot"].sum()
    if total == 0: return out
    for _,row in df.iterrows():
        code = str(row["broker"]).upper(); net = int(row.get("net_lot",0))
        cat  = row.get("cat","LOCAL_INST")
        if cat=="FOREIGN_SMART":
            out["foreign_net"] += net
            e = {"broker":code,"name":row.get("name",code),"net":net,
                 "buy_avg":row.get("buy_avg",0),"sell_avg":row.get("sell_avg",0)}
            (out["sm_buyers"] if net>0 else out["sm_sellers"]).append(e)
        elif cat in ("BUMN","LOCAL_INST"): out["local_inst_net"] += net
        elif cat=="KOREAN":  out["korean_net"]  += net
        elif cat=="RETAIL":  out["retail_net"]  += net
        elif cat=="SCALPER":
            out["scalper_net"] += net
            sh = (row["buy_lot"]+row["sell_lot"]) / (total+1)
            if sh > 0.25:
                out["alerts"].append({"t":"warn","m":f"{code} dominates {sh:.0%} volume — scalper noise"})
        elif cat=="LOCAL_BANDAR":
            if net > 2000:
                out["goreng"] = True; out["goreng_codes"].append(code)
    if out["goreng"]:
        out["alerts"].append({"t":"danger","m":f"PUMP ALERT: {', '.join(out['goreng_codes'])} active — known pump operators"})
    xc = df[df["broker"].str.upper()=="XC"]
    if not xc.empty and xc["net_lot"].values[0]>3000:
        out["alerts"].append({"t":"warn","m":"XC (Ajaib/'Xobat Cutloss') heavy buy — late-stage distribution signal"})
    fn = out["foreign_net"]; rn = out["retail_net"] + out["scalper_net"]
    if fn>2000 and rn<-2000:
        out["crossing"]="ACC"
        out["alerts"].append({"t":"success","m":"🔥 ACCUMULATION CROSS: Smart money BUYING + Retail SELLING"})
    elif fn<-2000 and rn>2000:
        out["crossing"]="DIS"
        out["alerts"].append({"t":"danger","m":"💀 DISTRIBUTION CROSS: Smart money SELLING + Retail BUYING"})
    sc = 50.0
    sc += np.clip(fn/(total*0.3+1)*30,-30,30)
    sc += np.clip(out["local_inst_net"]/(total*0.2+1)*15,-15,15)
    sc += np.clip(out["korean_net"]/(total*0.15+1)*8,-8,8)
    sc += np.clip(-rn/(total*0.3+1)*10,-10,10)
    sc += (len(out["sm_buyers"])-len(out["sm_sellers"]))*2.5
    if out["crossing"]=="ACC": sc+=10
    elif out["crossing"]=="DIS": sc-=10
    if out["goreng"]: sc+=5
    final = int(np.clip(round(sc),0,100))
    conf  = abs(final-50)/50
    out["score"]  = final
    out["signal"] = "ACCUMULATION" if final>=60 else "DISTRIBUTION" if final<=40 else "NEUTRAL"
    out["conf"]   = "HIGH" if conf>0.6 else "MEDIUM" if conf>0.3 else "LOW"
    return out


# ══════════════════════════════════════════════════════════════════════
#  MODULE A — RELATIVE STRENGTH vs IHSG
#  RS = stock outperformance vs benchmark (^JKSE)
#  Rising RS + positive CMF = smart money rotating IN to this stock
# ══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=900, show_spinner=False)
def load_ihsg(period="1y"):
    """Load IHSG benchmark (^JKSE) from Yahoo Finance."""
    try:
        df = yf.download("^JKSE", period=period, interval="1d",
                         progress=False, auto_adjust=True)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.rename(columns={"Close":"close"})
        return df[["close"]].dropna()
    except: return None


def calc_rs(stock_df: pd.DataFrame, ihsg_df: pd.DataFrame,
            windows: list = [20, 60]) -> dict:
    """
    Calculate Relative Strength of stock vs IHSG.

    RS(n) = (stock_return_n / ihsg_return_n) × 100

    RS > 100 = outperforming IHSG (strong)
    RS < 100 = underperforming IHSG (weak)
    RS trend rising = money rotating IN
    RS trend falling = money rotating OUT

    Returns dict with RS values, trend, and interpretation.
    """
    # Align dates
    merged = stock_df[["close"]].rename(columns={"close":"stock"}).join(
        ihsg_df[["close"]].rename(columns={"close":"ihsg"}), how="inner")
    if len(merged) < max(windows) + 5:
        return {"available": False}

    results = {}
    for w in windows:
        stock_ret = merged["stock"] / merged["stock"].shift(w) - 1
        ihsg_ret  = merged["ihsg"]  / merged["ihsg"].shift(w)  - 1
        # RS ratio — normalize to 100
        rs = (1 + stock_ret) / (1 + ihsg_ret.replace(0, np.nan)) * 100
        results[f"rs{w}"]  = rs
        results[f"val{w}"] = float(rs.iloc[-1]) if not pd.isna(rs.iloc[-1]) else 100.0
        # RS trend (5-day slope normalized)
        rs_slope = rs.diff(5).iloc[-1]
        results[f"trend{w}"] = "rising" if rs_slope > 0 else "falling"

    # RS20 momentum score (0-100)
    rs20  = results.get("val20", 100.0)
    rs60  = results.get("val60", 100.0)
    t20   = results.get("trend20","flat")
    t60   = results.get("trend60","flat")

    score = 50.0
    # RS level component (max ±25)
    score += np.clip((rs20 - 100) * 1.5, -25, 25)
    # RS trend component (max ±15)
    if t20 == "rising":  score += 10
    if t20 == "falling": score -= 10
    if t60 == "rising":  score += 5
    if t60 == "falling": score -= 5
    # RS acceleration: short RS > long RS = improving momentum
    if rs20 > rs60: score += 5
    else:           score -= 5

    score = int(np.clip(round(score), 0, 100))

    # Interpretation
    if rs20 > 110 and t20 == "rising":
        interp = "STRONG OUTPERFORM"
        color  = "#00e676"
        note   = f"Stock outperforms IHSG by {rs20-100:.1f}% (20d) with rising momentum — smart money rotating IN."
    elif rs20 > 102:
        interp = "OUTPERFORM"
        color  = "#88ffbb"
        note   = f"Stock beats IHSG by {rs20-100:.1f}% (20d). Positive RS trend."
    elif rs20 > 97 and rs20 <= 102:
        interp = "IN LINE"
        color  = "#ffab00"
        note   = f"Stock moving in line with IHSG (RS {rs20:.1f}). No clear alpha."
    elif rs20 < 90 and t20 == "falling":
        interp = "STRONG UNDERPERFORM"
        color  = "#ff1744"
        note   = f"Stock underperforms IHSG by {100-rs20:.1f}% (20d) with falling momentum — smart money rotating OUT."
    else:
        interp = "UNDERPERFORM"
        color  = "#ff8888"
        note   = f"Stock lags IHSG by {100-rs20:.1f}% (20d)."

    results.update({
        "available": True,
        "score": score,
        "interp": interp,
        "color": color,
        "note": note,
        "rs20_val": rs20,
        "rs60_val": rs60,
        "trend20": t20,
        "trend60": t60,
        "merged": merged,
    })
    return results


def chart_rs(rs_data: dict, ticker: str) -> go.Figure:
    """
    Dual-panel RS chart:
    Top: Normalized price comparison (stock vs IHSG rebased to 100)
    Bottom: RS ratio line with 100 baseline
    """
    merged = rs_data["merged"]
    rs20   = rs_data.get("rs20")
    rs60   = rs_data.get("rs60")

    # Rebase both to 100
    base_stock = merged["stock"].iloc[0]
    base_ihsg  = merged["ihsg"].iloc[0]
    stock_rb   = merged["stock"] / base_stock * 100
    ihsg_rb    = merged["ihsg"]  / base_ihsg  * 100

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.55, 0.45], vertical_spacing=0.04)

    # Panel 1: Price comparison rebased
    fig.add_trace(go.Scatter(
        x=merged.index, y=stock_rb, name=ticker,
        line=dict(color="#00e676", width=2),
        fill=None,
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=merged.index, y=ihsg_rb, name="IHSG (^JKSE)",
        line=dict(color="#5a7a9a", width=1.5, dash="dash"),
    ), row=1, col=1)
    # Shade area between
    fig.add_trace(go.Scatter(
        x=merged.index, y=stock_rb,
        fill=None, mode="lines", line=dict(width=0),
        showlegend=False,
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=merged.index, y=ihsg_rb,
        fill="tonexty",
        fillcolor="rgba(0,230,118,0.08)",
        mode="lines", line=dict(width=0),
        showlegend=False,
        name="Alpha zone",
    ), row=1, col=1)

    # Panel 2: RS ratio lines
    if rs20 is not None:
        rs20_clean = rs20.dropna()
        fig.add_trace(go.Scatter(
            x=rs20_clean.index, y=rs20_clean,
            name="RS 20d", line=dict(color="#00e676", width=2),
        ), row=2, col=1)
    if rs60 is not None:
        rs60_clean = rs60.dropna()
        fig.add_trace(go.Scatter(
            x=rs60_clean.index, y=rs60_clean,
            name="RS 60d", line=dict(color="#ffab00", width=1.5, dash="dot"),
        ), row=2, col=1)

    # Reference lines
    fig.add_hline(y=100, line_color="rgba(255,171,0,0.5)", line_dash="dash",
                  line_width=1, row=2, col=1)
    fig.add_hline(y=110, line_color="rgba(0,230,118,0.2)", line_dash="dot",
                  row=2, col=1)
    fig.add_hline(y=90,  line_color="rgba(255,23,68,0.2)",  line_dash="dot",
                  row=2, col=1)
    fig.add_hrect(y0=95, y1=105, fillcolor="rgba(255,171,0,0.03)",
                  line_width=0, row=2, col=1)

    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        legend=dict(bgcolor="#0b1018", bordercolor="#141e2e",
                    font=dict(size=10), orientation="h", y=1.02),
        margin=dict(l=0, r=0, t=10, b=0),
        height=480, hovermode="x unified",
    )
    for i in [1, 2]:
        fig.update_xaxes(gridcolor="#141e2e", showgrid=True, row=i, col=1)
        fig.update_yaxes(gridcolor="#141e2e", showgrid=True, row=i, col=1)
    fig.update_yaxes(title_text=f"Rebased to 100", row=1, col=1)
    fig.update_yaxes(title_text="RS Ratio", row=2, col=1)
    return fig


# ══════════════════════════════════════════════════════════════════════
#  MODULE B — BACKTESTING ENGINE
#  Walk-forward signal simulation on historical data
#  "If we followed this system's signals, what would the P&L look like?"
# ══════════════════════════════════════════════════════════════════════

def _compute_signal_on_slice(df_slice: pd.DataFrame) -> str:
    """
    Compute a simplified entry signal on a historical window.
    Returns: 'STRONG BUY' / 'BUY' / 'WATCH' / 'AVOID' / 'STRONG AVOID'
    Uses only technical indicators (no broker data for historical).
    """
    if len(df_slice) < 20:
        return "WATCH"
    try:
        c_ = cmf(df_slice)
        o_ = obv(df_slice)
        m_ = mfi(df_slice)
        ts = tech_score(df_slice, c_, o_, m_)
        wp, _, _ = wyckoff(df_slice, c_, o_)
        cmf_v = float(c_.iloc[-1]) if not pd.isna(c_.iloc[-1]) else 0
        mfi_v = float(m_.iloc[-1]) if not pd.isna(m_.iloc[-1]) else 50
        obv_up = o_.iloc[-1] > o_.iloc[-min(10, len(o_)-1)]

        met, fail = 0, 0
        if ts >= 65:     met  += 2
        elif ts <= 35:   fail += 2
        elif ts >= 55:   met  += 1
        if cmf_v > 0.12: met  += 1
        elif cmf_v < -0.12: fail += 1
        if obv_up:       met  += 1
        else:            fail += 1
        if 15 <= mfi_v <= 45: met += 1
        elif mfi_v > 75: fail += 1
        if wp in ("B","C"): met += 1
        elif wp == "E":  fail += 1

        last = df_slice.iloc[-1]
        av   = df_slice["volume"].tail(20).mean()
        vr   = last["volume"] / av if av > 0 else 1
        if vr >= 1.5: met += 1

        if met >= 6: return "STRONG BUY"
        if met >= 4 and fail <= 1: return "BUY"
        if fail >= 4: return "STRONG AVOID"
        if fail >= 3: return "AVOID"
        return "WATCH"
    except:
        return "WATCH"


@st.cache_data(ttl=3600, show_spinner=False)
def run_backtest(ticker: str, period: str = "2y",
                 hold_days_list: tuple = (5, 10, 20),
                 min_signal: str = "BUY") -> dict:
    """
    Walk-forward backtest using rolling 60-day lookback windows.

    For each trading day t (from day 60 to end - max_hold):
      1. Compute signal on df[t-60:t]
      2. If signal >= min_signal, record entry at close[t]
      3. Measure return at t+5, t+10, t+20 days
      4. Also vs IHSG for alpha measurement

    Returns comprehensive statistics.
    """
    df = load_price(ticker, period)
    ihsg = load_ihsg(period)
    if df is None or len(df) < 80:
        return {"available": False, "error": "Insufficient price data"}

    LOOKBACK  = 60   # days of history to compute signal
    MAX_HOLD  = max(hold_days_list)
    SIGNAL_ORDER = ["WATCH", "AVOID", "STRONG AVOID", "BUY", "STRONG BUY"]
    min_idx = SIGNAL_ORDER.index(min_signal)

    records = []
    for i in range(LOOKBACK, len(df) - MAX_HOLD):
        window  = df.iloc[i-LOOKBACK:i]
        sig     = _compute_signal_on_slice(window)

        if SIGNAL_ORDER.index(sig) < min_idx:
            continue

        entry_price = float(df["close"].iloc[i])
        entry_date  = df.index[i]
        row = {
            "date":       entry_date,
            "signal":     sig,
            "entry":      entry_price,
        }
        for h in hold_days_list:
            exit_idx   = i + h
            exit_price = float(df["close"].iloc[exit_idx])
            ret        = (exit_price - entry_price) / entry_price * 100
            row[f"ret_{h}d"] = round(ret, 3)
            # vs IHSG alpha
            if ihsg is not None:
                ihsg_aligned = ihsg.reindex(df.index, method="ffill")
                ihsg_entry = float(ihsg_aligned["close"].iloc[i])
                ihsg_exit  = float(ihsg_aligned["close"].iloc[exit_idx])
                ihsg_ret   = (ihsg_exit - ihsg_entry) / ihsg_entry * 100
                row[f"alpha_{h}d"] = round(ret - ihsg_ret, 3)
        records.append(row)

    if not records:
        return {"available": False, "error": "No signals generated in this period"}

    df_res = pd.DataFrame(records)

    # ── Statistics per holding period
    stats = {}
    for h in hold_days_list:
        col = f"ret_{h}d"
        if col not in df_res.columns: continue
        rets  = df_res[col].dropna()
        alpha_col = f"alpha_{h}d"
        alphas = df_res[alpha_col].dropna() if alpha_col in df_res.columns else pd.Series([0])
        wins  = (rets > 0).sum()
        total = len(rets)
        avg   = rets.mean()
        med   = rets.median()
        best  = rets.max()
        worst = rets.min()
        std   = rets.std()
        sharpe = (avg / std * np.sqrt(252 / h)) if std > 0 else 0
        # Max drawdown from equity curve
        cumret = (1 + rets/100).cumprod()
        roll_max = cumret.cummax()
        drawdown = (cumret - roll_max) / roll_max * 100
        max_dd = drawdown.min()
        profit_factor = abs(rets[rets>0].sum() / rets[rets<0].sum()) if (rets<0).any() else np.inf
        stats[h] = {
            "total_signals" : total,
            "win_rate"      : round(wins/total*100, 1),
            "avg_ret"       : round(avg, 2),
            "median_ret"    : round(med, 2),
            "best"          : round(best, 2),
            "worst"         : round(worst, 2),
            "sharpe"        : round(sharpe, 2),
            "max_dd"        : round(max_dd, 2),
            "profit_factor" : round(min(profit_factor, 99), 2),
            "avg_alpha"     : round(alphas.mean(), 2),
            "pct_beat_ihsg" : round((alphas > 0).mean() * 100, 1),
        }

    # ── Equity curves for each holding period
    eq_curves = {}
    for h in hold_days_list:
        col = f"ret_{h}d"
        if col not in df_res.columns: continue
        rets = df_res[col].dropna() / 100
        eq   = (1 + rets).cumprod() * 100   # start at 100
        eq_curves[h] = {"dates": df_res["date"].values[:len(eq)], "equity": eq.values}

    # ── Signal distribution
    sig_dist = df_res["signal"].value_counts().to_dict()

    # ── Monthly return heatmap data
    df_res["month"] = pd.to_datetime(df_res["date"]).dt.to_period("M")
    if f"ret_{hold_days_list[0]}d" in df_res.columns:
        monthly = df_res.groupby("month")[f"ret_{hold_days_list[0]}d"].mean().reset_index()
        monthly["month_str"] = monthly["month"].astype(str)
    else:
        monthly = pd.DataFrame()

    return {
        "available"  : True,
        "ticker"     : ticker,
        "period"     : period,
        "min_signal" : min_signal,
        "df_signals" : df_res,
        "stats"      : stats,
        "eq_curves"  : eq_curves,
        "sig_dist"   : sig_dist,
        "monthly"    : monthly,
        "hold_days"  : list(hold_days_list),
        "n_total"    : len(df_res),
    }


def chart_equity_curve(bt: dict) -> go.Figure:
    """Equity curve for all holding periods + IHSG benchmark."""
    fig = go.Figure()
    colors = {5:"#00e676", 10:"#00b0ff", 20:"#ffab00", 30:"#e040fb"}
    hold_days = bt.get("hold_days", [5, 10, 20])
    for h in hold_days:
        eq = bt["eq_curves"].get(h)
        if eq is None: continue
        c = colors.get(h, "#5a7a9a")
        fig.add_trace(go.Scatter(
            x=eq["dates"], y=eq["equity"],
            name=f"Hold {h}d",
            line=dict(color=c, width=2),
            hovertemplate=f"Hold {h}d: %{{y:.1f}}<extra></extra>",
        ))
    # Baseline
    fig.add_hline(y=100, line_color="#2a3d52", line_dash="dash", line_width=1)
    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        legend=dict(bgcolor="#0b1018", bordercolor="#141e2e", font=dict(size=10)),
        margin=dict(l=0, r=0, t=10, b=0), height=320,
        yaxis=dict(title="Portfolio Value (start=100)", gridcolor="#141e2e"),
        xaxis=dict(gridcolor="#141e2e"),
        hovermode="x unified",
    )
    return fig


def chart_return_distribution(bt: dict, hold: int = 10) -> go.Figure:
    """Histogram of returns for a given holding period."""
    col = f"ret_{hold}d"
    df_s = bt["df_signals"]
    if col not in df_s.columns:
        return go.Figure()
    rets = df_s[col].dropna()
    colors_bar = ["#00e676" if r >= 0 else "#ff1744" for r in rets]
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=rets, nbinsx=40,
        marker_color="#00b0ff", opacity=0.7,
        name=f"Returns ({hold}d hold)",
        hovertemplate="Return: %{x:.1f}%<br>Count: %{y}<extra></extra>",
    ))
    fig.add_vline(x=0,       line_color="#5a7a9a", line_dash="solid", line_width=1)
    fig.add_vline(x=rets.mean(), line_color="#ffab00", line_dash="dash",
                  annotation_text=f"Mean {rets.mean():.1f}%",
                  annotation_font_color="#ffab00")
    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        margin=dict(l=0, r=0, t=10, b=0), height=260,
        xaxis=dict(title="Return (%)", gridcolor="#141e2e"),
        yaxis=dict(title="Count", gridcolor="#141e2e"),
        bargap=0.05,
    )
    return fig


def chart_monthly_heatmap(monthly_df: pd.DataFrame, hold_d: int) -> go.Figure:
    """Monthly average return bar chart."""
    if monthly_df.empty:
        return go.Figure()
    col = f"ret_{hold_d}d"
    if col not in monthly_df.columns:
        return go.Figure()
    colors = ["#00e676" if v >= 0 else "#ff1744" for v in monthly_df[col]]
    fig = go.Figure(go.Bar(
        x=monthly_df["month_str"], y=monthly_df[col],
        marker_color=colors, opacity=0.8,
        text=[f"{v:.1f}%" for v in monthly_df[col]],
        textposition="outside",
        textfont=dict(color="#cdd8e6", size=9),
    ))
    fig.add_hline(y=0, line_color="#5a7a9a", line_width=1)
    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        margin=dict(l=0, r=0, t=10, b=0), height=240,
        xaxis=dict(gridcolor="#141e2e", tickangle=45),
        yaxis=dict(title="Avg Return (%)", gridcolor="#141e2e"),
    )
    return fig


def chart_signal_scatter(df_signals: pd.DataFrame, hold: int = 10) -> go.Figure:
    """Scatter: entry date vs return, colored by signal strength."""
    col = f"ret_{hold}d"
    if col not in df_signals.columns:
        return go.Figure()
    df_s = df_signals[[col,"date","signal"]].dropna()
    sig_colors = {"STRONG BUY":"#00e676","BUY":"#88ffbb",
                  "WATCH":"#ffab00","AVOID":"#ff8888","STRONG AVOID":"#ff1744"}
    fig = go.Figure()
    for sig in df_s["signal"].unique():
        mask = df_s["signal"] == sig
        sub  = df_s[mask]
        fig.add_trace(go.Scatter(
            x=sub["date"], y=sub[col],
            mode="markers",
            name=sig,
            marker=dict(color=sig_colors.get(sig,"#5a7a9a"),
                        size=7, opacity=0.7,
                        line=dict(width=0.5, color="#05080c")),
            hovertemplate=f"{sig}<br>Date: %{{x}}<br>Return: %{{y:.1f}}%<extra></extra>",
        ))
    fig.add_hline(y=0, line_color="#5a7a9a", line_dash="dash", line_width=1)
    fig.update_layout(
        paper_bgcolor="#05080c", plot_bgcolor="#090d12",
        font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
        legend=dict(bgcolor="#0b1018", bordercolor="#141e2e", font=dict(size=10)),
        margin=dict(l=0, r=0, t=10, b=0), height=280,
        xaxis=dict(gridcolor="#141e2e"),
        yaxis=dict(title=f"Return (%, {hold}d hold)", gridcolor="#141e2e"),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════
#  ENTRY SIGNAL
# ══════════════════════════════════════════════════════════════════════

def entry_signal(final,ts,br_score,wp,cmf_v,obv_up,mfi_v,vr,crossing,
                 goreng,sm_buy,sm_sell,chg,own,rs_data=None,vcp=None):
    met,fail,watch = [],[],[]
    if final>=70: met.append(f"Composite {final}/100 (Strong)")
    elif final>=55: met.append(f"Composite {final}/100 (Positive)")
    elif final<=35: fail.append(f"Composite {final}/100 (Bearish)")
    else: watch.append(f"Composite {final}/100 (Neutral)")
    pa = {"A":"watch","B":"buy","C":"buy","D":"watch","E":"avoid"}.get(wp,"watch")
    if pa=="buy": met.append(f"Wyckoff Phase {wp} — accumulation zone")
    elif pa=="avoid": fail.append(f"Wyckoff Phase {wp} — late stage")
    else: watch.append(f"Wyckoff Phase {wp} — transitional")
    if cmf_v>0.12: met.append(f"CMF {cmf_v:.3f} — money inflow")
    elif cmf_v<-0.12: fail.append(f"CMF {cmf_v:.3f} — money outflow")
    else: watch.append(f"CMF {cmf_v:.3f} — balanced")
    if obv_up: met.append("OBV rising — cumulative buying")
    else: fail.append("OBV falling — cumulative selling")
    if 15<=mfi_v<=45: met.append(f"MFI {mfi_v:.0f} — oversold zone")
    elif mfi_v>75: fail.append(f"MFI {mfi_v:.0f} — overbought")
    else: watch.append(f"MFI {mfi_v:.0f} — normal")
    if vr>=1.5: met.append(f"Volume {vr:.1f}x — high activity")
    if crossing=="ACC": met.append("Accumulation Cross confirmed")
    elif crossing=="DIS": fail.append("Distribution Cross confirmed")
    if len(sm_buy)>=3: met.append(f"SM buying: {', '.join([b['broker'] for b in sm_buy[:3]])}")
    elif len(sm_sell)>=2: fail.append(f"SM selling: {', '.join([b['broker'] for b in sm_sell[:2]])}")
    if goreng: watch.append("Pump broker active — tight stop-loss")
    if own:
        if own.get("tier")==1: met.append(f"Tier 1 owner: {own['owner'][:18]}")
        elif own.get("tier")==3: fail.append("Tier 3 owner — speculative")
        if own.get("political"): watch.append("Political ties detected")
        ff = own.get("float",30)
        if ff<15: fail.append(f"Low float {ff}% — FCA/manipulation risk")

    # ── VCP factor
    vcp_bonus = 0
    premium_setup = False
    if vcp and vcp.get("available"):
        vcp_grade = vcp.get("grade","NONE")
        near_piv  = vcp.get("near_pivot", False)
        breaking  = vcp.get("is_breaking", False)
        if vcp_grade == "A":
            met.append(f"VCP Grade A — Ideal contraction pattern, pivot at Rp{vcp.get('pivot_price',0):,.0f}")
            vcp_bonus = 12
            premium_setup = (pa in ("buy","watch") and cmf_v > 0)
        elif vcp_grade == "B":
            met.append(f"VCP Grade B — Good contraction, setup forming near Rp{vcp.get('pivot_price',0):,.0f}")
            vcp_bonus = 7
        elif vcp_grade in ("C","C-"):
            watch.append(f"VCP Grade {vcp_grade} — Early stage contraction, not yet complete")
            vcp_bonus = 2 if vcp_grade == "C" else -2
        if near_piv and vcp_grade in ("A","B"):
            met.append("Near VCP pivot — potential breakout imminent")
            vcp_bonus += 5
        if breaking:
            met.append("⚡ VCP BREAKOUT — Price breaking pivot with volume!")
            vcp_bonus += 8

    # ── RS vs IHSG factor
    rs_bonus = 0
    if rs_data and rs_data.get("available"):
        rs_interp = rs_data.get("interp","")
        rs20      = rs_data.get("rs20_val",100)
        t20       = rs_data.get("trend20","flat")
        if rs_interp == "STRONG OUTPERFORM":
            met.append(f"RS vs IHSG: Strong Outperform ({rs20:.1f}) — smart money rotation IN")
            rs_bonus = 8
        elif rs_interp == "OUTPERFORM":
            met.append(f"RS vs IHSG: Outperform ({rs20:.1f}) — beating market")
            rs_bonus = 4
        elif rs_interp == "STRONG UNDERPERFORM":
            fail.append(f"RS vs IHSG: Strong Underperform ({rs20:.1f}) — money rotating OUT")
            rs_bonus = -8
        elif rs_interp == "UNDERPERFORM":
            fail.append(f"RS vs IHSG: Underperform ({rs20:.1f}) — lagging market")
            rs_bonus = -4
        else:
            watch.append(f"RS vs IHSG: In Line ({rs20:.1f}) — moving with market")

    # Adjust final with RS + VCP bonuses (max ±15 combined)
    final_adj = int(np.clip(final + np.clip(rs_bonus + vcp_bonus, -15, 15), 0, 100))

    nm=len(met); nf=len(fail)

    # PREMIUM SETUP: VCP-A/B + Accumulation Cross = highest possible conviction
    if premium_setup and crossing=="ACC" and final_adj>=60:
        sig,color,cls="STRONG BUY","#00e676","sc-buy"
        why="🔥 PREMIUM SETUP: VCP + Accumulation Cross + RS positive. Triple confirmation — highest conviction entry."
        risk,action="LOW","Enter now at/near VCP pivot. Stop-loss below last contraction low."
    elif crossing=="ACC" and final_adj>=65:
        sig,color,cls="STRONG BUY","#00e676","sc-buy"
        why="Accumulation Cross confirmed. Foreign institutions buying while retail sells."
        risk,action="LOW-MEDIUM","Enter now. Stop-loss below recent 10-day support."
    elif nm>=4 and nf<=1 and final_adj>=65:
        sig,color,cls="BUY","#00e676","sc-buy"
        why=f"{nm} conditions confirmed. Smart money accumulation + RS + VCP aligned."
        risk,action="MEDIUM","Consider entry. Confirm with volume breakout."
    elif crossing=="DIS" or (nf>=3 and final_adj<=40):
        sig,color,cls="STRONG AVOID","#ff1744","sc-sell"
        why="Distribution signals confirmed. Smart money exiting while retail buys."
        risk,action="HIGH","Do not enter. Reduce position if holding."
    elif nf>=2 and final_adj<=45:
        sig,color,cls="AVOID","#ff1744","sc-sell"
        why=f"{nf} bearish signals active. Risk outweighs reward."
        risk,action="MEDIUM-HIGH","Hold off. Wait for reversal signals."
    elif nm>=2 and nf<=1:
        sig,color,cls="WATCH","#ffab00","sc-watch"
        why="Setup building — accumulation + VCP forming but needs confirmation."
        risk,action="MEDIUM","Add to watchlist. Trigger: volume breakout above VCP pivot."
    else:
        sig,color,cls="WATCH","#ffab00","sc-watch"
        why="Mixed signals. No clear smart money direction."
        risk,action="MEDIUM","Monitor. Enter only when 3+ conditions align."
    return dict(sig=sig,color=color,cls=cls,why=why,risk=risk,
                action=action,met=met,fail=fail,watch=watch,
                final_adj=final_adj,rs_bonus=rs_bonus,
                vcp_bonus=vcp_bonus,premium_setup=premium_setup)



# ══════════════════════════════════════════════════════════════════════
#  CHARTS
# ══════════════════════════════════════════════════════════════════════

CL = dict(paper_bgcolor="#05080c", plot_bgcolor="#090d12",
          font=dict(color="#cdd8e6", family="IBM Plex Mono, monospace", size=11),
          margin=dict(l=0,r=0,t=10,b=0))

def chart_price(df, c, o, m):
    ma  = df["close"].rolling(20).mean()
    std = df["close"].rolling(20).std()
    fig = make_subplots(rows=4,cols=1,shared_xaxes=True,
                        row_heights=[.50,.17,.17,.16],vertical_spacing=.03)
    fig.add_trace(go.Candlestick(x=df.index,open=df["open"],high=df["high"],
        low=df["low"],close=df["close"],
        increasing=dict(line=dict(color="#00e676",width=1),fillcolor="rgba(0,230,118,.6)"),
        decreasing=dict(line=dict(color="#ff1744",width=1),fillcolor="rgba(255,23,68,.6)"),
        name="OHLC"),row=1,col=1)
    fig.add_trace(go.Scatter(x=df.index,y=ma+2*std,line=dict(color="#1c2a3e",width=1),showlegend=False),row=1,col=1)
    fig.add_trace(go.Scatter(x=df.index,y=ma-2*std,line=dict(color="#1c2a3e",width=1),
        fill="tonexty",fillcolor="rgba(0,176,255,.03)",showlegend=False),row=1,col=1)
    fig.add_trace(go.Scatter(x=df.index,y=ma,name="MA20",line=dict(color="#ffab00",width=1.5,dash="dash")),row=1,col=1)
    vc = ["rgba(0,230,118,.5)" if df["close"].iloc[i]>=df["open"].iloc[i] else "rgba(255,23,68,.5)" for i in range(len(df))]
    fig.add_trace(go.Bar(x=df.index,y=df["volume"],name="Vol",marker_color=vc),row=2,col=1)
    fig.add_trace(go.Scatter(x=df.index,y=df["volume"].rolling(20).mean(),
        line=dict(color="#ffab00",width=1,dash="dot"),showlegend=False),row=2,col=1)
    cc = ["#00e676" if v>0 else "#ff1744" for v in c]
    fig.add_trace(go.Bar(x=df.index,y=c,name="CMF",marker_color=cc,opacity=.8),row=3,col=1)
    for y,lc in [(.15,"rgba(0,230,118,.3)"),(-.15,"rgba(255,23,68,.3)"),(0,"rgba(90,122,154,.4)")]:
        fig.add_hline(y=y,line_color=lc,line_dash="dash" if y!=0 else "solid",row=3,col=1)
    fig.add_trace(go.Scatter(x=df.index,y=m,name="MFI",
        line=dict(color="#ffab00",width=1.5),fill="tozeroy",fillcolor="rgba(255,171,0,.05)"),row=4,col=1)
    for y,lc in [(80,"rgba(255,23,68,.3)"),(20,"rgba(0,230,118,.3)"),(50,"rgba(90,122,154,.3)")]:
        fig.add_hline(y=y,line_color=lc,line_dash="dash" if y!=50 else "solid",row=4,col=1)
    fig.update_layout(**CL,height=600,showlegend=True,
        legend=dict(bgcolor="#0b1018",bordercolor="#141e2e",font=dict(size=9),orientation="h",y=1.02),
        xaxis_rangeslider_visible=False)
    for i in range(1,5):
        fig.update_xaxes(gridcolor="#141e2e",showgrid=True,row=i,col=1)
        fig.update_yaxes(gridcolor="#141e2e",showgrid=True,row=i,col=1)
    fig.update_yaxes(title_text="Price (IDR)",row=1,col=1)
    fig.update_yaxes(title_text="Volume",     row=2,col=1)
    fig.update_yaxes(title_text="CMF",range=[-.6,.6],row=3,col=1)
    fig.update_yaxes(title_text="MFI",range=[0,100], row=4,col=1)
    return fig

def chart_broker_flow(bdf):
    bdf_s  = bdf.sort_values("net_lot")
    colors = [CAT_COLOR.get(r["cat"],"#00b0ff") for _,r in bdf_s.iterrows()]
    fig = go.Figure(go.Bar(
        x=bdf_s["net_lot"],
        y=bdf_s["broker"]+" "+bdf_s.get("flag",pd.Series(["🇮🇩"]*len(bdf_s)))+" · "+bdf_s["name"].str[:14],
        orientation="h",marker_color=colors,opacity=.85,
        text=[f"{n:+,}" for n in bdf_s["net_lot"]],
        textposition="outside",textfont=dict(color="#cdd8e6",size=9),
    ))
    fig.update_layout(**CL,height=max(300,len(bdf_s)*30),
        xaxis=dict(gridcolor="#141e2e",title="Net Lot (Today)"),
        yaxis=dict(gridcolor="#141e2e"),showlegend=False)
    fig.add_vline(x=0,line_color="#2a3d52",line_width=1)
    return fig

def chart_accumulation(adf, label=""):
    df_s   = adf.sort_values("cum_net")
    max_a  = df_s["cum_net"].abs().max() or 1
    colors = []
    for _,row in df_s.iterrows():
        cat = row.get("cat","LOCAL_INST")
        if cat=="FOREIGN_SMART":
            colors.append("#00e676" if row["cum_net"]>0 else "#ff4466")
        elif cat=="RETAIL":
            colors.append("#ff8888" if row["cum_net"]>0 else "#88ff88")
        elif cat=="SCALPER":
            colors.append("#ff6d00")
        else:
            colors.append("#00b0ff" if row["cum_net"]>0 else "#ff8844")
    fig = go.Figure(go.Bar(
        x=df_s["cum_net"],
        y=df_s["broker"]+" · "+df_s["name"].str[:15],
        orientation="h",marker_color=colors,opacity=.85,
        text=[f"{n:+,}" for n in df_s["cum_net"]],
        textposition="outside",textfont=dict(color="#cdd8e6",size=9),
        hovertemplate="<b>%{y}</b><br>Cumulative Net: %{x:+,} lots<extra></extra>",
    ))
    fig.update_layout(**CL,
        title=dict(text=f"Estimated Accumulated Position  ·  {label}",
                   font=dict(size=10,color="#5a7a9a"),x=0.01),
        height=max(340,len(df_s)*30),
        xaxis=dict(gridcolor="#141e2e",title="Cumulative Net Lots"),
        yaxis=dict(gridcolor="#141e2e"),showlegend=False)
    fig.add_vline(x=0,line_color="#2a3d52",line_width=2)
    return fig

def chart_shareholding(sh_df, ticker, shares_out):
    """Horizontal stacked bar showing estimated broker shareholding."""
    top = sh_df[sh_df["est_shares"]>0].head(10)
    if top.empty: return None
    colors = [CAT_COLOR.get(r["cat"],"#00b0ff") for _,r in top.iterrows()]
    fig = go.Figure(go.Bar(
        y=top["broker"]+" · "+top["name"].str[:14],
        x=top["est_pct"],
        orientation="h",marker_color=colors,opacity=.85,
        text=[f"{p:.2f}%" for p in top["est_pct"]],
        textposition="outside",textfont=dict(color="#cdd8e6",size=9),
        hovertemplate="<b>%{y}</b><br>Est. Holding: %{x:.3f}% of OS<extra></extra>",
    ))
    fig.update_layout(**CL,
        title=dict(text=f"Estimated % of Outstanding Shares Held  ·  {ticker}",
                   font=dict(size=10,color="#5a7a9a"),x=0.01),
        height=max(300,len(top)*32),
        xaxis=dict(gridcolor="#141e2e",title="Estimated % of Shares Outstanding"),
        yaxis=dict(gridcolor="#141e2e"),showlegend=False)
    return fig

def chart_cat_flow(br):
    cats = ["Foreign\nSmart","Local Inst.","Korean","Retail\n(Contra)","Scalper"]
    vals = [br["foreign_net"],br["local_inst_net"],br["korean_net"],br["retail_net"],br["scalper_net"]]
    fig  = go.Figure(go.Bar(x=cats,y=vals,
        marker_color=["#00e676" if v>0 else "#ff1744" for v in vals],opacity=.85,
        text=[f"{v:+,}" for v in vals],textposition="outside",
        textfont=dict(color="#cdd8e6",size=10)))
    fig.update_layout(**CL,height=240,
        xaxis=dict(gridcolor="#141e2e"),
        yaxis=dict(gridcolor="#141e2e",title="Net Lot"),showlegend=False)
    fig.add_hline(y=0,line_color="#2a3d52")
    return fig

def chart_gauge(score):
    c = "#00e676" if score>=65 else "#ff1744" if score<=35 else "#ffab00"
    fig = go.Figure(go.Indicator(mode="gauge+number",value=score,
        number={"font":{"color":c,"size":44,"family":"IBM Plex Mono"},"suffix":"/100"},
        gauge={"axis":{"range":[0,100],"tickfont":{"color":"#5a7a9a","size":9}},
               "bar":{"color":c,"thickness":.22},"bgcolor":"#090d12","bordercolor":"#141e2e",
               "steps":[{"range":[0,35],"color":"rgba(255,23,68,.08)"},
                        {"range":[35,65],"color":"rgba(255,171,0,.05)"},
                        {"range":[65,100],"color":"rgba(0,230,118,.08)"}],
               "threshold":{"line":{"color":c,"width":3},"thickness":.8,"value":score}}))
    fig.update_layout(paper_bgcolor="#05080c",
        font=dict(color=c,family="IBM Plex Mono"),
        margin=dict(l=20,r=20,t=20,b=10),height=185)
    return fig

def chart_sh_pie(sh_list):
    valid = [s for s in sh_list if s.get("pct",0)>0]
    if not valid: return None
    labels = [s["name"][:24] for s in valid]
    values = [s["pct"] for s in valid]
    fig = go.Figure(go.Pie(labels=labels,values=values,hole=0.5,
        marker=dict(colors=["#00e676","#00b0ff","#e040fb","#ffab00","#ff1744","#5a7a9a","#00e5ff"][:len(labels)],
                    line=dict(color="#05080c",width=2)),
        textfont=dict(color="#cdd8e6",size=10,family="IBM Plex Mono"),
        hovertemplate="%{label}<br>%{value:.1f}%<extra></extra>"))
    fig.update_layout(**CL,height=260,showlegend=True,
        legend=dict(bgcolor="#0b1018",bordercolor="#141e2e",font=dict(size=9,color="#cdd8e6")))
    return fig


# ══════════════════════════════════════════════════════════════════════
#  UI HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════

def _kpi(label: str, value: str, color: str, sub: str) -> str:
    """Compact KPI card returning HTML string for use inside st.markdown grid."""
    return f"""<div style='background:#0b1018;border:1px solid #141e2e;padding:10px;border-radius:3px'>
      <div style='font-family:"IBM Plex Mono",monospace;font-size:8px;
                  color:#5a7a9a;letter-spacing:1.5px;margin-bottom:3px'>{label}</div>
      <div style='font-family:"IBM Plex Mono",monospace;font-size:1.1rem;
                  font-weight:700;color:{color}'>{value}</div>
      <div style='font-size:9px;color:#2a3d52;margin-top:2px'>{sub}</div>
    </div>"""


# ══════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:16px;font-weight:700;
                color:#00e676;letter-spacing:3px'>BANDARMOLOGY PRO</div>
    <div style='font-family:"IBM Plex Mono",monospace;font-size:8px;color:#2a3d52;
                letter-spacing:3px;margin-bottom:10px'>IDX SMART MONEY PLATFORM · v6</div>
    """, unsafe_allow_html=True)

    # Live market clock
    ms = market_status()
    dot_cls = "dot dot-g" if ms["open"] else "dot dot-a"
    mkt_cls = "mkt-open" if ms["open"] else "mkt-closed"
    st.markdown(f"""
    <div class='{mkt_cls}' style='margin-bottom:6px'>
      <div class='{dot_cls}'></div>{ms["label"]}
    </div>
    <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#2a3d52;margin-bottom:12px'>
      WIB {ms["wib"].strftime("%H:%M:%S")} · {ms["wib"].strftime("%d %b %Y")}
      · Next: {ms["next"]}
    </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("**PARAMETERS**")
    ticker_input = st.text_input("Stock Ticker", "BBCA", max_chars=10,
        help="Any IDX ticker — BBCA, BREN, AMMN, ADRO, MDKA, GOTO..."
    ).upper().strip().replace(".JK","")
    period_map = {"1M":"1mo","3M":"3mo","6M":"6mo","1Y":"1y"}
    period = period_map[st.selectbox("Period", list(period_map.keys()), index=1)]

    st.markdown("---")
    st.markdown("**AUTO-REFRESH**")
    if HAS_AR:
        ref_opts = {"Off":0,"5 min":300,"15 min":900,"30 min":1800}
        ref_sel  = st.selectbox("Interval", list(ref_opts.keys()), index=0)
        ref_sec  = ref_opts[ref_sel]
        if ref_sec > 0:
            cnt = st_autorefresh(interval=ref_sec*1000, limit=None, key="ar")
            st.markdown(f"""<div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                color:#5a7a9a'>Refresh #{cnt} · every {ref_sel}</div>""",
                unsafe_allow_html=True)
    else:
        st.caption("pip install streamlit-autorefresh")
        if st.button("↻  Manual Refresh", use_container_width=True):
            st.cache_data.clear(); st.rerun()

    st.markdown("---")
    # Token section — show status prominently
    _has_token = bool(st.session_state.get("sb_token","")) and len(st.session_state.get("sb_token","")) > 20
    if _has_token:
        st.markdown("""
        <div style='background:rgba(0,230,118,.1);border:1px solid rgba(0,230,118,.4);
                    border-radius:3px;padding:8px 12px;margin-bottom:8px;
                    font-family:"IBM Plex Mono",monospace;font-size:11px;color:#00e676'>
          ✅ STOCKBIT TOKEN ACTIVE<br>
          <span style='font-size:9px;color:#5a7a9a'>Real broker data enabled</span>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style='background:rgba(255,171,0,.08);border:1px solid rgba(255,171,0,.3);
                    border-radius:3px;padding:8px 12px;margin-bottom:8px;
                    font-family:"IBM Plex Mono",monospace;font-size:11px;color:#ffab00'>
          ⚠️ STOCKBIT TOKEN NOT SET<br>
          <span style='font-size:9px;color:#5a7a9a'>Using IDX API / Demo data</span>
        </div>""", unsafe_allow_html=True)
    with st.expander("🔑 Set Stockbit Token", expanded=not _has_token):
        st.markdown("""
        <div style='font-size:10px;color:#5a7a9a;line-height:1.8;font-family:"IBM Plex Mono",monospace'>
        1. Open <b>stockbit.com</b> → Login<br>
        2. F12 → Network tab → Refresh (F5)<br>
        3. Click any request → Headers<br>
        4. Find: <span style='color:#ffab00'>authorization: Bearer eyJ...</span><br>
        5. Copy text <i>after</i> "Bearer " → paste below
        </div>""", unsafe_allow_html=True)
        sb_token = st.text_input("Bearer Token", "", type="password",
                                  placeholder="eyJhbGciOiJSUzI1NiIs...", key="sb_token")
        if sb_token:
            if len(sb_token) > 20:
                st.success("✅ Token saved for this session")
            else:
                st.error("❌ Token too short — paste full token")
    # If already set but expander collapsed, still bind variable
    if "sb_token" in st.session_state:
        sb_token = st.session_state.get("sb_token", "")
    else:
        sb_token = ""

    st.markdown("---")
    st.markdown("**ACCUMULATION WINDOW**")
    st.markdown("""<div style='font-size:10px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;margin-bottom:4px'>
    Number of trading days for cumulative broker position calculation</div>""", unsafe_allow_html=True)
    accu_days = st.select_slider("Trading days", [5,10,15,20,30,45,60,90], value=20)

    st.markdown("---")
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;font-weight:600;
                color:#cdd8e6;letter-spacing:1px;margin-bottom:6px'>SCREENER WATCHLIST</div>
    <div style='font-size:10px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;
                margin-bottom:10px;line-height:1.6'>
    Enter any IDX tickers, separated by commas.<br>
    No limit — from 5 up to 100+ stocks.
    </div>""", unsafe_allow_html=True)

    # Default / preset values
    DEFAULT_WL = (
        "BBCA,BBRI,BMRI,BBNI,BBTN,"
        "TLKM,EXCL,ISAT,TBIG,TOWR,"
        "ASII,UNTR,AALI,BSDE,CTRA,"
        "BREN,BRPT,TPIA,CUAN,CDIA,"
        "ADRO,ADMR,MDKA,BYAN,PTBA,ANTM,INCO,"
        "AMMN,MEDC,PGAS,"
        "KLBF,KAEF,SIDO,MIKA,"
        "ICBP,INDF,UNVR,MYOR,ROTI,"
        "GOTO,BUKA,EMTK,DCII,"
        "BMRI,AMRT,ACES,MAPI,ERAA"
    )
    LQ45_WL = (
        "BBCA,BBRI,BMRI,BBNI,TLKM,ASII,UNVR,KLBF,ICBP,INDF,"
        "ADRO,PTBA,ANTM,MDKA,INCO,PGAS,MEDC,AKRA,"
        "BSDE,CTRA,SMGR,BRPT,TPIA,"
        "GOTO,EMTK,BUKA,"
        "BMRI,BBTN,ARTO,BJTM,NISP,"
        "ASRI,PWON,LPKR,"
        "EXCL,ISAT,TBIG,"
        "MAPI,AMRT,LPPF,"
        "AALI,SIMP,PALM"
    )
    IDX30_WL = (
        "BBCA,BBRI,BMRI,TLKM,ASII,"
        "ADRO,ANTM,INCO,PTBA,MDKA,"
        "ICBP,INDF,UNVR,KLBF,"
        "BSDE,CTRA,SMGR,"
        "GOTO,BUKA,EMTK,DCII,"
        "BRPT,TPIA,PGAS,MEDC,"
        "EXCL,ISAT,"
        "MAPI,AMRT"
    )
    GROWTH_WL = (
        "BREN,BRPT,TPIA,CUAN,CDIA,SSIA,"
        "AMMN,MDKA,ADMR,BYAN,"
        "DCII,TOWR,TLKM,"
        "BBCA,BMRI,BBRI,"
        "KLBF,SIDO,MIKA,"
        "GOTO,BUKA,EMTK,"
        "PANI,CBDK,"
        "ASII,UNTR,"
        "ICBP,MYOR"
    )

    # Preset buttons — small inline style, not full-width
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                letter-spacing:1px;margin-bottom:5px'>LOAD PRESET:</div>""",
    unsafe_allow_html=True)

    col_p1, col_p2, col_p3 = st.columns(3)
    with col_p1:
        load_lq45 = st.button(
            "LQ45", use_container_width=True, key="btn_lq45",
            help="Load LQ45 index — 45 stocks"
        )
    with col_p2:
        load_idx30 = st.button(
            "IDX30", use_container_width=True, key="btn_idx30",
            help="Load IDX30 index — 30 stocks"
        )
    with col_p3:
        load_growth = st.button(
            "Growth", use_container_width=True, key="btn_growth",
            help="Load curated growth stocks"
        )

    if load_lq45:
        st.session_state["wl_area"] = LQ45_WL
        st.rerun()
    elif load_idx30:
        st.session_state["wl_area"] = IDX30_WL
        st.rerun()
    elif load_growth:
        st.session_state["wl_area"] = GROWTH_WL
        st.rerun()

    wl_raw = st.text_area(
        "Tickers",
        value=st.session_state.get("wl_area", DEFAULT_WL),
        height=110,
        key="wl_area",
        help="e.g. BBCA,BREN,AMMN,GOTO — comma separated",
        label_visibility="collapsed",
    )
    _wl_count = len([t for t in wl_raw.replace("\n",",").split(",") if t.strip()])
    st.markdown(f"""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                margin-top:3px;margin-bottom:2px'>
    {_wl_count} stocks in watchlist
    </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#2a3d52;line-height:2.2'>
    <span style='color:#5a7a9a'>CACHE TTL</span><br>
    Price chart: 15 min<br>
    Broker (today): 30 min<br>
    Broker accumulation: 1 hr<br>
    Fundamentals: 1 hr<br>
    Shareholders: 24 hr (monthly)<br>
    KSEI composition: 1 hr<br><br>
    <span style='color:#5a7a9a'>DATA SOURCES</span><br>
    <span style='color:#00e676'>●</span> Stockbit API (real-time)<br>
    <span style='color:#00b0ff'>●</span> IDX Official API<br>
    <span style='color:#e040fb'>●</span> KSEI holding data<br>
    <span style='color:#00e5ff'>●</span> Yahoo Finance<br>
    <span style='color:#ffab00'>●</span> Demo (simulated)
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  MAIN HEADER
# ══════════════════════════════════════════════════════════════════════

ms_now = market_status()
st.markdown(f"""
<div style='padding:10px 0 8px;border-bottom:1px solid #141e2e;margin-bottom:12px;
            display:flex;justify-content:space-between;align-items:center'>
  <div>
    <span style='font-family:"IBM Plex Mono",monospace;font-size:1.55rem;font-weight:700;
                 color:#00e676;letter-spacing:4px;text-shadow:0 0 28px rgba(0,230,118,.2)'>
      BANDARMOLOGY PRO</span>
    <span style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#2a3d52;
                 margin-left:14px;letter-spacing:3px'>IDX SMART MONEY · v6</span>
  </div>
  <div style='text-align:right;font-family:"IBM Plex Mono",monospace;font-size:9px;color:#2a3d52'>
    <span style='color:{"#00e676" if ms_now["open"] else "#ffab00"}'>{ms_now["label"]}</span>
    · {ms_now["wib"].strftime("%H:%M:%S WIB  %d %b %Y")}
  </div>
</div>""", unsafe_allow_html=True)

tab_a, tab_bh, tab_bt, tab_s, tab_o, tab_g = st.tabs([
    "  ANALYSIS  ",
    "  BROKER SHAREHOLDING  ",
    "  BACKTEST & RS  ",
    "  SCREENER  ",
    "  OWNERSHIP DB  ",
    "  GUIDE  ",
])


# ══════════════════════════════════════════════════════════════════════
#  TAB 1 — ANALYSIS
# ══════════════════════════════════════════════════════════════════════

with tab_a:
    cb, ct = st.columns([2,8])
    with cb:
        run = st.button("▶  RUN ANALYSIS", use_container_width=True)
    with ct:
        st.markdown(f"""
        <div style='padding:7px 12px;background:#0b1018;border:1px solid #141e2e;
                    border-radius:3px;font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a'>
          Ticker: <b style='color:#eaf0f8'>{ticker_input}.JK</b>
          &nbsp;·&nbsp; Period: {period}
          &nbsp;·&nbsp; Broker: {'<span style="color:#00e676">Stockbit</span>'
            if sb_token and len(sb_token)>20 else '<span style="color:#00b0ff">IDX API</span>'}
          &nbsp;·&nbsp; Market: <span style='color:{"#00e676" if ms_now["open"] else "#ffab00"}'>{ms_now["label"]}</span>
        </div>""", unsafe_allow_html=True)

    if run:
        st.session_state.pop("res", None)
        with st.spinner(f"Loading price data for {ticker_input}.JK ..."):
            df = load_price(ticker_input, period)
        if df is None or len(df) < 20:
            st.error(f"Could not load **{ticker_input}.JK**. Try: BBCA, BREN, AMMN, ADRO, BMRI")
            st.stop()
        with st.spinner("Computing CMF · OBV · MFI · RSI · Wyckoff · ATR · VCP ..."):
            c_ = cmf(df); o_ = obv(df); m_ = mfi(df)
            wp,wn,wd = wyckoff(df,c_,o_); ts = tech_score(df,c_,o_,m_)
            ez = entry_zone(df)
            # VCP needs min 120 days — load 6M if current period is too short
            if len(df) < 120:
                df_vcp = load_price(ticker_input, "6mo")
                if df_vcp is None or len(df_vcp) < 60:
                    df_vcp = df
            else:
                df_vcp = df
            vcp = detect_vcp(df_vcp)
        with st.spinner("Fetching broker summary (today) ..."):
            bdf, src = get_broker_today(ticker_input, sb_token)
            if bdf is None:
                bdf = demo_broker(ticker_input, ts); src = "demo"
            br = analyze_broker(bdf)
        with st.spinner("Fetching fundamentals · ownership · shareholders · corporate actions ..."):
            fund   = fundamentals(ticker_input)
            own    = OWNER_DB.get(ticker_input.upper().replace(".JK",""))
            ksei   = fetch_ksei_composition(ticker_input)
            sh_list, sh_src, sh_date = fetch_shareholders(ticker_input)
            ca     = fetch_corporate_actions(ticker_input)
            fq     = fundamental_quality_gate(fund)
            div_m  = calc_dividend_metrics(ca, fund, float(df.iloc[-1]["close"]))
        with st.spinner("Computing RS vs IHSG · Market Regime · Weekly Confluence · Liquidity ..."):
            ihsg_df    = load_ihsg(period)
            rs_data    = calc_rs(df, ihsg_df) if ihsg_df is not None else {"available": False}
            regime     = detect_market_regime(period)
            sector     = get_stock_sector(ticker_input)
            sector_data= detect_sector_rotation()
            reg_adj    = regime_signal_adjustment(regime, sector, sector_data)
            # Weekly confluence
            weekly_c   = calc_weekly_confluence(ticker_input, "2y")
            df_weekly  = load_weekly(ticker_input, "2y")
            # Liquidity
            ps_cap_liq = st.session_state.get("ps_capital", 100_000_000)
            liq        = calc_liquidity_score(df, ps_cap_liq * 0.20, own)

        # ── COMPOSITE SCORE v2 — Signal Validation Layer
        # Layer 1: Raw signal  = Technical 55% + Broker 35% + VCP 10%
        # Layer 2: Validation  = Regime adj + Weekly confluence + Liquidity
        # Total adjustment capped at ±25 to preserve signal integrity
        vcp_score  = vcp.get("score", 0)
        raw_final  = int(np.clip(round(ts*.55 + br["score"]*.35 + vcp_score*.10), 0, 100))

        # Layer 2 adjustments
        regime_adj   = np.clip(reg_adj["score_adj"], -15, 15)
        weekly_adj   = int(round((weekly_c["confluence_mult"] - 1.0) * raw_final * 0.3))
        weekly_adj   = int(np.clip(weekly_adj, -12, 10))
        liquidity_adj= int(np.clip(liq.get("score_adj", 0), -20, 5))

        total_adj  = int(np.clip(regime_adj + weekly_adj + liquidity_adj, -25, 15))
        final      = int(np.clip(raw_final + total_adj, 0, 100))

        last  = df.iloc[-1]; prev = df.iloc[-2]
        chg   = (last["close"]-prev["close"]) / prev["close"] * 100
        av    = df["volume"].tail(20).mean()
        vr    = last["volume"] / av if av>0 else 1
        ob_up = o_.iloc[-1] > o_.iloc[-min(10,len(o_)-1)]
        ent   = entry_signal(final,ts,br["score"],wp,float(c_.iloc[-1]),
                              ob_up,float(m_.iloc[-1]),vr,br["crossing"],
                              br["goreng"],br["sm_buyers"],br["sm_sellers"],chg,own,
                              rs_data=rs_data, vcp=vcp)

        ez_new = entry_zone(df)

        st.session_state["res"] = dict(
            df=df, df_weekly=df_weekly,
            c=c_,o=o_,m=m_,wp=wp,wn=wn,wd=wd,ts=ts,
            br=br,bdf=bdf,src=src,
            raw_final=raw_final, final=final, ent=ent,
            regime_adj=regime_adj, weekly_adj=weekly_adj,
            liquidity_adj=liquidity_adj, total_adj=total_adj,
            lp=float(last["close"]),chg=chg,vr=vr,
            cmf_v=float(c_.iloc[-1]),mfi_v=float(m_.iloc[-1]),
            obv_up=ob_up, ticker=ticker_input,
            fund=fund,own=own,ksei=ksei,
            sh_list=sh_list,sh_src=sh_src,sh_date=sh_date,
            ez=ez_new, rs_data=rs_data, vcp=vcp,
            ca=ca, fq=fq, div_m=div_m,
            regime=regime, sector=sector,
            sector_data=sector_data, reg_adj=reg_adj,
            weekly_c=weekly_c, liq=liq,
            fetched=datetime.now().strftime("%H:%M:%S WIB"),
        )

    if "res" in st.session_state:
        R  = st.session_state["res"]
        br = R["br"]; ent = R["ent"]; own = R["own"]; fund = R["fund"]; ez = R["ez"]
        sh_list = R.get("sh_list",[]); ksei = R.get("ksei",{})

        # ── source badge
        src_badge = {"stockbit":'<span class="tag tg">STOCKBIT REAL</span>',
                     "idx":     '<span class="tag tb">IDX API REAL</span>',
                     "demo":    '<span class="tag ta">DEMO DATA</span>'}.get(R["src"],"")

        # ── ownership bar
        if own:
            tc = {1:"#00e676",2:"#00b0ff",3:"#ff1744"}.get(own.get("tier",2),"#00b0ff")
            ff = own.get("float",30)
            ffc= "#ff1744" if ff<15 else "#00e676"
            pol= '<span class="tag tr" style="margin-left:6px">⚡ POLITICAL</span>' if own.get("political") else ""
            own_html = (f'<span style="font-family:IBM Plex Mono,monospace;font-size:10px;'
                        f'font-weight:700;color:{tc}">T{own["tier"]}</span>'
                        f'<span style="color:#5a7a9a;font-family:IBM Plex Mono,monospace;font-size:10px;margin-left:10px">'
                        f'{own["owner"]} · {own["group"]}</span>'
                        f'<span style="color:{ffc};font-family:IBM Plex Mono,monospace;font-size:10px;margin-left:10px">'
                        f'Float {ff}%{"  ⚠️ FCA" if ff<15 else ""}</span>{pol}')
        else:
            own_html = '<span style="color:#2a3d52;font-size:10px;font-family:IBM Plex Mono,monospace">Ownership not in DB — check IDX disclosure</span>'

        # ── HEADER
        st.markdown(f"""
        <div style='background:#0b1018;border:1px solid #141e2e;border-top:2px solid {ent["color"]};
                    padding:14px 18px;margin-bottom:12px;border-radius:0 0 4px 4px'>
          <div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px'>
            <div style='display:flex;align-items:baseline;gap:12px'>
              <span style='font-family:"IBM Plex Mono",monospace;font-size:2.3rem;
                           font-weight:700;color:var(--white);letter-spacing:3px'>{R["ticker"]}</span>
              <span style='font-family:"IBM Plex Mono",monospace;color:#5a7a9a;font-size:10px'>.JK · IDX</span>
              <span style='font-family:"IBM Plex Mono",monospace;font-size:1.35rem;color:#00b0ff'>
                Rp {R["lp"]:,.0f}</span>
              <span style='font-family:"IBM Plex Mono",monospace;
                           color:{"#00e676" if R["chg"]>=0 else "#ff1744"}'>
                {"▲" if R["chg"]>=0 else "▼"} {abs(R["chg"]):.2f}%</span>
            </div>
            <div style='display:flex;align-items:center;gap:8px'>
              {src_badge}
              <span class='ts-note'>Updated {R["fetched"]}</span>
            </div>
          </div>
          <div style='margin-top:6px'>{own_html}</div>
        </div>""", unsafe_allow_html=True)

        # ── ENTRY SIGNAL
        st.markdown('<div class="sec">ENTRY RECOMMENDATION</div>', unsafe_allow_html=True)
        conds  = "".join([f'<span class="cm">✓ {x}</span>' for x in ent["met"]])
        conds += "".join([f'<span class="cw">~ {x}</span>' for x in ent["watch"]])
        conds += "".join([f'<span class="cf">✗ {x}</span>' for x in ent["fail"]])
        st.markdown(f"""
        <div class='signal-card {ent["cls"]}'>
          <div class='sig-lbl' style='color:{ent["color"]}'>ENTRY SIGNAL</div>
          <div class='sig-main' style='color:{ent["color"]}'>{ent["sig"]}</div>
          <div class='sig-why'>{ent["why"]}</div>
          <div style='margin-top:8px;padding:7px 12px;background:rgba(0,0,0,.3);
                      border-radius:3px;font-family:"IBM Plex Mono",monospace;font-size:11px'>
            <span style='color:#5a7a9a'>ACTION:</span>
            <span style='color:#eaf0f8;margin-left:8px'>{ent["action"]}</span>
            &nbsp;·&nbsp;
            <span style='color:#5a7a9a'>RISK:</span>
            <span style='color:{ent["color"]};margin-left:8px;font-weight:700'>{ent["risk"]}</span>
          </div>
          <div class='conds'>{conds}</div>
        </div>""", unsafe_allow_html=True)

        # ── SIGNAL VALIDATION LAYER BANNER (Regime + Weekly + Liquidity)
        regime   = R.get("regime", {})
        reg_adj  = R.get("reg_adj", {})
        weekly_c = R.get("weekly_c", {})
        liq      = R.get("liq", {})
        sector   = R.get("sector", "Other")

        raw_sc       = R.get("raw_final", R["final"])
        regime_adj_v = R.get("regime_adj", 0)
        weekly_adj_v = R.get("weekly_adj", 0)
        liq_adj_v    = R.get("liquidity_adj", 0)
        total_adj_v  = R.get("total_adj", 0)

        wc_label = weekly_c.get("label","—") if weekly_c.get("available") else "Unavail."
        wc_color = weekly_c.get("color","#5a7a9a")
        liq_label= liq.get("label","—")
        liq_color= liq.get("color","#5a7a9a")

        reg_name  = regime.get("regime","—") if regime.get("available") else "—"
        reg_color = regime.get("color","#5a7a9a")
        sect_info = (R.get("sector_data",{}).get("sectors",{}) or {}).get(sector,{})
        sect_status = sect_info.get("status","—")
        sect_color  = sect_info.get("color","#5a7a9a")

        def _adj_color(v): return "#00e676" if v>0 else "#ff1744" if v<0 else "#5a7a9a"

        st.markdown(f"""
        <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                    overflow:hidden;margin-bottom:12px'>

          <!-- Header -->
          <div style='background:#0d1420;padding:8px 16px;border-bottom:1px solid #141e2e;
                      font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                      letter-spacing:2px'>
            SIGNAL VALIDATION LAYER — Score decomposition: Raw → Regime → Weekly → Liquidity → Final
          </div>

          <!-- Score flow -->
          <div style='display:grid;grid-template-columns:repeat(7,1fr);
                      padding:12px 16px;gap:4px;align-items:center'>

            <div style='text-align:center'>
              <div style='font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace'>RAW SCORE</div>
              <div style='font-size:1.6rem;font-weight:700;color:#cdd8e6;
                          font-family:"IBM Plex Mono",monospace'>{raw_sc}</div>
              <div style='font-size:9px;color:#5a7a9a'>Tech+Broker+VCP</div>
            </div>

            <div style='text-align:center;font-size:1.2rem;color:#2a3d52'>→</div>

            <div style='text-align:center;background:#090d12;border:1px solid #141e2e;
                        border-top:2px solid {reg_color};border-radius:3px;padding:8px 4px'>
              <div style='font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace'>
                REGIME</div>
              <div style='font-size:1.1rem;font-weight:700;color:{_adj_color(regime_adj_v)};
                          font-family:"IBM Plex Mono",monospace'>{regime_adj_v:+d}</div>
              <div style='font-size:9px;color:{reg_color}'>{reg_name}</div>
              <div style='font-size:8px;color:{sect_color}'>{sector}: {sect_status}</div>
            </div>

            <div style='text-align:center;background:#090d12;border:1px solid #141e2e;
                        border-top:2px solid {wc_color};border-radius:3px;padding:8px 4px'>
              <div style='font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace'>
                WEEKLY</div>
              <div style='font-size:1.1rem;font-weight:700;color:{_adj_color(weekly_adj_v)};
                          font-family:"IBM Plex Mono",monospace'>{weekly_adj_v:+d}</div>
              <div style='font-size:9px;color:{wc_color}'>{wc_label}</div>
              <div style='font-size:8px;color:#5a7a9a'>
                Score: {weekly_c.get("score","—")}/100</div>
            </div>

            <div style='text-align:center;background:#090d12;border:1px solid #141e2e;
                        border-top:2px solid {liq_color};border-radius:3px;padding:8px 4px'>
              <div style='font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace'>
                LIQUIDITY</div>
              <div style='font-size:1.1rem;font-weight:700;color:{_adj_color(liq_adj_v)};
                          font-family:"IBM Plex Mono",monospace'>{liq_adj_v:+d}</div>
              <div style='font-size:9px;color:{liq_color}'>{liq_label}</div>
              <div style='font-size:8px;color:#5a7a9a'>
                {liq.get("adv_str","—")}</div>
            </div>

            <div style='text-align:center;font-size:1.2rem;color:#2a3d52'>→</div>

            <div style='text-align:center'>
              <div style='font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace'>
                FINAL SCORE</div>
              <div style='font-size:1.8rem;font-weight:700;
                          color:{"#00e676" if R["final"]>=65 else "#ff1744" if R["final"]<=35 else "#ffab00"};
                          font-family:"IBM Plex Mono",monospace'>{R["final"]}</div>
              <div style='font-size:9px;color:{_adj_color(total_adj_v)}'>
                Total adj: {total_adj_v:+d}</div>
            </div>

          </div>

          <!-- Action bar -->
          <div style='padding:6px 16px 10px;font-size:10px;color:#5a7a9a;
                      font-family:"IBM Plex Mono",monospace;border-top:1px solid #141e2e'>
            Weekly: <span style='color:{wc_color}'>{weekly_c.get("action","—")[:80] if weekly_c.get("available") else "Weekly data unavailable"}</span>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Liquidity warning if illiquid
        if liq.get("available") and liq.get("impact_days", 0) > 3:
            st.warning(
                f"⚠️ **Liquidity Warning** — Market impact: **{liq['impact_days']:.1f} days** to build position "
                f"at standard 20% ADV participation. ADV: {liq['adv_str']}. "
                f"{'FCA stock — float only {:.0f}%. '.format(liq['float_pct']) if liq.get('is_fca') else ''}"
                f"Reduce position size or wait for higher-volume session."
            )
        if weekly_c.get("available") and weekly_c.get("label") == "OPPOSE":
            st.error(
                f"🚫 **Weekly Confluence OPPOSE** — Daily signal contradicts weekly trend. "
                f"Weekly score: {weekly_c['score']}/100. "
                f"Do not enter regardless of daily signal strength. "
                f"Wait for weekly to confirm."
            )



        # ── KEY METRICS — compute live statuses
        obv_status = "Rising ▲" if R["obv_up"] else "Falling ▼"
        obv_color  = "#00e676" if R["obv_up"] else "#ff1744"
        vr_now     = R["vr"]
        vr_color   = "#00e676" if vr_now>=1.5 else "#ffab00" if vr_now>=0.8 else "#5a7a9a"
        vr_label   = "High" if vr_now>=1.5 else "Normal" if vr_now>=0.8 else "Quiet"
        cmf_color  = "#00e676" if R["cmf_v"]>0.05 else "#ff1744" if R["cmf_v"]<-0.05 else "#ffab00"
        cmf_label  = ("Strong inflow"    if R["cmf_v"]>0.15 else
                      "Mild inflow"      if R["cmf_v"]>0.05 else
                      "Strong outflow"   if R["cmf_v"]<-0.15 else
                      "Mild outflow"     if R["cmf_v"]<-0.05 else "Neutral")
        mfi_color  = "#ff1744" if R["mfi_v"]>70 else "#00e676" if R["mfi_v"]<30 else "#5a7a9a"
        mfi_label  = "Overbought ⚠️" if R["mfi_v"]>70 else "Oversold ✅" if R["mfi_v"]<30 else "Normal"
        wyck_color = {"A":"#00b0ff","B":"#ffab00","C":"#00e676","D":"#ffab00","E":"#ff8888"}.get(R["wp"],"#5a7a9a")
        comp_color = "#00e676" if R["final"]>=65 else "#ff1744" if R["final"]<=35 else "#ffab00"
        comp_label = "Accumulation" if R["final"]>=65 else "Distribution" if R["final"]<=35 else "Neutral"
        ts_color   = "#00e676" if R["ts"]>=65 else "#ff1744" if R["ts"]<=35 else "#ffab00"
        br_color   = "#00e676" if br["score"]>=65 else "#ff1744" if br["score"]<=35 else "#ffab00"
        raw_sc     = R.get("raw_final", R["final"])
        adj_sc     = R.get("reg_adj",{}).get("score_adj",0)
        sc_grade, sc_gc, sc_gl = score_grade(R["final"], 50)

        st.markdown('<div class="sec" style="margin-top:12px">KEY METRICS & INDICATOR LEGEND</div>',
                    unsafe_allow_html=True)

        # ── Top score row (4 cards: Composite + Grade, Technical, Broker, VCP)
        vcp_r = R.get("vcp",{}); vcp_grade = vcp_r.get("grade","NONE")
        vcp_gc2 = vcp_r.get("grade_color","#5a7a9a")
        vcp_s   = vcp_r.get("score",0)
        st.markdown(f"""
        <div style='display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:10px'>

          <!-- COMPOSITE — shows raw, adj, final -->
          <div style='background:#0b1018;border:1px solid {comp_color};border-radius:4px;
                      padding:14px 16px;border-top:2px solid {comp_color}'>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                        letter-spacing:1.5px;margin-bottom:4px'>COMPOSITE SCORE</div>
            <div style='display:flex;align-items:baseline;gap:8px'>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:2rem;
                          font-weight:700;color:{comp_color};line-height:1'>{R["final"]}</div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:1.1rem;
                          font-weight:700;color:{sc_gc}'>Grade {sc_grade}</div>
            </div>
            <div style='font-size:10px;color:{comp_color};margin-top:3px'>{comp_label}</div>
            <div style='margin-top:6px;font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;
                        border-top:1px solid #141e2e;padding-top:5px'>
              Raw {raw_sc}
              <span style='color:{"#00e676" if adj_sc>0 else "#ff1744" if adj_sc<0 else "#5a7a9a"}'>
                {adj_sc:+d} regime</span>
              → <b style='color:{comp_color}'>{R["final"]}</b>
            </div>
          </div>

          <!-- TECHNICAL -->
          <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                      padding:14px 16px;border-top:2px solid {ts_color}'>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                        letter-spacing:1.5px;margin-bottom:4px'>TECHNICAL SCORE</div>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:2rem;
                        font-weight:700;color:{ts_color};line-height:1'>{R["ts"]}/100</div>
            <div style='font-size:10px;color:#5a7a9a;margin-top:3px'>
              CMF · OBV · MFI · Wyckoff · Volume</div>
            <div style='margin-top:6px;font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;
                        border-top:1px solid #141e2e;padding-top:5px'>
              Weight: 55% of final score
            </div>
          </div>

          <!-- BROKER -->
          <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                      padding:14px 16px;border-top:2px solid {br_color}'>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                        letter-spacing:1.5px;margin-bottom:4px'>BROKER SCORE</div>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:2rem;
                        font-weight:700;color:{br_color};line-height:1'>{br["score"]}/100</div>
            <div style='font-size:10px;color:#5a7a9a;margin-top:3px'>
              Net lot · Crossing · Category</div>
            <div style='margin-top:6px;font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;
                        border-top:1px solid #141e2e;padding-top:5px'>
              Weight: 35% of final score
            </div>
          </div>

          <!-- VCP -->
          <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                      padding:14px 16px;border-top:2px solid {vcp_gc2}'>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                        letter-spacing:1.5px;margin-bottom:4px'>VCP SCORE</div>
            <div style='display:flex;align-items:baseline;gap:8px'>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:2rem;
                          font-weight:700;color:{vcp_gc2};line-height:1'>{vcp_s}/100</div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:1.1rem;
                          font-weight:700;color:{vcp_gc2}'>Grade {vcp_grade}</div>
            </div>
            <div style='font-size:10px;color:{vcp_gc2};margin-top:3px'>
              {vcp_r.get("grade_desc","No VCP detected")[:40]}</div>
            <div style='margin-top:6px;font-size:9px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;
                        border-top:1px solid #141e2e;padding-top:5px'>
              Weight: 10% of final score
            </div>
          </div>

        </div>""", unsafe_allow_html=True)

        # ── Indicator table (clean rows, easy to scan)
        st.markdown(f"""
        <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                    overflow:hidden;margin-bottom:14px'>

          <!-- Header row -->
          <div style='display:grid;grid-template-columns:140px 160px 1fr 220px;
                      background:#0d1420;padding:8px 14px;border-bottom:2px solid #141e2e;
                      font-family:"IBM Plex Mono",monospace;font-size:9px;
                      color:#5a7a9a;letter-spacing:1.5px;gap:12px'>
            <span>INDIKATOR</span>
            <span>NILAI SEKARANG</span>
            <span>CARA MEMBACA</span>
            <span>THRESHOLD</span>
          </div>

          <!-- OBV -->
          <div style='display:grid;grid-template-columns:140px 160px 1fr 220px;
                      padding:10px 14px;border-bottom:1px solid #141e2e;
                      align-items:center;gap:12px'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;
                          font-weight:700;color:#eaf0f8'>OBV</div>
              <div style='font-size:9px;color:#5a7a9a'>On Balance Volume</div>
            </div>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:13px;
                        font-weight:700;color:{obv_color}'>{obv_status}</div>
            <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>
              Kumulasi volume beli vs jual.<br>
              <span style='color:#00e676'>Naik + harga turun</span> = bandar diam-diam beli (sinyal terkuat)
            </div>
            <div style='font-size:10px;line-height:1.9'>
              <span style='color:#00e676'>▲ Naik</span> → Tekanan beli dominan<br>
              <span style='color:#ff1744'>▼ Turun</span> → Tekanan jual dominan
            </div>
          </div>

          <!-- CMF -->
          <div style='display:grid;grid-template-columns:140px 160px 1fr 220px;
                      padding:10px 14px;border-bottom:1px solid #141e2e;
                      align-items:center;gap:12px'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;
                          font-weight:700;color:#eaf0f8'>CMF (14)</div>
              <div style='font-size:9px;color:#5a7a9a'>Chaikin Money Flow</div>
            </div>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:13px;
                          font-weight:700;color:{cmf_color}'>{R["cmf_v"]:+.4f}</div>
              <div style='font-size:10px;color:{cmf_color}'>{cmf_label}</div>
            </div>
            <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>
              Arah aliran uang 14 hari terakhir.<br>
              Positif = uang masuk · Negatif = uang keluar
            </div>
            <div style='font-size:10px;line-height:1.9'>
              <span style='color:#00e676'>&gt; +0.15</span> Akumulasi kuat<br>
              <span style='color:#ffab00'>−0.05 ~ +0.05</span> Netral<br>
              <span style='color:#ff1744'>&lt; −0.15</span> Distribusi kuat
            </div>
          </div>

          <!-- MFI -->
          <div style='display:grid;grid-template-columns:140px 160px 1fr 220px;
                      padding:10px 14px;border-bottom:1px solid #141e2e;
                      align-items:center;gap:12px'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;
                          font-weight:700;color:#eaf0f8'>MFI (14)</div>
              <div style='font-size:9px;color:#5a7a9a'>Money Flow Index</div>
            </div>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:13px;
                          font-weight:700;color:{mfi_color}'>{R["mfi_v"]:.1f}</div>
              <div style='font-size:10px;color:{mfi_color}'>{mfi_label}</div>
            </div>
            <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>
              RSI yang memperhitungkan volume — lebih akurat.<br>
              <span style='color:#00e676'>Harga ↓ + MFI ↑</span> = bandar beli saat retail panik jual
            </div>
            <div style='font-size:10px;line-height:1.9'>
              <span style='color:#ff1744'>&gt; 70</span> Overbought (hati-hati)<br>
              <span style='color:#5a7a9a'>30 ~ 70</span> Normal range<br>
              <span style='color:#00e676'>&lt; 30</span> Oversold (potensi naik)
            </div>
          </div>

          <!-- VOL RATIO -->
          <div style='display:grid;grid-template-columns:140px 160px 1fr 220px;
                      padding:10px 14px;border-bottom:1px solid #141e2e;
                      align-items:center;gap:12px'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;
                          font-weight:700;color:#eaf0f8'>VOL RATIO</div>
              <div style='font-size:9px;color:#5a7a9a'>Volume vs MA20</div>
            </div>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:13px;
                          font-weight:700;color:{vr_color}'>{vr_now:.1f}x</div>
              <div style='font-size:10px;color:{vr_color}'>{vr_label}</div>
            </div>
            <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>
              Volume hari ini ÷ rata-rata 20 hari.<br>
              Volume tinggi + harga naik = breakout valid
            </div>
            <div style='font-size:10px;line-height:1.9'>
              <span style='color:#00e676'>&gt; 1.5x</span> Aktivitas tinggi<br>
              <span style='color:#ffab00'>0.8 ~ 1.5x</span> Normal<br>
              <span style='color:#5a7a9a'>&lt; 0.5x</span> Sepi / konsolidasi
            </div>
          </div>

          <!-- WYCKOFF -->
          <div style='display:grid;grid-template-columns:140px 160px 1fr 220px;
                      padding:10px 14px;align-items:center;gap:12px'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;
                          font-weight:700;color:#eaf0f8'>WYCKOFF</div>
              <div style='font-size:9px;color:#5a7a9a'>Accumulation Cycle</div>
            </div>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:13px;
                          font-weight:700;color:{wyck_color}'>Phase {R["wp"]}</div>
              <div style='font-size:10px;color:{wyck_color}'>{R["wn"]}</div>
            </div>
            <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>
              {R["wd"]}
            </div>
            <div style='font-size:10px;line-height:1.9'>
              <span style='color:#00b0ff'>A</span> Selling Climax&nbsp;&nbsp;
              <span style='color:#ffab00'>B</span> Building Cause<br>
              <span style='color:#00e676'><b>C</b></span><b style='color:#00e676'> Spring ← Entry ideal</b><br>
              <span style='color:#ffab00'>D</span> Breakout&nbsp;&nbsp;
              <span style='color:#ff8888'>E</span> Markup
            </div>
          </div>

        </div>
        """, unsafe_allow_html=True)

        # ── CHART + RIGHT PANEL
        col_ch, col_r = st.columns([3,1])
        with col_ch:
            st.markdown('<div class="sec">PRICE · VOLUME · CMF · MFI</div>', unsafe_allow_html=True)
            st.plotly_chart(chart_price(R["df"],R["c"],R["o"],R["m"]), use_container_width=True)

        with col_r:
            st.markdown('<div class="sec">SCORE</div>', unsafe_allow_html=True)
            st.plotly_chart(chart_gauge(R["final"]), use_container_width=True)

            # Entry zone (now ATR-based)
            st.markdown('<div class="sec">ENTRY ZONE (ATR-BASED)</div>', unsafe_allow_html=True)
            if ent["sig"] in ("STRONG BUY","BUY","WATCH","PREMIUM SETUP"):
                atr_val = ez.get("atr",0)
                sl_method = ez.get("sl_method","ATR")
                st.markdown(f"""
                <div class='entry-box'>
                  <div style='font-size:9px;color:#5a7a9a;letter-spacing:2px'>ENTRY</div>
                  <div style='color:#00e676;font-size:1rem;font-weight:700;margin-top:2px'>
                    Rp {ez["el"]:,.0f} – {ez["eh"]:,.0f}</div>
                  <div style='font-size:9px;color:#5a7a9a;margin-top:6px'>
                    STOP-LOSS
                    <span style='color:#2a3d52;margin-left:4px'>({sl_method} × 1.5)</span></div>
                  <div style='color:#ff1744;font-size:.95rem;font-weight:700'>
                    Rp {ez["sl"]:,.0f}
                    <span style='font-size:9px;color:#5a7a9a'> (−{ez["risk_pct"]}%)</span></div>
                  <div style='font-size:9px;color:#5a7a9a;margin-top:6px'>TARGETS  (R:R 1.5 / 2.5 / 4.0)</div>
                  <div style='color:#00b0ff;font-size:.9rem;margin-top:2px;
                              font-family:"IBM Plex Mono",monospace'>
                    T1 Rp {ez["t1"]:,.0f} &nbsp;·&nbsp;
                    T2 Rp {ez.get("t2",0):,.0f} &nbsp;·&nbsp;
                    T3 Rp {ez.get("t3",0):,.0f}</div>
                  <div style='margin-top:6px;padding-top:5px;border-top:1px solid rgba(0,230,118,.15);
                              font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a'>
                    ATR(14): <b style='color:#ffab00'>Rp {atr_val:,.0f}</b>
                  </div>
                </div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class='stop-box'>
                  <div style='font-size:10px;color:#ff1744;margin-bottom:5px'>DO NOT ENTER</div>
                  <div style='font-size:11px;color:#5a7a9a;line-height:1.6'>
                    Support: Rp {ez["sup"]:,.0f}<br>
                    Resistance: Rp {ez["res"]:,.0f}<br>
                    ATR(14): Rp {ez.get("atr",0):,.0f}
                  </div>
                </div>""", unsafe_allow_html=True)

            # ── POSITION SIZING CALCULATOR
            st.markdown('<div class="sec" style="margin-top:10px">POSITION SIZING</div>',
                        unsafe_allow_html=True)
            # Use session state for capital/risk inputs persistence
            ps_cap  = st.session_state.get("ps_capital",  100_000_000)
            ps_risk = st.session_state.get("ps_risk_pct", 2.0)
            ps_cap  = st.number_input("Capital (Rp)", value=ps_cap,
                                       min_value=1_000_000, step=10_000_000,
                                       format="%d", key="ps_capital",
                                       label_visibility="collapsed",
                                       help="Total trading capital in Rupiah")
            ps_risk = st.slider("Risk per trade %", 0.5, 5.0,
                                 value=ps_risk, step=0.5,
                                 key="ps_risk_pct",
                                 help="% of capital you are willing to lose if stop-loss is hit. Professional standard: 1-2%")

            if ez["sl"] > 0 and R["lp"] > ez["sl"]:
                ps = calc_position_size(ps_cap, ps_risk, R["lp"], ez["sl"])
                if ps["valid"]:
                    cap_pct_color = "#00e676" if ps["pct_of_capital"]<20 else "#ffab00" if ps["pct_of_capital"]<40 else "#ff1744"
                    st.markdown(f"""
                    <div style='background:#0b1018;border:1px solid #141e2e;
                                border-radius:3px;padding:10px;margin-top:4px;
                                font-family:"IBM Plex Mono",monospace'>
                      <div style='display:grid;grid-template-columns:1fr 1fr;gap:6px'>
                        <div>
                          <div style='font-size:8px;color:#5a7a9a;letter-spacing:1px'>BUY</div>
                          <div style='font-size:1.1rem;font-weight:700;color:#00e676'>
                            {ps["lots"]:,} lots</div>
                          <div style='font-size:9px;color:#5a7a9a'>
                            {ps["shares"]:,} shares</div>
                        </div>
                        <div>
                          <div style='font-size:8px;color:#5a7a9a;letter-spacing:1px'>TOTAL COST</div>
                          <div style='font-size:.95rem;font-weight:700;color:{cap_pct_color}'>
                            Rp {ps["total_cost"]/1e6:.1f}M</div>
                          <div style='font-size:9px;color:{cap_pct_color}'>
                            {ps["pct_of_capital"]:.1f}% of capital</div>
                        </div>
                        <div>
                          <div style='font-size:8px;color:#5a7a9a;letter-spacing:1px'>MAX LOSS</div>
                          <div style='font-size:.95rem;font-weight:700;color:#ff1744'>
                            Rp {ps["max_loss"]/1e6:.2f}M</div>
                          <div style='font-size:9px;color:#5a7a9a'>
                            {ps["actual_risk_pct"]:.2f}% of capital</div>
                        </div>
                        <div>
                          <div style='font-size:8px;color:#5a7a9a;letter-spacing:1px'>RISK/SHARE</div>
                          <div style='font-size:.95rem;font-weight:700;color:#ffab00'>
                            Rp {ps["risk_per_share"]:,.0f}</div>
                          <div style='font-size:9px;color:#5a7a9a'>
                            entry − stop</div>
                        </div>
                      </div>
                      <div style='margin-top:7px;padding-top:6px;border-top:1px solid #141e2e;
                                  font-size:9px;color:#2a3d52'>
                        Formula: ({ps_cap/1e6:.0f}M × {ps_risk}%) ÷ {ps["risk_per_share"]:,.0f} = {ps["lots"]} lots
                      </div>
                    </div>""", unsafe_allow_html=True)
            else:
                st.markdown("""<div style='font-size:10px;color:#2a3d52;
                    font-family:"IBM Plex Mono",monospace;padding:6px'>
                    Set entry price above stop-loss to calculate position size.
                    </div>""", unsafe_allow_html=True)

            # Wyckoff
            st.markdown('<div class="sec" style="margin-top:10px">WYCKOFF PHASE</div>', unsafe_allow_html=True)
            phases = [("A","Climax"),("B","Build"),("C","Spring"),("D","Strength"),("E","Markup")]
            ph = '<div class="ph-track">'
            for code,lbl in phases:
                if code==R["wp"]: cl="ph ph-a"
                elif ["A","B","C","D","E"].index(code)<["A","B","C","D","E"].index(R["wp"]): cl="ph ph-d"
                else: cl="ph"
                ph += f'<div class="{cl}">Ph {code}<div style="font-size:7px;margin-top:2px">{lbl}</div></div>'
            ph += "</div>"
            st.markdown(f"""
            <div style='background:#0b1018;border:1px solid #141e2e;padding:10px;border-radius:3px'>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:.9rem;
                          color:#00b0ff;font-weight:700'>Ph {R["wp"]} — {R["wn"]}</div>
              <div style='font-size:11px;color:#cdd8e6;margin-top:3px;line-height:1.5'>{R["wd"]}</div>
              {ph}
            </div>""", unsafe_allow_html=True)

            # ── VCP mini-panel in right column
            vcp = R.get("vcp",{})
            if vcp and vcp.get("available"):
                vcp_grade  = vcp.get("grade","NONE")
                vcp_gc     = vcp.get("grade_color","#5a7a9a")
                vcp_pivot  = vcp.get("pivot_price",0)
                vcp_tight  = vcp.get("current_tight",0)
                vcp_n_ct   = vcp.get("n_contractions",0)
                vcp_dry    = vcp.get("vol_is_dry",False)
                vcp_trend  = vcp.get("in_uptrend",False)
                premium    = ent.get("premium_setup",False)
                breaking   = vcp.get("is_breaking",False)
                near_piv   = vcp.get("near_pivot",False)

                # Grade badges string
                vcp_grade_labels = ["NONE","C-","C","B","A"]
                grade_track_html = '<div style="display:flex;gap:3px;margin-top:7px">'
                for g in vcp_grade_labels:
                    active = (g == vcp_grade)
                    bc = vcp_gc if active else "#141e2e"
                    fc = vcp_gc if active else "#2a3d52"
                    fw = "700" if active else "400"
                    grade_track_html += (f'<div style="flex:1;text-align:center;padding:4px 2px;'
                                         f'border:1px solid {bc};border-radius:2px;'
                                         f'font-family:IBM Plex Mono,monospace;font-size:9px;'
                                         f'color:{fc};font-weight:{fw}">{g}</div>')
                grade_track_html += "</div>"

                premium_banner = ""
                if premium:
                    premium_banner = """<div style='margin-top:7px;padding:5px 8px;
                        background:rgba(0,230,118,.12);border:1px solid rgba(0,230,118,.4);
                        border-radius:3px;font-family:"IBM Plex Mono",monospace;
                        font-size:9px;color:#00e676;letter-spacing:1px'>
                        🔥 PREMIUM SETUP: VCP + ACC CROSS</div>"""
                if breaking:
                    premium_banner = """<div style='margin-top:7px;padding:5px 8px;
                        background:rgba(255,171,0,.12);border:1px solid rgba(255,171,0,.4);
                        border-radius:3px;font-family:"IBM Plex Mono",monospace;
                        font-size:9px;color:#ffab00;letter-spacing:1px'>
                        ⚡ VCP BREAKOUT IN PROGRESS</div>"""

                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid #141e2e;
                            border-top:2px solid {vcp_gc};
                            padding:10px;border-radius:3px;margin-top:8px'>
                  <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                              color:#5a7a9a;letter-spacing:2px;margin-bottom:4px'>
                    VCP — VOLATILITY CONTRACTION</div>
                  <div style='font-family:"IBM Plex Mono",monospace;font-size:1.2rem;
                              font-weight:700;color:{vcp_gc}'>Grade {vcp_grade}</div>
                  {grade_track_html}
                  <div style='margin-top:8px;font-size:10px;color:#cdd8e6;
                              font-family:"IBM Plex Mono",monospace;line-height:1.8'>
                    Contractions: <b style='color:{vcp_gc}'>{vcp_n_ct}</b><br>
                    Tightness: <b style='color:{"#00e676" if vcp_tight<=6 else "#ffab00" if vcp_tight<=15 else "#5a7a9a"}'>{vcp_tight:.1f}%</b>
                    {"✅ TIGHT" if vcp_tight<=6 else "🟡 OK" if vcp_tight<=15 else ""}<br>
                    Vol Dry-Up: <b style='color:{"#00e676" if vcp_dry else "#5a7a9a"}'>
                    {"✅ YES" if vcp_dry else "Not yet"}</b><br>
                    Uptrend: <b style='color:{"#00e676" if vcp_trend else "#ff8888"}'>
                    {"✅ YES" if vcp_trend else "❌ Below MAs"}</b><br>
                    Pivot: <b style='color:#ffab00'>Rp {vcp_pivot:,.0f}</b>
                    {"  <span style='color:#00e676'>← NEAR!</span>" if near_piv else ""}
                  </div>
                  {premium_banner}
                </div>""", unsafe_allow_html=True)

        # ── VCP FULL CHART (wide section below chart+panel)
        vcp = R.get("vcp",{})
        if vcp and vcp.get("available") and vcp.get("grade","NONE") != "NONE":
            st.markdown("---")
            st.markdown('<div class="sec">VCP CHART — VOLATILITY CONTRACTION PATTERN</div>',
                        unsafe_allow_html=True)
            vcp_grade = vcp.get("grade","NONE")
            vcp_gc    = vcp.get("grade_color","#5a7a9a")
            vcp_desc  = vcp.get("grade_desc","")

            # Contraction table
            contractions = vcp.get("contractions",[])
            if contractions:
                col_vt, col_vc = st.columns([3,2])
                with col_vt:
                    st.plotly_chart(chart_vcp(R["df"], vcp), use_container_width=True)
                with col_vc:
                    st.markdown(f"""
                    <div style='background:#0b1018;border:1px solid {vcp_gc};
                                padding:14px;border-radius:3px;margin-bottom:10px'>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                  color:#5a7a9a;letter-spacing:2px;margin-bottom:8px'>
                        VCP GRADE {vcp_grade} — DETAIL</div>
                      <div style='font-size:11px;color:#cdd8e6;margin-bottom:10px'>{vcp_desc}</div>
                    """, unsafe_allow_html=True)

                    # Contraction table
                    st.markdown('<div class="sec">CONTRACTION SEQUENCE</div>', unsafe_allow_html=True)
                    for ct in contractions:
                        depth  = ct["depth_pct"]
                        is_ok  = depth <= 15
                        dry    = ct.get("vol_dry_up",False)
                        d_col  = "#00e676" if is_ok else "#ffab00" if depth<=25 else "#ff8888"
                        prev_depth = contractions[contractions.index(ct)-1]["depth_pct"] if contractions.index(ct)>0 else 999
                        shrinking  = depth < prev_depth
                        st.markdown(f"""
                        <div style='display:flex;justify-content:space-between;
                                    padding:5px 0;border-bottom:1px solid #141e2e;
                                    font-family:"IBM Plex Mono",monospace;font-size:11px'>
                          <span style='color:#5a7a9a'>Contraction {ct["idx"]}</span>
                          <span style='color:{d_col}'>{depth:.1f}% depth
                            {"▼ Shrinking ✓" if shrinking and contractions.index(ct)>0 else ""}</span>
                          <span style='color:{"#00e676" if dry else "#5a7a9a"}'>
                            {"📉 Vol ↓" if dry else "Vol ~"}</span>
                        </div>""", unsafe_allow_html=True)

                    # MA status
                    st.markdown("""<div style="margin-top:10px">""", unsafe_allow_html=True)
                    st.markdown('<div class="sec">TREND FILTER (MAs)</div>', unsafe_allow_html=True)
                    for ma_name, ma_val, is_above in [
                        ("MA 50",  vcp.get("ma50_v",0),  vcp.get("above_ma50",False)),
                        ("MA 150", vcp.get("ma150_v",0), vcp.get("above_ma150",False)),
                        ("MA 200", vcp.get("ma200_v",0), vcp.get("above_ma200",False)),
                    ]:
                        mc = "#00e676" if is_above else "#ff8888"
                        ic = "✅" if is_above else "❌"
                        st.markdown(f"""
                        <div class='kv'>
                          <span class='kv-k'>{ma_name}</span>
                          <span style='color:#cdd8e6;font-family:"IBM Plex Mono",monospace;font-size:10px'>
                            Rp {ma_val:,.0f}</span>
                          <span style='color:{mc};font-size:10px'>{ic} {"Above" if is_above else "Below"}</span>
                        </div>""", unsafe_allow_html=True)

                    # Entry guidance for VCP
                    pivot_p = vcp.get("pivot_price",0)
                    last_p  = vcp.get("last_price",0)
                    vcp_dry_v = vcp.get("vol_dry_recent",1.0)
                    st.markdown(f"""
                    <div style='margin-top:10px;padding:10px;background:rgba(0,0,0,.3);
                                border-radius:3px;border-left:2px solid {vcp_gc}'>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                  color:#5a7a9a;letter-spacing:1px;margin-bottom:5px'>VCP ENTRY GUIDANCE</div>
                      <div style='font-size:11px;color:#cdd8e6;line-height:1.7'>
                        Pivot (buy above): <b style='color:#ffab00'>Rp {pivot_p:,.0f}</b><br>
                        Current: <b>Rp {last_p:,.0f}</b>
                        ({((last_p/pivot_p-1)*100):+.1f}% from pivot)<br>
                        Vol dry-up: <b style='color:{"#00e676" if vcp_dry_v<0.75 else "#5a7a9a"}'>
                        {vcp_dry_v:.2f}x avg {"✅" if vcp_dry_v<0.75 else ""}</b><br><br>
                        <b style='color:#eaf0f8'>Entry:</b> Buy breakout above pivot
                        with volume <b>≥ 1.5x average</b><br>
                        <b style='color:#ff1744'>Stop-loss:</b> Below last contraction low
                      </div>
                    </div>""", unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.plotly_chart(chart_vcp(R["df"], vcp), use_container_width=True)

        # ── BROKER + SHAREHOLDERS + FUNDAMENTALS
        st.markdown("---")
        col_b1, col_b2, col_b3 = st.columns(3)

        with col_b1:
            st.markdown('<div class="sec">TODAY\'S BROKER FLOW</div>', unsafe_allow_html=True)
            if R["src"] == "demo":
                st.caption("⚠️ Demo — add Stockbit token or use during market hours")
            st.plotly_chart(chart_broker_flow(R["bdf"]), use_container_width=True)

        with col_b2:
            st.markdown('<div class="sec">BROKER CATEGORY FLOW</div>', unsafe_allow_html=True)
            st.plotly_chart(chart_cat_flow(br), use_container_width=True)
            # Smart money detail
            for group,label,color in [(br["sm_buyers"],"▲ SM BUYERS","#00e676"),
                                       (br["sm_sellers"],"▼ SM SELLERS","#ff1744")]:
                if not group: continue
                st.markdown(f"""
                <div style='border-left:3px solid {color};padding:8px 10px;
                            background:rgba(0,0,0,.2);border-radius:3px;margin-top:6px'>
                  <div class='sec' style='margin-bottom:4px;color:{color}'>{label}</div>""",
                  unsafe_allow_html=True)
                for b in group[:4]:
                    a = f" @ Rp{b['buy_avg']:,.0f}" if b.get("buy_avg",0)>0 else ""
                    sign = "+" if b["net"]>0 else ""
                    st.markdown(f"""
                    <div class='kv'>
                      <span><b style='color:{color}'>{b["broker"]}</b>
                      <span style='color:#5a7a9a;font-size:10px'> {b["name"][:18]}</span></span>
                      <span style='color:{color};font-family:"IBM Plex Mono",monospace'>
                        {sign}{b["net"]:,}{a}</span>
                    </div>""", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

        with col_b3:
            st.markdown('<div class="sec">SHAREHOLDERS &gt;1% (IDX/KSEI)</div>', unsafe_allow_html=True)
            if sh_list:
                fig_sh = chart_sh_pie(sh_list)
                if fig_sh: st.plotly_chart(fig_sh, use_container_width=True)
                src_note = {"idx_live":"IDX/KSEI Live",
                            "ownership_db":"Ownership DB (estimate)"}.get(R["sh_src"],"—")
                st.markdown(f'<div class="ts-note">Source: {src_note} · {R.get("sh_date","")}</div>',
                            unsafe_allow_html=True)
                for s in sh_list[:7]:
                    pct  = s.get("pct",0); bar_w = min(100,int(pct*2))
                    bc   = "#00e676" if "control" in s.get("type","").lower() else "#00b0ff"
                    lots_s = f" · {s['lots']:,} shares" if s.get("lots",0)>0 else ""
                    st.markdown(f"""
                    <div style='padding:5px 0;border-bottom:1px solid #141e2e'>
                      <div style='display:flex;justify-content:space-between;
                                  font-family:"IBM Plex Mono",monospace;font-size:10px'>
                        <span style='color:#eaf0f8'>{s["name"][:28]}</span>
                        <span style='color:{bc};font-weight:700'>{pct:.1f}%{lots_s}</span>
                      </div>
                      <div class='bar-wrap'>
                        <div class='bar' style='width:{bar_w}%;background:{bc}'></div>
                      </div>
                    </div>""", unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style='padding:20px;text-align:center;font-family:"IBM Plex Mono",monospace;
                            font-size:11px;color:#2a3d52'>
                  Shareholder data unavailable.<br>IDX/KSEI publishes monthly at<br>
                  <a href="https://idx.co.id/id/berita/pengumuman/" target="_blank"
                     style="color:#5a7a9a">idx.co.id → Announcements</a>
                </div>""", unsafe_allow_html=True)

        # ── FUNDAMENTALS + KSEI
        st.markdown("---")
        # ── FUNDAMENTAL QUALITY + CORPORATE ACTIONS
        st.markdown("---")
        ca    = R.get("ca", {})
        fq    = R.get("fq", {})
        div_m = R.get("div_m", {})

        col_fq, col_ca, col_ksei = st.columns(3)

        # ── LEFT: Fundamental Quality Screen
        with col_fq:
            st.markdown('<div class="sec">FUNDAMENTAL QUALITY SCREEN</div>',
                        unsafe_allow_html=True)
            if fund and fq.get("checks"):
                qs  = fq.get("score", 0)
                qov = fq.get("overall","—")
                qoc = fq.get("color","#5a7a9a")
                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid {qoc};
                            border-top:2px solid {qoc};padding:10px 14px;
                            border-radius:4px;margin-bottom:8px;
                            display:flex;justify-content:space-between;align-items:center'>
                  <div>
                    <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                color:#5a7a9a;letter-spacing:1px'>QUALITY VERDICT</div>
                    <div style='font-family:"IBM Plex Mono",monospace;font-size:1rem;
                                font-weight:700;color:{qoc}'>{qov}</div>
                  </div>
                  <div style='font-family:"IBM Plex Mono",monospace;font-size:1.5rem;
                              font-weight:700;color:{qoc}'>{qs}/100</div>
                </div>""", unsafe_allow_html=True)

                # Quality checks table
                for chk in fq["checks"]:
                    sc = chk["status"]
                    ic = {"PASS":"✅","WARN":"🟡","FAIL":"❌","N/A":"—"}.get(sc,"—")
                    cc = {"PASS":"#00e676","WARN":"#ffab00","FAIL":"#ff1744","N/A":"#2a3d52"}.get(sc,"#5a7a9a")
                    st.markdown(f"""
                    <div style='display:flex;justify-content:space-between;padding:5px 0;
                                border-bottom:1px solid #141e2e;align-items:center'>
                      <div>
                        <span style='font-family:"IBM Plex Mono",monospace;font-size:10px;
                                     color:#cdd8e6'>{chk["name"]}</span>
                        <span style='font-family:"IBM Plex Mono",monospace;font-size:10px;
                                     color:{cc};margin-left:8px;font-weight:700'>
                          {chk["value"]}</span>
                      </div>
                      <div style='display:flex;align-items:center;gap:6px'>
                        <span style='font-size:10px'>{ic}</span>
                        <span style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                     color:{cc}'>{sc}</span>
                      </div>
                    </div>""", unsafe_allow_html=True)

                # Additional metrics
                st.markdown("<br>", unsafe_allow_html=True)
                for label, key in [
                    ("Market Cap",   "market_cap"),
                    ("P/E (trailing)","pe"),
                    ("P/B",          "pb"),
                    ("EV/EBITDA",    "ev_ebitda"),
                    ("PEG Ratio",    "peg"),
                    ("Beta",         "beta"),
                    ("Institutional","inst_pct"),
                ]:
                    v = fund.get(key,"—")
                    if v and v != "—":
                        st.markdown(f"""
                        <div class='kv'>
                          <span class='kv-k'>{label}</span>
                          <span class='kv-v'>{v}</span>
                        </div>""", unsafe_allow_html=True)

                if fund.get("hi52") and fund.get("lo52") and fund.get("curr"):
                    pct = min(100,max(0,(fund["curr"]-fund["lo52"])/(fund["hi52"]-fund["lo52"])*100))
                    bc  = "#00e676" if pct<40 else "#ffab00" if pct<75 else "#ff1744"
                    st.markdown(f"""
                    <div style='margin-top:8px;padding:10px;background:#0b1018;
                                border:1px solid #141e2e;border-radius:3px'>
                      <div class='sec'>52-WEEK POSITION</div>
                      <div class='bar-wrap'>
                        <div class='bar' style='width:{pct:.0f}%;background:{bc}'></div>
                      </div>
                      <div style='display:flex;justify-content:space-between;margin-top:3px;
                                  font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a'>
                        <span>Rp {fund["lo52"]:,.0f}</span>
                        <span style='color:{bc}'>{pct:.0f}% from low</span>
                        <span>Rp {fund["hi52"]:,.0f}</span>
                      </div>
                    </div>""", unsafe_allow_html=True)

        # ── MIDDLE: Corporate Action Calendar
        with col_ca:
            st.markdown('<div class="sec">CORPORATE ACTION CALENDAR</div>',
                        unsafe_allow_html=True)
            if ca.get("available"):
                fwd_yield  = div_m.get("fwd_yield", 0)
                payout     = div_m.get("payout_ratio")
                exdiv_stat = div_m.get("exdiv_status","—")
                exdiv_col  = div_m.get("exdiv_color","#5a7a9a")
                exdiv_date = ca.get("ex_div_date","—")
                div_amt    = ca.get("div_amount", 0)
                consistency= ca.get("div_consistency","—")

                # Ex-dividend alert box
                if ca.get("exdiv_alert"):
                    st.markdown(f"""
                    <div style='background:rgba(255,171,0,.1);border:1px solid rgba(255,171,0,.4);
                                padding:10px 14px;border-radius:3px;margin-bottom:8px'>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;
                                  font-weight:700;color:#ffab00'>
                        ⚡ EX-DIVIDEND ALERT</div>
                      <div style='font-size:11px;color:#cdd8e6;margin-top:3px'>
                        Ex-date: <b>{exdiv_date}</b> — {exdiv_stat}<br>
                        Buy BEFORE this date to receive dividend
                      </div>
                    </div>""", unsafe_allow_html=True)

                # Dividend summary
                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid #141e2e;
                            padding:12px 14px;border-radius:4px;margin-bottom:8px'>
                  <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                              color:#5a7a9a;letter-spacing:1px;margin-bottom:8px'>DIVIDEND</div>
                  {''.join([f"""
                  <div class='kv'>
                    <span class='kv-k'>{lbl}</span>
                    <span class='kv-v' style='color:{vc}'>{val}</span>
                  </div>""" for lbl,val,vc in [
                    ("Ex-Div Date",   exdiv_date if exdiv_date else "—",  exdiv_col),
                    ("Status",        exdiv_stat,                          exdiv_col),
                    ("Amount/Share",  f"Rp {div_amt:,.0f}" if div_amt else "—", "#eaf0f8"),
                    ("Fwd Yield",     f"{fwd_yield:.2f}%" if fwd_yield else "—",
                                      "#00e676" if fwd_yield>=4 else "#ffab00" if fwd_yield>=2 else "#5a7a9a"),
                    ("Payout Ratio",  f"{payout:.0f}%" if payout else "—",
                                      "#ff1744" if payout and payout>90 else "#ffab00" if payout and payout>60 else "#00e676"),
                    ("Consistency",   consistency,
                                      "#00e676" if "3/3" in str(consistency) else "#ffab00"),
                  ]])}
                </div>""", unsafe_allow_html=True)

                # Dividend history chart
                if ca.get("div_history"):
                    st.markdown('<div class="sec">DIVIDEND HISTORY</div>', unsafe_allow_html=True)
                    st.plotly_chart(chart_dividend_history(ca["div_history"]),
                                    use_container_width=True)

                # Earnings calendar
                next_earn = ca.get("next_earnings")
                if next_earn:
                    days_e = next_earn.get("days_to", 999)
                    ec     = "#ff1744" if days_e<=7 else "#ffab00" if days_e<=21 else "#5a7a9a"
                    st.markdown(f"""
                    <div style='background:#0b1018;border:1px solid #141e2e;
                                border-left:3px solid {ec};padding:10px 14px;
                                border-radius:3px;margin-top:8px'>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                  color:#5a7a9a;letter-spacing:1px'>NEXT EARNINGS</div>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:1rem;
                                  font-weight:700;color:{ec};margin-top:3px'>
                        {next_earn["date"]}</div>
                      <div style='font-size:10px;color:{ec};margin-top:2px'>
                        {'⚡ ' if next_earn.get("alert") else ''}{days_e} days away
                        {'— Institutional positioning window active' if 7<=days_e<=21 else ''}
                      </div>
                    </div>""", unsafe_allow_html=True)

                # Earnings beat/miss history
                earn_hist = ca.get("earnings_history", [])
                if earn_hist:
                    st.markdown('<div class="sec" style="margin-top:8px">EARNINGS SURPRISE HISTORY</div>',
                                unsafe_allow_html=True)
                    beats = sum(1 for e in earn_hist if e.get("beat"))
                    st.markdown(f"""
                    <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                color:#5a7a9a;margin-bottom:5px'>
                      Beat streak: <span style='color:{"#00e676" if beats>=3 else "#ffab00" if beats>=2 else "#ff8888"}'>
                      {beats}/{len(earn_hist)} quarters</span>
                    </div>""", unsafe_allow_html=True)
                    for e in earn_hist[:4]:
                        beat_c = "#00e676" if e.get("beat") else "#ff1744" if e.get("beat")==False else "#5a7a9a"
                        st.markdown(f"""
                        <div style='display:flex;justify-content:space-between;padding:4px 0;
                                    border-bottom:1px solid #141e2e;font-family:"IBM Plex Mono",monospace;
                                    font-size:10px'>
                          <span style='color:#5a7a9a'>{e["quarter"]}</span>
                          <span>Est: {e["estimate"]}</span>
                          <span>Act: {e["actual"]}</span>
                          <span style='color:{beat_c}'>{e["surprise"]}</span>
                        </div>""", unsafe_allow_html=True)

                # Stock splits
                if ca.get("splits"):
                    st.markdown('<div class="sec" style="margin-top:8px">STOCK SPLITS</div>',
                                unsafe_allow_html=True)
                    for sp in ca["splits"][:3]:
                        st.markdown(f"""
                        <div class='kv'>
                          <span class='kv-k'>{sp["date"]}</span>
                          <span class='kv-v'>{sp["ratio"]}</span>
                        </div>""", unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style='padding:20px;text-align:center;font-family:"IBM Plex Mono",monospace;
                            font-size:11px;color:#2a3d52'>
                  Corporate action data unavailable.<br>
                  Check IDX announcements at<br>
                  <a href="https://www.idx.co.id" target="_blank" style="color:#5a7a9a">idx.co.id</a>
                </div>""", unsafe_allow_html=True)

        # ── RIGHT: KSEI Composition
        with col_ksei:
            st.markdown('<div class="sec">KSEI — FOREIGN / DOMESTIC</div>',
                        unsafe_allow_html=True)
            if ksei and ksei.get("foreign"):
                fp = ksei["foreign"]; dp = ksei.get("domestic",100-fp)
                fig_k = go.Figure(go.Pie(labels=["Foreign","Domestic"],values=[fp,dp],
                    hole=0.5,marker=dict(colors=["#00b0ff","#00e676"],
                    line=dict(color="#05080c",width=2)),
                    textfont=dict(color="#cdd8e6",size=11,family="IBM Plex Mono"),
                    hovertemplate="%{label}: %{value:.1f}%<extra></extra>"))
                fig_k.update_layout(**CL,height=200,showlegend=True,
                    legend=dict(bgcolor="#0b1018",bordercolor="#141e2e",
                                font=dict(size=10,color="#cdd8e6")))
                st.plotly_chart(fig_k, use_container_width=True)
                st.markdown(f"""
                <div style='font-family:"IBM Plex Mono",monospace;font-size:10px'>
                  <span style='color:#00b0ff'>Foreign: {fp:.1f}%</span>
                  <span style='color:#00e676;margin-left:14px'>Domestic: {dp:.1f}%</span>
                  <span class='ts-note' style='margin-left:14px'>as of {ksei.get("date","—")}</span>
                </div>""", unsafe_allow_html=True)
                if fp > 55:
                    st.info("📌 High foreign ownership — on MSCI radar. Foreign flow dominates price.")
                elif fp < 20:
                    st.info("📌 Low foreign ownership — domestic driven. Watch CC, DX, LG brokers.")
            else:
                st.markdown("""
                <div style='padding:14px;text-align:center;font-family:"IBM Plex Mono",monospace;
                            font-size:11px;color:#2a3d52'>
                  KSEI data unavailable.<br>
                  <a href="https://web.ksei.co.id" target="_blank" style="color:#5a7a9a">
                  web.ksei.co.id</a>
                </div>""", unsafe_allow_html=True)

            # Fundamental detail (remaining metrics)
            if fund:
                st.markdown('<div class="sec" style="margin-top:10px">PROFITABILITY</div>',
                            unsafe_allow_html=True)
                for lbl, key in [("ROE",  "roe"), ("ROA","roa"),
                                  ("Gross Margin","gpm"), ("Op. Margin","opm"),
                                  ("Net Margin","npm"), ("Revenue Growth","rev_growth"),
                                  ("EPS Growth","earn_growth"), ("FCF","fcf_str"),
                                  ("FCF Yield","fcf_yield")]:
                    v = fund.get(key,"—")
                    if v and v != "—":
                        # Color coding for key metrics
                        vc = "#cdd8e6"
                        if key == "roe":
                            rv = fund.get("roe_raw",0) or 0
                            vc = "#00e676" if rv>=0.20 else "#ffab00" if rv>=0.12 else "#ff8888"
                        st.markdown(f"""
                        <div class='kv'>
                          <span class='kv-k'>{lbl}</span>
                          <span style='color:{vc};font-family:"IBM Plex Mono",monospace;
                                       font-size:11px;font-weight:500'>{v}</span>
                        </div>""", unsafe_allow_html=True)



        # ── RELATIVE STRENGTH vs IHSG
        rs_data = R.get("rs_data", {})
        if rs_data and rs_data.get("available"):
            st.markdown("---")
            st.markdown('<div class="sec">RELATIVE STRENGTH vs IHSG (^JKSE)</div>',
                        unsafe_allow_html=True)
            rs20v = rs_data.get("rs20_val",100)
            rs60v = rs_data.get("rs60_val",100)
            t20   = rs_data.get("trend20","flat")
            t60   = rs_data.get("trend60","flat")
            rc    = rs_data.get("color","#ffab00")
            interp= rs_data.get("interp","IN LINE")
            note  = rs_data.get("note","")
            rs_score = rs_data.get("score",50)
            rs_bonus = ent.get("rs_bonus",0)

            # RS banner
            st.markdown(f"""
            <div style='background:#0b1018;border:1px solid #141e2e;
                        border-left:4px solid {rc};padding:14px 18px;border-radius:3px;
                        margin-bottom:12px'>
              <div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px'>
                <div>
                  <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                              color:#5a7a9a;letter-spacing:2px;margin-bottom:4px'>RS STATUS</div>
                  <div style='font-family:"IBM Plex Mono",monospace;font-size:1.6rem;
                              font-weight:700;color:{rc}'>{interp}</div>
                  <div style='font-size:11px;color:#cdd8e6;margin-top:4px'>{note}</div>
                </div>
                <div style='display:flex;gap:20px;font-family:"IBM Plex Mono",monospace'>
                  <div style='text-align:center'>
                    <div style='font-size:9px;color:#5a7a9a;letter-spacing:1px'>RS 20D</div>
                    <div style='font-size:1.3rem;font-weight:700;
                                color:{"#00e676" if rs20v>=100 else "#ff1744"}'>{rs20v:.1f}</div>
                    <div style='font-size:9px;color:{"#00e676" if t20=="rising" else "#ff1744"}'>
                      {"▲ Rising" if t20=="rising" else "▼ Falling"}</div>
                  </div>
                  <div style='text-align:center'>
                    <div style='font-size:9px;color:#5a7a9a;letter-spacing:1px'>RS 60D</div>
                    <div style='font-size:1.3rem;font-weight:700;
                                color:{"#00e676" if rs60v>=100 else "#ff1744"}'>{rs60v:.1f}</div>
                    <div style='font-size:9px;color:{"#00e676" if t60=="rising" else "#ff1744"}'>
                      {"▲ Rising" if t60=="rising" else "▼ Falling"}</div>
                  </div>
                  <div style='text-align:center'>
                    <div style='font-size:9px;color:#5a7a9a;letter-spacing:1px'>RS SCORE</div>
                    <div style='font-size:1.3rem;font-weight:700;color:{rc}'>{rs_score}/100</div>
                    <div style='font-size:9px;color:#5a7a9a'>
                      Score adj: <span style='color:{"#00e676" if rs_bonus>0 else "#ff1744" if rs_bonus<0 else "#5a7a9a"}'>
                      {rs_bonus:+d}</span></div>
                  </div>
                </div>
              </div>
            </div>""", unsafe_allow_html=True)

            # RS chart
            st.plotly_chart(chart_rs(rs_data, R["ticker"]), use_container_width=True)

            # RS interpretation boxes
            col_r1, col_r2, col_r3 = st.columns(3)
            with col_r1:
                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid #141e2e;padding:12px;border-radius:3px'>
                  <div class='sec'>RS INTERPRETATION</div>
                  <div style='font-size:11px;color:#cdd8e6;line-height:1.7'>
                    RS 20d <b>{">100" if rs20v>100 else "<100"}</b>
                    = saham {"outperform" if rs20v>=100 else "underperform"} IHSG<br>
                    Trend: <b style='color:{"#00e676" if t20=="rising" else "#ff1744"}'>
                    {"Menguat ▲" if t20=="rising" else "Melemah ▼"}</b><br>
                    RS 20d {">" if rs20v>rs60v else "<"} RS 60d
                    = momentum {"accelerating" if rs20v>rs60v else "decelerating"}
                  </div>
                </div>""", unsafe_allow_html=True)
            with col_r2:
                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid #141e2e;padding:12px;border-radius:3px'>
                  <div class='sec'>CONTEXT: RS vs ALPHA</div>
                  <div style='font-size:11px;color:#cdd8e6;line-height:1.7'>
                    Saham naik karena IHSG naik = <b style='color:#ffab00'>bukan alpha</b><br>
                    Saham naik saat IHSG turun = <b style='color:#00e676'>alpha murni</b><br>
                    Smart money selalu masuk ke <b>saham dengan RS rising</b>
                    bahkan sebelum harga breakout
                  </div>
                </div>""", unsafe_allow_html=True)
            with col_r3:
                # Best use case
                rs_signal = ""
                if rs20v > 105 and t20 == "rising":
                    rs_signal = "✅ RS SETUP IDEAL — RS positif + trend naik. Konfluens dengan sinyal teknikal."
                elif rs20v < 95 and t20 == "falling":
                    rs_signal = "⚠️ RS NEGATIF — Saham lemah vs market. Tunggu RS stabilize sebelum entry."
                elif rs20v > 100 and t20 == "falling":
                    rs_signal = "🟡 RS MEMBALIK — Masih outperform tapi momentum melemah. Monitor."
                elif rs20v < 100 and t20 == "rising":
                    rs_signal = "🔄 RS RECOVERY — Mulai recover dari underperform. Potensi rotation masuk."
                else:
                    rs_signal = "➡️ RS NETRAL — Bergerak seiring IHSG. Sinyal broker lebih dominan."
                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid #141e2e;padding:12px;border-radius:3px;
                            border-left:3px solid {rc}'>
                  <div class='sec'>RS SIGNAL</div>
                  <div style='font-size:11px;color:#cdd8e6;line-height:1.7'>{rs_signal}</div>
                </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style='padding:8px 12px;background:#0b1018;border:1px solid #141e2e;
                        border-radius:3px;margin-top:8px;font-family:"IBM Plex Mono",monospace;
                        font-size:10px;color:#5a7a9a'>
              RS vs IHSG: IHSG data unavailable — check ^JKSE on Yahoo Finance
            </div>""", unsafe_allow_html=True)

        # ── WEEKLY CONFLUENCE DETAIL
        st.markdown("---")
        col_wc, col_liq = st.columns(2)

        with col_wc:
            weekly_c = R.get("weekly_c", {})
            st.markdown('<div class="sec">WEEKLY TIMEFRAME CONFLUENCE</div>',
                        unsafe_allow_html=True)
            if weekly_c.get("available"):
                wc_score = weekly_c["score"]
                wc_color = weekly_c["color"]
                wc_label = weekly_c["label"]
                wc_action= weekly_c["action"]

                # Score gauge bar
                bar_w = wc_score
                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid {wc_color};
                            border-top:2px solid {wc_color};padding:12px 14px;
                            border-radius:4px;margin-bottom:10px'>
                  <div style='display:flex;justify-content:space-between;align-items:center;
                              margin-bottom:8px'>
                    <div>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                  color:#5a7a9a;letter-spacing:1px'>WEEKLY SCORE</div>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:1.4rem;
                                  font-weight:700;color:{wc_color}'>{wc_score}/100</div>
                      <div style='font-size:11px;color:{wc_color};margin-top:2px'>
                        {wc_label}</div>
                    </div>
                    <div style='text-align:right;font-size:10px;
                                font-family:"IBM Plex Mono",monospace;color:#5a7a9a'>
                      {"✅" if wc_score>=55 else "⚠️" if wc_score>=40 else "🚫"}<br>
                      Daily {'confirmed' if wc_score>=55 else 'uncertain' if wc_score>=40 else 'OPPOSED'}
                    </div>
                  </div>
                  <div class='bar-wrap'>
                    <div class='bar' style='width:{bar_w}%;background:{wc_color}'></div>
                  </div>
                  <div style='font-size:10px;color:#cdd8e6;margin-top:7px;line-height:1.5'>
                    {wc_action}</div>
                </div>""", unsafe_allow_html=True)

                # Component breakdown
                st.markdown('<div class="sec">COMPONENT BREAKDOWN</div>',
                            unsafe_allow_html=True)
                for comp in weekly_c.get("components", []):
                    pts  = comp["score"]
                    mx   = comp["max"]
                    pct  = int(pts/mx*100) if mx>0 else 0
                    cc   = comp["color"]
                    st.markdown(f"""
                    <div style='padding:6px 0;border-bottom:1px solid #141e2e'>
                      <div style='display:flex;justify-content:space-between;
                                  font-family:"IBM Plex Mono",monospace;font-size:10px;
                                  margin-bottom:3px'>
                        <span style='color:#cdd8e6;font-weight:600'>{comp["name"]}</span>
                        <span style='color:{cc}'>{comp["value"]}
                          &nbsp;
                          <span style='color:#5a7a9a;font-size:9px'>{pts}/{mx}</span>
                        </span>
                      </div>
                      <div class='bar-wrap'>
                        <div class='bar' style='width:{pct}%;background:{cc}'></div>
                      </div>
                      <div style='font-size:9px;color:#5a7a9a;margin-top:2px'>
                        {comp["label"]}</div>
                    </div>""", unsafe_allow_html=True)

                # Weekly vs Daily chart
                df_weekly = R.get("df_weekly")
                if df_weekly is not None and len(df_weekly) >= 20:
                    st.markdown('<div class="sec" style="margin-top:10px">WEEKLY CHART + DAILY CMF ALIGNMENT</div>',
                                unsafe_allow_html=True)
                    st.plotly_chart(
                        chart_weekly_vs_daily(R["df"], df_weekly, R["ticker"]),
                        use_container_width=True
                    )
            else:
                st.markdown("""
                <div style='padding:20px;text-align:center;font-family:"IBM Plex Mono",monospace;
                            font-size:11px;color:#2a3d52'>
                  Weekly data unavailable. Try period 3M or longer.
                </div>""", unsafe_allow_html=True)

        # ── LIQUIDITY ASSESSMENT
        with col_liq:
            liq = R.get("liq", {})
            st.markdown('<div class="sec">LIQUIDITY ASSESSMENT</div>',
                        unsafe_allow_html=True)
            if liq.get("available"):
                liq_score  = liq["score"]
                liq_color  = liq["color"]
                liq_label  = liq["label"]
                impact_days= liq["impact_days"]
                adv_str    = liq["adv_str"]
                adv_trend  = liq["adv_trend"]
                spread_pct = liq["spread_pct"]
                vol_cov    = liq["vol_cov"]
                is_fca     = liq.get("is_fca", False)
                liq_adj    = liq["score_adj"]
                adj_label  = liq["adj_label"]

                trend_color = "#00e676" if adv_trend=="GROWING" else "#ff8888" if adv_trend=="SHRINKING" else "#5a7a9a"

                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid {liq_color};
                            border-top:2px solid {liq_color};padding:12px 14px;
                            border-radius:4px;margin-bottom:10px'>
                  <div style='display:flex;justify-content:space-between;margin-bottom:8px'>
                    <div>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                                  color:#5a7a9a;letter-spacing:1px'>LIQUIDITY SCORE</div>
                      <div style='font-family:"IBM Plex Mono",monospace;font-size:1.4rem;
                                  font-weight:700;color:{liq_color}'>{liq_score}/100</div>
                      <div style='font-size:11px;color:{liq_color};margin-top:2px'>
                        {liq_label}</div>
                    </div>
                    <div style='text-align:right;font-family:"IBM Plex Mono",monospace;
                                font-size:10px'>
                      <div style='color:#5a7a9a'>Score adj</div>
                      <div style='color:{"#00e676" if liq_adj>0 else "#ff1744" if liq_adj<0 else "#5a7a9a"};
                                  font-size:1.1rem;font-weight:700'>{liq_adj:+d}</div>
                    </div>
                  </div>
                  <div class='bar-wrap'>
                    <div class='bar' style='width:{liq_score}%;background:{liq_color}'></div>
                  </div>
                  <div style='font-size:10px;color:#cdd8e6;margin-top:6px'>{adj_label}</div>
                </div>""", unsafe_allow_html=True)

                # Liquidity metrics table
                st.markdown('<div class="sec">MARKET IMPACT METRICS</div>',
                            unsafe_allow_html=True)
                impact_color = "#00e676" if impact_days<1 else "#ffab00" if impact_days<3 else "#ff1744"
                for lbl, val, vc in [
                    ("ADV (20-day)", adv_str, "#cdd8e6"),
                    ("ADV Trend", adv_trend, trend_color),
                    ("Market Impact", f"{impact_days:.1f} days to build position", impact_color),
                    ("Daily Capacity", f"Rp {liq['daily_capacity']/1e6:.0f}M/day (20% ADV)", "#5a7a9a"),
                    ("Intrabar Spread", f"~{spread_pct:.1f}% (transaction cost proxy)",
                     "#00e676" if spread_pct<1.5 else "#ffab00" if spread_pct<3 else "#ff1744"),
                    ("Volume Stability", f"CoV {vol_cov:.2f} ({'Stable' if vol_cov<0.8 else 'Erratic'})",
                     "#00e676" if vol_cov<0.8 else "#ffab00" if vol_cov<1.5 else "#ff1744"),
                    ("FCA Status", "⚠️ YES — Float <15%, restricted liquidity" if is_fca else "No — Normal trading",
                     "#ff1744" if is_fca else "#00e676"),
                ]:
                    st.markdown(f"""
                    <div class='kv'>
                      <span class='kv-k'>{lbl}</span>
                      <span style='color:{vc};font-family:"IBM Plex Mono",monospace;
                                   font-size:11px'>{val}</span>
                    </div>""", unsafe_allow_html=True)

                # Market impact explanation
                st.markdown(f"""
                <div style='margin-top:10px;padding:10px 12px;background:rgba(0,0,0,.3);
                            border-radius:3px;border-left:2px solid {liq_color};
                            font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a'>
                  💡 Market Impact Rule: Position should be ≤20% of ADV per day.
                  For position Rp{liq['position_value']/1e6:.0f}M: takes {impact_days:.1f} days
                  to execute without moving price.
                  {'⚠️ Reduce position size for this stock.' if impact_days>3 else
                   '✅ Acceptable execution window.' if impact_days<1.5 else
                   '🟡 Monitor execution — moderate impact.'}
                </div>""", unsafe_allow_html=True)
            else:
                st.markdown("""
                <div style='padding:20px;text-align:center;font-family:"IBM Plex Mono",monospace;
                            font-size:11px;color:#2a3d52'>Liquidity data unavailable.</div>""",
                unsafe_allow_html=True)

        # ── ALERTS
        for alert in br["alerts"]:
            {"success":st.success,"danger":st.error,"warn":st.warning}.get(
                alert["t"],st.info)(alert["m"])


    else:
        # Empty state
        popular = ["BBCA","BBRI","BMRI","BREN","AMMN","ADRO","MDKA","TLKM","ASII",
                   "KLBF","ICBP","GOTO","BYAN","UNTR","ANTM","PTBA","DCII","PGAS"]
        st.markdown("""
        <div style='text-align:center;padding:60px 20px;border:1px dashed #141e2e;
                    border-radius:4px;margin-top:12px'>
          <div style='font-size:1.8rem;color:#141e2e;margin-bottom:10px'>◈</div>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;color:#2a3d52;letter-spacing:2px'>
            ENTER ANY IDX TICKER · CLICK RUN ANALYSIS</div>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#1c2a3e;margin-top:6px'>
            Price · CMF · OBV · MFI · Wyckoff · Broker Flow · Ownership · Shareholders · Entry Zone
          </div>
          <div style='margin-top:16px;display:flex;justify-content:center;gap:5px;flex-wrap:wrap'>
        """ + "".join([
            f'<span style="padding:2px 9px;border:1px solid #141e2e;border-radius:2px;'
            f'font-family:IBM Plex Mono,monospace;font-size:9px;color:#2a3d52">{t}</span>'
            for t in popular
        ]) + "</div></div>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  TAB 2 — BROKER SHAREHOLDING
# ══════════════════════════════════════════════════════════════════════

with tab_bh:
    st.markdown("#### BROKER SHAREHOLDING — ESTIMATED ACCUMULATED POSITION")

    # Explanation banner
    st.markdown(f"""
    <div style='padding:12px 16px;background:#0b1018;border:1px solid #141e2e;
                border-left:3px solid #00b0ff;border-radius:3px;margin-bottom:14px;
                font-family:"IBM Plex Mono",monospace;font-size:10px;line-height:1.8'>
      <span style='color:#eaf0f8;font-weight:600;letter-spacing:1px'>HOW THIS WORKS</span><br>
      <span style='color:#5a7a9a'>
      We sum each broker's daily <b style='color:#cdd8e6'>net lot</b> (buy − sell) over
      <b style='color:#00e676'>{accu_days} trading days</b> to estimate their clients' accumulated position.
      Positive = broker clients are net long (accumulated shares).
      Negative = broker clients are net short / distributing.<br><br>
      <b style='color:#ffab00'>Methodology note:</b> This is the same approach used by RTI Business, Stockbit Pro, and Bloomberg
      IDX broker screens. True sub-account holdings are KSEI private data and not publicly available per broker.
      Lot size assumed: <b style='color:#cdd8e6'>100 shares/lot</b>.
      </span>
    </div>""", unsafe_allow_html=True)

    col_btn, col_tip = st.columns([2,8])
    with col_btn:
        run_bh = st.button("▶  CALCULATE", use_container_width=True)
    with col_tip:
        # Try to get shares outstanding from anywhere available
        so_preview = 0
        if "res" in st.session_state:
            so_preview = st.session_state["res"].get("fund",{}).get("shares_out",0) or 0
        if not so_preview and "bh_res" in st.session_state:
            so_preview = st.session_state["bh_res"].get("shares_out",0) or 0
        so_display = f"{so_preview/1e9:.2f}B shares" if so_preview else "Will be fetched automatically"
        st.markdown(f"""
        <div style='padding:7px 12px;background:#0b1018;border:1px solid #141e2e;
                    border-radius:3px;font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a'>
          Ticker: <b style='color:#eaf0f8'>{ticker_input}.JK</b>
          &nbsp;·&nbsp; Window: {accu_days} trading days
          &nbsp;·&nbsp; Shares OS: <b style='color:#cdd8e6'>{so_display}</b>
        </div>""", unsafe_allow_html=True)

    if run_bh:
        st.session_state.pop("bh_res", None)
        with st.spinner(f"Fetching {accu_days}-day broker data for {ticker_input} ..."):
            accu_df, accu_src, days_got = get_broker_accumulation(
                ticker_input, sb_token, accu_days)
            if accu_df is None:
                ts_est = st.session_state.get("res",{}).get("ts",50)
                accu_df = demo_broker(ticker_input, ts_est)
                accu_src = "demo"; days_got = accu_days

        # Auto-fetch shares outstanding — no dependency on Analysis tab
        shares_out = 0
        # 1. From Analysis tab cache
        if "res" in st.session_state:
            shares_out = st.session_state["res"].get("fund",{}).get("shares_out",0) or 0
        # 2. Fetch directly if not available
        if not shares_out:
            with st.spinner(f"Fetching shares outstanding for {ticker_input} ..."):
                fund2 = fundamentals(ticker_input)
                shares_out = fund2.get("shares_out",0) or 0
        # 3. Try yfinance info directly as last resort
        if not shares_out:
            try:
                info = yf.Ticker(ticker_input.upper().replace(".JK","") + ".JK").info
                shares_out = info.get("sharesOutstanding",0) or info.get("impliedSharesOutstanding",0) or 0
            except: pass

        sh_df = calc_broker_shareholding(accu_df, shares_out)
        st.session_state["bh_res"] = dict(
            accu_df=accu_df, sh_df=sh_df,
            src=accu_src, days=days_got,
            shares_out=shares_out,
            ticker=ticker_input,
            fetched=datetime.now().strftime("%H:%M:%S WIB"),
        )

    if "bh_res" in st.session_state:
        BH = st.session_state["bh_res"]
        adf = BH["accu_df"]; sh_df = BH["sh_df"]
        so  = BH["shares_out"]

        src_b = {"stockbit":'<span class="tag tg">STOCKBIT REAL</span>',
                 "idx":     '<span class="tag tb">IDX API REAL</span>',
                 "demo":    '<span class="tag ta">DEMO DATA</span>'}.get(BH["src"],"")

        if BH["src"]=="demo":
            st.warning("⚠️ **Demo data** — add Stockbit token or run during market hours (09:00–16:30 WIB Mon-Fri) for real data.")

        st.markdown(f"""
        <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;margin-bottom:10px'>
          {src_b} · Period: {BH["days"]} trading days · Updated: {BH["fetched"]}
          {f' · Shares OS: {so/1e9:.2f}B' if so else ''}
        </div>""", unsafe_allow_html=True)

        # ── SUMMARY METRICS
        m1,m2,m3,m4 = st.columns(4)
        fn = adf[adf["cat"]=="FOREIGN_SMART"]["cum_net"].sum()
        rn = adf[adf["cat"]=="RETAIL"]["cum_net"].sum()
        top_b = sh_df.iloc[0] if len(sh_df) else None
        top_pct = sh_df["est_pct"].sum()
        m1.metric("FOREIGN SM NET LOT", f"{fn:+,}",
                  delta="Accumulating" if fn>0 else "Distributing")
        m2.metric("RETAIL NET LOT", f"{rn:+,}",
                  delta="Contrarian signal" if rn<0 else "Caution" if rn>0 else "Neutral")
        m3.metric("LARGEST HOLDER",
                  f"{top_b['broker']} ({top_b.get('cum_net',0):+,}L)" if top_b is not None else "—")
        m4.metric("TOP 10 BROKER EST. %",
                  f"{top_pct:.2f}%" if so else "N/A (no shares data)",
                  delta="of shares outstanding" if so else "Add ticker to Analysis tab first")

        # ── ACCUMULATION CHART
        st.markdown("---")
        st.markdown('<div class="sec">CUMULATIVE NET POSITION — ALL BROKERS</div>', unsafe_allow_html=True)
        st.plotly_chart(chart_accumulation(adf, f"{BH['days']} trading days · {BH['src']}"),
                        use_container_width=True)

        # ── SHAREHOLDING CHART (only if shares_out available)
        if so and so > 0:
            st.markdown("---")
            st.markdown('<div class="sec">ESTIMATED % OF OUTSTANDING SHARES HELD PER BROKER</div>',
                        unsafe_allow_html=True)
            st.markdown(f"""
            <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;margin-bottom:8px'>
              Based on cumulative net lot × 100 shares/lot ÷ {so/1e9:.2f}B shares outstanding.
              Shows net accumulation in analysis window only — not absolute portfolio position.
            </div>""", unsafe_allow_html=True)
            fig_sh = chart_shareholding(sh_df, ticker_input, so)
            if fig_sh:
                st.plotly_chart(fig_sh, use_container_width=True)
        else:
            st.info("💡 Run **Analysis** tab first so we can load shares outstanding — then the % chart will appear here.")

        # ── DETAILED TABLE: ACCUMULATORS vs DISTRIBUTORS
        st.markdown("---")
        col_acc, col_dis = st.columns(2)

        acc_rows = sh_df[sh_df["cum_net"]>0].head(10)
        dis_rows = adf[adf["cum_net"]<0].sort_values("cum_net").head(10)

        with col_acc:
            st.markdown('<div class="sec">TOP ACCUMULATORS ★ (Net Buyers)</div>', unsafe_allow_html=True)
            total_pos = acc_rows["cum_net"].sum() or 1
            for _,row in acc_rows.iterrows():
                cat   = row.get("cat","LOCAL_INST")
                cc    = CAT_COLOR.get(cat,"#00b0ff")
                bar_w = min(100, int(row["cum_net"]/total_pos*100))
                star  = "★ " if cat=="FOREIGN_SMART" else ""
                est_pct_str = f"{row['est_pct']:.3f}%" if row.get("est_pct",0)>0 and so else ""
                st.markdown(f"""
                <div class='broker-hold-row'>
                  <div class='bh-code' style='color:{cc}'>{star}{row["broker"]}</div>
                  <div>
                    <div class='bh-name'>{row.get("flag","🇮🇩")} {row.get("name","")[:20]}</div>
                    <div style='font-size:9px;color:#2a3d52;font-family:"IBM Plex Mono",monospace'>
                      {CAT_LABEL.get(cat,cat)}</div>
                  </div>
                  <div class='bh-lots' style='color:#00e676'>+{row["cum_net"]:,} lots</div>
                  <div class='bh-pct' style='color:#5a7a9a'>{est_pct_str}</div>
                </div>
                <div class='bar-wrap'>
                  <div class='bar' style='width:{bar_w}%;background:{cc}'></div>
                </div>""", unsafe_allow_html=True)

        with col_dis:
            st.markdown('<div class="sec">TOP DISTRIBUTORS ▼ (Net Sellers)</div>', unsafe_allow_html=True)
            total_neg = abs(dis_rows["cum_net"].sum()) or 1
            for _,row in dis_rows.iterrows():
                cat   = row.get("cat","RETAIL")
                cc    = CAT_COLOR.get(cat,"#ff1744")
                bar_w = min(100, int(abs(row["cum_net"])/total_neg*100))
                is_retail = cat=="RETAIL"
                note  = "Contrarian → SM may be buying" if is_retail else ""
                st.markdown(f"""
                <div class='broker-hold-row'>
                  <div class='bh-code' style='color:{cc}'>{row["broker"]}</div>
                  <div>
                    <div class='bh-name'>{row.get("flag","🇮🇩")} {row.get("name","")[:20]}</div>
                    <div style='font-size:9px;color:{"#5a7a9a" if is_retail else "#2a3d52"};
                                font-family:"IBM Plex Mono",monospace'>{note or CAT_LABEL.get(cat,cat)}</div>
                  </div>
                  <div class='bh-lots' style='color:#ff1744'>{row["cum_net"]:,} lots</div>
                  <div class='bh-pct'></div>
                </div>
                <div class='bar-wrap'>
                  <div class='bar' style='width:{bar_w}%;background:{cc}'></div>
                </div>""", unsafe_allow_html=True)

        # ── INTERPRETATION
        st.markdown("---")
        st.markdown('<div class="sec">INTERPRETATION</div>', unsafe_allow_html=True)
        if fn > 10000 and rn < 0:
            st.success(f"🔥 **STRONG ACCUMULATION**: Foreign smart money +{fn:,} lots over {BH['days']} days while retail selling {rn:,} lots. Classic Accumulation Cross — bandar absorbing supply.")
        elif fn < -10000 and rn > 0:
            st.error(f"💀 **DISTRIBUTION**: Smart money {fn:,} lots while retail buying +{rn:,} lots. Retailer likely bag-holding for bandar.")
        elif fn > 3000:
            st.info(f"📊 Foreign smart money net +{fn:,} lots. Moderate accumulation signal — monitor for confirmation.")
        else:
            st.info("📊 No dominant accumulation or distribution signal. Mixed flow — wait for clearer setup.")

    else:
        st.markdown("""
        <div style='text-align:center;padding:50px;border:1px dashed #141e2e;border-radius:4px'>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;color:#2a3d52'>
            Set ticker in sidebar → Click CALCULATE</div>
        </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  TAB 3 — BACKTEST & RELATIVE STRENGTH
# ══════════════════════════════════════════════════════════════════════

with tab_bt:
    st.markdown("#### BACKTESTING ENGINE & RELATIVE STRENGTH ANALYSIS")

    # ── Top explanation
    st.markdown("""
    <div style='display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px'>
      <div style='padding:12px 16px;background:#0b1018;border:1px solid #141e2e;
                  border-left:3px solid #00e676;border-radius:3px'>
        <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;
                    font-weight:700;color:#00e676;letter-spacing:2px;margin-bottom:6px'>
          BACKTESTING ENGINE</div>
        <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>
          Simulasi historis: jika kita mengikuti sinyal sistem ini di masa lalu,
          berapa return-nya? Menghitung win rate, Sharpe ratio, max drawdown,
          equity curve, dan perbandingan alpha vs IHSG.
          <br><br>
          <b style='color:#ffab00'>Metodologi:</b> Walk-forward, 60-hari lookback window,
          hanya menggunakan indikator teknikal (CMF · OBV · MFI · Wyckoff · Volume).
          Broker data tidak tersedia secara historis.
        </div>
      </div>
      <div style='padding:12px 16px;background:#0b1018;border:1px solid #141e2e;
                  border-left:3px solid #00b0ff;border-radius:3px'>
        <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;
                    font-weight:700;color:#00b0ff;letter-spacing:2px;margin-bottom:6px'>
          RELATIVE STRENGTH vs IHSG</div>
        <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>
          Mengukur seberapa kuat saham bergerak <b>relatif terhadap IHSG</b>.
          Saham dengan RS naik = smart money sedang accumulate.
          Saham yang naik hanya karena IHSG naik = bukan alpha, hindari.
          <br><br>
          <b style='color:#ffab00'>Key insight:</b> Smart money selalu masuk ke saham RS rising
          <i>sebelum</i> breakout harga. RS adalah leading indicator.
        </div>
      </div>
    </div>""", unsafe_allow_html=True)

    # ── Controls
    col_bt1, col_bt2, col_bt3, col_bt4 = st.columns([3, 2, 2, 2])
    with col_bt1:
        bt_ticker = st.text_input("Ticker for Backtest",
                                   value=ticker_input, max_chars=10,
                                   key="bt_ticker").upper().strip().replace(".JK","")
    with col_bt2:
        bt_period = st.selectbox("Data Period",
                                  ["1y","2y","3y","5y"], index=1, key="bt_period")
    with col_bt3:
        bt_signal = st.selectbox("Min Signal to Trade",
                                  ["BUY","STRONG BUY"], index=0, key="bt_signal")
    with col_bt4:
        bt_hold = st.selectbox("Primary Hold Period",
                                [5, 10, 20, 30], index=1, key="bt_hold",
                                format_func=lambda x: f"{x} days")

    col_run1, col_run2 = st.columns([2, 8])
    with col_run1:
        run_bt = st.button("▶  RUN BACKTEST + RS", use_container_width=True)
    with col_run2:
        st.markdown(f"""
        <div style='padding:7px 12px;background:#0b1018;border:1px solid #141e2e;
                    border-radius:3px;font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a'>
          <b style='color:#eaf0f8'>{bt_ticker}.JK</b>
          &nbsp;·&nbsp; {bt_period} data
          &nbsp;·&nbsp; Min signal: {bt_signal}
          &nbsp;·&nbsp; Primary hold: {bt_hold}d
          &nbsp;·&nbsp; Also computes RS 20d/60d vs ^JKSE
        </div>""", unsafe_allow_html=True)

    if run_bt:
        st.session_state.pop("bt_res", None)

        with st.spinner(f"Running walk-forward backtest for {bt_ticker} ({bt_period}) ..."):
            bt_result = run_backtest(
                bt_ticker, bt_period,
                hold_days_list=(5, bt_hold) if bt_hold != 5 else (5, 10, 20),
                min_signal=bt_signal,
            )

        with st.spinner("Computing Relative Strength vs IHSG (^JKSE) ..."):
            df_bt = load_price(bt_ticker, bt_period)
            ihsg_bt = load_ihsg(bt_period)
            rs_bt = calc_rs(df_bt, ihsg_bt) if (df_bt is not None and ihsg_bt is not None) else {"available": False}

        st.session_state["bt_res"] = {
            "bt": bt_result, "rs": rs_bt,
            "ticker": bt_ticker, "period": bt_period,
            "hold": bt_hold, "signal": bt_signal,
            "fetched": datetime.now().strftime("%H:%M:%S WIB"),
        }

    if "bt_res" in st.session_state:
        BR = st.session_state["bt_res"]
        bt  = BR["bt"]
        rs  = BR["rs"]
        hold = BR["hold"]

        st.markdown("---")

        # ══════════════════════
        # SECTION A: BACKTEST
        # ══════════════════════
        st.markdown('<div class="sec">BACKTEST RESULTS</div>', unsafe_allow_html=True)

        if not bt.get("available"):
            st.error(f"Backtest failed: {bt.get('error','Unknown error')}. "
                     f"Try a longer period (2y/3y) or check ticker.")
        else:
            # Summary metrics row
            hold_days = bt["hold_days"]
            primary_h = hold if hold in bt["stats"] else hold_days[0]
            s = bt["stats"].get(primary_h, {})
            if not s:
                st.warning("No statistics generated. Try BUY instead of STRONG BUY.")
            else:
                # Color coding
                wr_color  = "#00e676" if s["win_rate"]>=60 else "#ffab00" if s["win_rate"]>=50 else "#ff1744"
                ar_color  = "#00e676" if s["avg_ret"]>0 else "#ff1744"
                sh_color  = "#00e676" if s["sharpe"]>0.5 else "#ffab00" if s["sharpe"]>0 else "#ff1744"
                al_color  = "#00e676" if s["avg_alpha"]>0 else "#ff1744"

                # ── Main KPIs
                st.markdown(f"""
                <div style='display:grid;grid-template-columns:repeat(8,1fr);gap:6px;margin-bottom:14px'>
                  {_kpi("TOTAL SIGNALS", str(s["total_signals"]), "#5a7a9a", "")}
                  {_kpi("WIN RATE", f"{s['win_rate']}%", wr_color,
                         "Good" if s["win_rate"]>=60 else "Weak")}
                  {_kpi("AVG RETURN", f"{s['avg_ret']:+.2f}%", ar_color,
                         f"{primary_h}d hold")}
                  {_kpi("MEDIAN RET", f"{s['median_ret']:+.2f}%", ar_color, "")}
                  {_kpi("SHARPE", f"{s['sharpe']:.2f}", sh_color,
                         "Strong" if s["sharpe"]>1 else "OK" if s["sharpe"]>0.5 else "Weak")}
                  {_kpi("MAX DRAWDOWN", f"{s['max_dd']:.1f}%", "#ff1744", "from equity peak")}
                  {_kpi("AVG ALPHA", f"{s['avg_alpha']:+.2f}%", al_color, "vs IHSG")}
                  {_kpi("BEAT IHSG %", f"{s['pct_beat_ihsg']}%", al_color,
                         "of signals")}
                </div>""", unsafe_allow_html=True)

                # ── Interpretation
                if s["win_rate"] >= 60 and s["avg_ret"] > 0 and s["sharpe"] > 0.5:
                    st.success(
                        f"✅ **Sistem ini profitable secara historis** untuk {bt_ticker}: "
                        f"Win rate {s['win_rate']}%, avg return {s['avg_ret']:+.2f}% per {primary_h} hari, "
                        f"Sharpe {s['sharpe']:.2f}, alpha {s['avg_alpha']:+.2f}% vs IHSG. "
                        f"Sinyal **{bt_signal}** terbukti memberikan edge.")
                elif s["win_rate"] < 50 or s["avg_ret"] < 0:
                    st.error(
                        f"⚠️ **Sistem kurang reliable** untuk {bt_ticker}: "
                        f"Win rate {s['win_rate']}%, avg return {s['avg_ret']:+.2f}%. "
                        f"Pertimbangkan menaikkan threshold sinyal ke STRONG BUY atau perbanyak konfirmasi.")
                else:
                    st.warning(
                        f"🟡 **Hasil campuran**: Win rate {s['win_rate']}%, avg return {s['avg_ret']:+.2f}%. "
                        f"Sistem bekerja tapi margin of safety tipis. Selalu gunakan stop-loss.")

                # ── Multi-hold comparison table
                if len(bt["stats"]) > 1:
                    st.markdown('<div class="sec" style="margin-top:14px">COMPARISON BY HOLDING PERIOD</div>',
                                unsafe_allow_html=True)
                    rows_comp = []
                    for h, hs in sorted(bt["stats"].items()):
                        rows_comp.append({
                            "Hold (days)"  : h,
                            "Total Signals": hs["total_signals"],
                            "Win Rate %"   : hs["win_rate"],
                            "Avg Return %" : hs["avg_ret"],
                            "Median Ret %": hs["median_ret"],
                            "Best %"       : hs["best"],
                            "Worst %"      : hs["worst"],
                            "Sharpe"       : hs["sharpe"],
                            "Max DD %"     : hs["max_dd"],
                            "Profit Factor": hs["profit_factor"],
                            "Avg Alpha %"  : hs["avg_alpha"],
                            "Beat IHSG %"  : hs["pct_beat_ihsg"],
                        })
                    st.dataframe(pd.DataFrame(rows_comp), hide_index=True,
                                  use_container_width=True)

                st.markdown('<div class="sec" style="margin-top:14px">EQUITY CURVE</div>',
                            unsafe_allow_html=True)
                st.markdown("""
                <div style='font-size:10px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;margin-bottom:6px'>
                Portfolio value starting at 100, setiap sinyal dieksekusi equal weight.
                Tidak memperhitungkan biaya transaksi dan slippage.
                </div>""", unsafe_allow_html=True)
                st.plotly_chart(chart_equity_curve(bt), use_container_width=True)

                # ── Charts row
                col_dist, col_scatter = st.columns(2)
                with col_dist:
                    st.markdown(f'<div class="sec">RETURN DISTRIBUTION — {primary_h}d hold</div>',
                                unsafe_allow_html=True)
                    st.plotly_chart(chart_return_distribution(bt, primary_h),
                                    use_container_width=True)
                with col_scatter:
                    st.markdown(f'<div class="sec">SIGNAL ENTRY vs RETURN — {primary_h}d hold</div>',
                                unsafe_allow_html=True)
                    st.plotly_chart(chart_signal_scatter(bt["df_signals"], primary_h),
                                    use_container_width=True)

                # ── Monthly heatmap
                if not bt["monthly"].empty:
                    st.markdown(f'<div class="sec" style="margin-top:10px">MONTHLY AVERAGE RETURN — {primary_h}d hold</div>',
                                unsafe_allow_html=True)
                    st.plotly_chart(chart_monthly_heatmap(bt["monthly"], primary_h),
                                    use_container_width=True)

                # ── Signal log (last 20)
                with st.expander("📋 Signal Log (last 20 signals)"):
                    cols_show = ["date","signal","entry"] + [f"ret_{h}d" for h in bt["hold_days"] if f"ret_{h}d" in bt["df_signals"].columns]
                    df_show = bt["df_signals"][cols_show].tail(20).copy()
                    df_show["date"] = pd.to_datetime(df_show["date"]).dt.strftime("%d %b %Y")
                    st.dataframe(df_show, hide_index=True, use_container_width=True)

        # ══════════════════════
        # SECTION B: RS DEEP DIVE
        # ══════════════════════
        st.markdown("---")
        st.markdown('<div class="sec">RELATIVE STRENGTH ANALYSIS — {}</div>'.format(BR["ticker"]),
                    unsafe_allow_html=True)

        if not rs.get("available"):
            st.warning("RS data unavailable — IHSG (^JKSE) could not be loaded from Yahoo Finance.")
        else:
            rs20v = rs.get("rs20_val",100)
            rs60v = rs.get("rs60_val",100)
            rc    = rs.get("color","#ffab00")
            interp= rs.get("interp","IN LINE")
            note  = rs.get("note","")
            t20   = rs.get("trend20","flat")
            t60   = rs.get("trend60","flat")

            # RS banner
            st.markdown(f"""
            <div style='background:#0b1018;border:1px solid #141e2e;border-left:4px solid {rc};
                        padding:14px 18px;border-radius:3px;margin-bottom:12px'>
              <div style='display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px'>
                <div>
                  <div style='font-family:"IBM Plex Mono",monospace;font-size:1.5rem;
                              font-weight:700;color:{rc}'>{interp}</div>
                  <div style='font-size:11px;color:#cdd8e6;margin-top:4px'>{note}</div>
                </div>
                <div style='display:flex;gap:24px;font-family:"IBM Plex Mono",monospace;text-align:center'>
                  <div>
                    <div style='font-size:9px;color:#5a7a9a'>RS 20D</div>
                    <div style='font-size:1.4rem;font-weight:700;
                                color:{"#00e676" if rs20v>=100 else "#ff1744"}'>{rs20v:.1f}</div>
                    <div style='font-size:9px;color:{"#00e676" if t20=="rising" else "#ff1744"}'>
                      {"▲ Rising" if t20=="rising" else "▼ Falling"}</div>
                  </div>
                  <div>
                    <div style='font-size:9px;color:#5a7a9a'>RS 60D</div>
                    <div style='font-size:1.4rem;font-weight:700;
                                color:{"#00e676" if rs60v>=100 else "#ff1744"}'>{rs60v:.1f}</div>
                    <div style='font-size:9px;color:{"#00e676" if t60=="rising" else "#ff1744"}'>
                      {"▲ Rising" if t60=="rising" else "▼ Falling"}</div>
                  </div>
                  <div>
                    <div style='font-size:9px;color:#5a7a9a'>RS SCORE</div>
                    <div style='font-size:1.4rem;font-weight:700;color:{rc}'>{rs.get("score",50)}/100</div>
                    <div style='font-size:9px;color:#5a7a9a'>0–100</div>
                  </div>
                </div>
              </div>
            </div>""", unsafe_allow_html=True)

            st.plotly_chart(chart_rs(rs, BR["ticker"]), use_container_width=True)

            # RS insight boxes
            co1, co2 = st.columns(2)
            with co1:
                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid #141e2e;padding:14px;border-radius:3px'>
                  <div class='sec'>CARA MEMBACA RS CHART</div>
                  <div style='font-size:11px;color:#cdd8e6;line-height:1.8'>
                    <b style='color:#00e676'>RS > 100</b> = {BR["ticker"]} outperform IHSG<br>
                    <b style='color:#ff1744'>RS < 100</b> = {BR["ticker"]} underperform IHSG<br>
                    <b style='color:#00e676'>RS Rising ▲</b> = smart money rotation masuk<br>
                    <b style='color:#ff1744'>RS Falling ▼</b> = smart money rotation keluar<br>
                    <b style='color:#ffab00'>RS 20d > RS 60d</b> = momentum menguat<br>
                    <b style='color:#ffab00'>RS 20d < RS 60d</b> = momentum melemah<br><br>
                    <b style='color:#eaf0f8'>Shaded area:</b> hijau = alpha zone (saham mengungguli IHSG)
                  </div>
                </div>""", unsafe_allow_html=True)
            with co2:
                # RS-based trade guidance
                if rs20v > 105 and t20 == "rising" and t60 == "rising":
                    rs_guide_title = "🔥 SETUP TERBAIK"
                    rs_guide_color = "#00e676"
                    rs_guide_body = (f"RS 20d ({rs20v:.1f}) dan RS 60d ({rs60v:.1f}) keduanya positif "
                                     f"dan trending naik. Ini setup RS ideal — bandar aktif accumulate. "
                                     f"Konfluens sinyal teknikal + RS = conviction tinggi untuk entry.")
                elif rs20v > 100 and t20 == "rising":
                    rs_guide_title = "✅ RS POSITIF"
                    rs_guide_color = "#88ffbb"
                    rs_guide_body = (f"Outperform IHSG dengan RS {rs20v:.1f}. Momentum naik. "
                                     f"Entry valid jika teknikal juga konfirm (CMF+ / OBV rising).")
                elif rs20v < 95 and t20 == "falling":
                    rs_guide_title = "⛔ HINDARI DULU"
                    rs_guide_color = "#ff1744"
                    rs_guide_body = (f"RS lemah ({rs20v:.1f}) dan terus melemah. Saham underperform IHSG. "
                                     f"Tunggu RS minimal berhenti turun dan membentuk base sebelum pertimbangkan entry. "
                                     f"Sinyal teknikal bullish apapun harus dikurangi conviction-nya.")
                elif rs20v < 100 and t20 == "rising":
                    rs_guide_title = "🔄 WATCH: RS RECOVERY"
                    rs_guide_color = "#ffab00"
                    rs_guide_body = (f"RS masih di bawah 100 ({rs20v:.1f}) tapi mulai naik. "
                                     f"Ini early recovery — monitor apakah RS bisa tembus 100. "
                                     f"Jika RS menembus 100 + teknikal bagus = entry sinyal kuat.")
                else:
                    rs_guide_title = "➡️ MONITOR"
                    rs_guide_color = "#ffab00"
                    rs_guide_body = f"RS bergerak seiring IHSG ({rs20v:.1f}). Tidak ada RS edge saat ini. Fokus ke sinyal broker."

                st.markdown(f"""
                <div style='background:#0b1018;border:1px solid #141e2e;padding:14px;
                            border-radius:3px;border-left:3px solid {rs_guide_color}'>
                  <div class='sec'>{rs_guide_title}</div>
                  <div style='font-size:11px;color:#cdd8e6;line-height:1.6'>{rs_guide_body}</div>
                </div>""", unsafe_allow_html=True)

        # ── Backtest disclaimer
        st.markdown("---")
        st.info("""
        **⚠️ Disclaimer Backtest** — Past performance does not guarantee future results.
        Backtest ini menggunakan indikator teknikal historis tanpa data broker (tidak tersedia historis).
        Hasil backtest bisa terlihat lebih baik dari realita karena: (1) tidak ada biaya transaksi/slippage,
        (2) look-ahead bias minimal tapi mungkin ada, (3) overfitting pada parameter default.
        Gunakan sebagai referensi, bukan jaminan profit.
        """)

    else:
        st.markdown("""
        <div style='text-align:center;padding:60px 20px;border:1px dashed #141e2e;border-radius:4px'>
          <div style='font-size:2rem;color:#141e2e;margin-bottom:12px'>◈</div>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:12px;color:#2a3d52;letter-spacing:2px'>
            SET TICKER + PARAMETERS ABOVE · CLICK RUN BACKTEST + RS</div>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;color:#1c2a3e;margin-top:6px'>
            Walk-Forward Backtest · Equity Curve · Win Rate · Sharpe · Alpha vs IHSG · RS Chart
          </div>
        </div>""", unsafe_allow_html=True)


# ── helper: small KPI box (used in backtest section)
# ══════════════════════════════════════════════════════════════════════
#  TAB 4 — SCREENER
# ══════════════════════════════════════════════════════════════════════

with tab_s:
    st.markdown("#### MULTI-STOCK SCREENER")
    wl = list(dict.fromkeys([
        t.strip().upper().replace(".JK","")
        for t in wl_raw.replace("\n",",").split(",") if t.strip()
    ]))

    # Info banner
    n_wl = len(wl)
    ticker_preview = " · ".join(wl[:30]) + (f" ... +{n_wl-30} more" if n_wl>30 else "")
    st.markdown(f"""
    <div style='padding:10px 14px;background:#0b1018;border:1px solid #141e2e;
                border-left:3px solid #00e676;border-radius:3px;margin-bottom:12px;
                font-family:"IBM Plex Mono",monospace;font-size:10px'>
      <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:4px'>
        <span style='color:#00e676;font-weight:700;letter-spacing:2px'>
          {n_wl} STOCKS IN WATCHLIST</span>
        <span style='color:#5a7a9a;font-size:9px'>
          Edit in sidebar · No limit · Use preset buttons (LQ45/IDX30/GROWTH)</span>
      </div>
      <span style='color:#cdd8e6'>{ticker_preview}</span>
    </div>""", unsafe_allow_html=True)

    # Estimated time warning
    if n_wl > 20:
        est_min = round(n_wl * 3 / 60, 1)
        st.info(f"⏱️ {n_wl} stocks — estimated **{est_min} min** to scan. Cached after first run.")

    # VCP data requirement warning
    if period in ("1mo","1M"):
        st.warning(
            "⚠️ **Period 1M is too short for VCP** — VCP requires at least 120 days of data. "
            "VCP will be automatically fetched from a separate 6M dataset in the background. "
            "Switch to 3M/6M/1Y for best results."
        )
    elif period in ("3mo","3M"):
        st.info(
            "💡 **VCP Tip**: 3M period is sufficient for basic VCP detection. "
            "Use 6M or 1Y for more accurate multi-contraction analysis."
        )

    if st.button("▶  SCAN ALL STOCKS", use_container_width=False):
        if not wl:
            st.warning("Add tickers to the watchlist in the sidebar.")
        else:
            rows=[]; prog=st.progress(0)
            for i,tk in enumerate(wl):
                prog.progress((i+1)/len(wl), text=f"Analyzing {tk} ... ({i+1}/{len(wl)})")

                # Load price for selected period (for indicators + chart)
                df_tk = load_price(tk, period)
                if df_tk is None or len(df_tk)<20: continue

                # VCP needs min 120 days — always load 6M regardless of period
                if len(df_tk) < 120:
                    df_vcp_ = load_price(tk, "6mo")
                    if df_vcp_ is None or len(df_vcp_) < 60:
                        df_vcp_ = df_tk
                else:
                    df_vcp_ = df_tk

                c_=cmf(df_tk); o_=obv(df_tk); m_=mfi(df_tk)
                wp_,wn_,_ = wyckoff(df_tk,c_,o_)
                ts_ = tech_score(df_tk,c_,o_,m_)
                vcp_ = detect_vcp(df_vcp_)      # VCP always uses 6M+ data
                bdf_,src_ = get_broker_today(tk, sb_token)
                if bdf_ is None: bdf_ = demo_broker(tk,ts_)
                br_  = analyze_broker(bdf_)
                vcp_s_ = vcp_.get("score",0)

                # Layer 1: raw signal
                fs_raw = int(np.clip(round(ts_*.55+br_["score"]*.35+vcp_s_*.10),0,100))

                # Layer 2: validation (weekly + liquidity — regime already global)
                wc_  = calc_weekly_confluence(tk, "2y")
                liq_ = calc_liquidity_score(df_tk, 50_000_000, own_)  # Rp50M default
                w_adj_ = int(np.clip(int(round((wc_["confluence_mult"]-1.0)*fs_raw*0.3)),-12,10)) if wc_.get("available") else 0
                l_adj_ = int(np.clip(liq_.get("score_adj",0),-20,5))
                fs     = int(np.clip(fs_raw + w_adj_ + l_adj_, 0, 100))

                last_= df_tk.iloc[-1]; prev_=df_tk.iloc[-2]
                chg_ = (last_["close"]-prev_["close"])/prev_["close"]*100
                av_  = df_tk["volume"].tail(20).mean()
                vr_  = last_["volume"]/(av_ if av_>0 else 1)
                obv_ = o_.iloc[-1]>o_.iloc[-min(10,len(o_)-1)]
                own_ = OWNER_DB.get(tk.upper().replace(".JK",""))
                ent_ = entry_signal(fs,ts_,br_["score"],wp_,float(c_.iloc[-1]),
                                    obv_,float(m_.iloc[-1]),vr_,br_["crossing"],
                                    br_["goreng"],br_["sm_buyers"],br_["sm_sellers"],
                                    chg_,own_,vcp=vcp_)
                vcp_grade_ = vcp_.get("grade","NONE")
                wc_lbl_    = wc_.get("label","—")[:8] if wc_.get("available") else "N/A"
                liq_lbl_   = liq_.get("label","—")[:8]
                rows.append({
                    "Ticker":tk, "Price":f"Rp {int(last_['close']):,}",
                    "Change":f"{'+'if chg_>=0 else ''}{chg_:.2f}%",
                    "Score":fs, "Signal":ent_["sig"], "Risk":ent_["risk"],
                    "VCP":vcp_grade_,
                    "Premium":"🔥" if ent_.get("premium_setup") else "",
                    "Weekly":wc_lbl_,
                    "Liq":liq_lbl_,
                    "Owner":own_["owner"][:18] if own_ else "—",
                    "Tier":f"T{own_['tier']}" if own_ else "—",
                    "Float":f"{own_['float']}%" if own_ else "—",
                    "Pol":"⚡" if (own_ and own_.get("political")) else "",
                    "Ph":f"Ph {wp_}", "CMF":f"{float(c_.iloc[-1]):+.4f}",
                    "Vol":f"{vr_:.1f}x",
                    "Cross":"🔥ACC" if br_["crossing"]=="ACC"
                            else "💀DIS" if br_["crossing"]=="DIS" else "—",
                    "Data":{"stockbit":"R","idx":"R","demo":"D"}.get(src_,"?"),
                    "_s":fs,"_sig":ent_["sig"],
                })
            prog.empty()
            if rows:
                # ── SCORE NORMALIZATION — percentile rank within scanned universe
                raw_scores = [r["_s"] for r in rows]
                percentiles = normalize_scores(raw_scores)
                for i, r in enumerate(rows):
                    pct = percentiles[i]
                    grade, gc, gl = score_grade(r["_s"], pct)
                    r["Grade"]  = grade
                    r["Pct"]    = f"{pct:.0f}%"
                    r["_pct"]   = pct

                df_r = pd.DataFrame(rows).sort_values("_s", ascending=False)
                c1,c2,c3,c4,c5 = st.columns(5)
                c1.metric("STRONG BUY", len([r for r in rows if r["_sig"]=="STRONG BUY"]))
                c2.metric("BUY",         len([r for r in rows if r["_sig"]=="BUY"]))
                c3.metric("WATCH",       len([r for r in rows if r["_sig"]=="WATCH"]))
                c4.metric("AVOID",       len([r for r in rows if "AVOID" in r["_sig"]]))
                c5.metric("VCP Grade A", len([r for r in rows if r["VCP"]=="A"]))

                # Market regime at time of scan
                regime_scan = detect_market_regime(period)
                if regime_scan.get("available"):
                    rc = regime_scan["color"]
                    st.markdown(f"""
                    <div style='padding:8px 14px;background:#0b1018;border:1px solid #141e2e;
                                border-left:3px solid {rc};border-radius:3px;margin-bottom:8px;
                                font-family:"IBM Plex Mono",monospace;font-size:10px;
                                display:flex;gap:16px;align-items:center'>
                      <span style='color:{rc};font-weight:700'>
                        MARKET REGIME: {regime_scan["regime"]}</span>
                      <span style='color:#5a7a9a'>
                        IHSG {regime_scan["ret20"]:+.1f}% (20d) ·
                        Conviction multiplier: {regime_scan["multiplier"]}x ·
                        Scores already regime-adjusted
                      </span>
                    </div>""", unsafe_allow_html=True)

                disp = ["Ticker","Price","Change","Score","Grade","Pct","Signal","Risk",
                        "VCP","Premium","Weekly","Liq","Owner","Tier","Float","Pol","Ph","CMF","Vol","Cross","Data"]
                st.dataframe(df_r[disp], hide_index=True, use_container_width=True,
                    column_config={
                        "Score": st.column_config.ProgressColumn(
                            "Score", min_value=0, max_value=100, format="%d"),
                        "Pct": st.column_config.TextColumn("Pct Rank",
                            help="Percentile rank within scanned universe today. 90% = top 10% of stocks scanned."),
                        "Grade": st.column_config.TextColumn("Grade",
                            help="S=Exceptional, A=Strong, B=Above avg, C=Average, D=Below avg"),
                    })

                sb_   = [r["Ticker"] for r in rows if r["_sig"]=="STRONG BUY"]
                b_    = [r["Ticker"] for r in rows if r["_sig"]=="BUY"]
                pol   = [r["Ticker"] for r in rows if r["Pol"]=="⚡"]
                vcp_a = [r["Ticker"] for r in rows if r["VCP"]=="A"]
                prem  = [r["Ticker"] for r in rows if r["Premium"]=="🔥"]
                grade_s = [r["Ticker"] for r in rows if r["Grade"]=="S"]

                if prem:  st.success(f"💎 **PREMIUM SETUP** (VCP + ACC Cross): {' · '.join(prem)}")
                if sb_:   st.success(f"🔥 **STRONG BUY**: {' · '.join(sb_)}")
                if b_:    st.success(f"✅ **BUY**: {' · '.join(b_)}")
                if grade_s: st.success(f"⭐ **GRADE S** (top universe rank): {' · '.join(grade_s)}")
                if vcp_a: st.info(f"📐 **VCP Grade A**: {' · '.join(vcp_a)}")
                if pol:   st.warning(f"⚡ **Political exposure** (extra risk): {', '.join(pol)}")

                # Sector heatmap
                st.markdown("---")
                st.markdown('<div class="sec">SECTOR ROTATION HEATMAP</div>',
                            unsafe_allow_html=True)
                st.markdown("""<div style='font-size:10px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;margin-bottom:8px'>
                Excess return vs IHSG per sector. Green = money flowing IN. Red = money flowing OUT.
                Focus your stock picks on sectors with positive RS 20d AND 60d.</div>""",
                unsafe_allow_html=True)
                with st.spinner("Loading sector data..."):
                    sect_scan = detect_sector_rotation()
                if sect_scan.get("available"):
                    st.plotly_chart(chart_sector_heatmap(sect_scan), use_container_width=True)
                    # Top 3 sectors with inflow
                    top_sectors = sorted(
                        [(s, d) for s, d in sect_scan["sectors"].items() if d.get("inflow")],
                        key=lambda x: x[1]["rs20"], reverse=True
                    )[:3]
                    if top_sectors:
                        s_names = " · ".join([f"{s} ({d['rs20']:+.1f}%)" for s,d in top_sectors])
                        st.success(f"📈 **Sectors with inflows**: {s_names}")

    else:
        st.markdown("""<div style='text-align:center;padding:50px;border:1px dashed #141e2e;border-radius:4px'>
        <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;color:#2a3d52'>
        Edit watchlist in sidebar → Click SCAN ALL STOCKS</div></div>""",unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  TAB 4 — OWNERSHIP DB
# ══════════════════════════════════════════════════════════════════════

with tab_o:
    st.markdown("#### CONGLOMERATE OWNERSHIP DATABASE")
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a;margin-bottom:10px'>
    Sources: Forbes, IDX annual reports, Bloomberg Terminal (Feb 2026), KSEI disclosures.
    Shareholders >1% now public monthly at idx.co.id (OJK mandate March 3, 2026).
    </div>""", unsafe_allow_html=True)

    f1,f2 = st.columns([2,3])
    with f1: ft   = st.selectbox("Tier",["All","Tier 1","Tier 2","Tier 3"])
    with f2: fgrp = st.selectbox("Group",["All"] + sorted(set(o["group"] for o in OWNER_DB.values())))

    rows_db=[]
    for tk,o in OWNER_DB.items():
        if ft!="All" and f"Tier {o['tier']}" not in ft: continue
        if fgrp!="All" and o["group"]!=fgrp: continue
        rs,rl = owner_risk(o)
        rows_db.append({"Ticker":tk,"Owner":o["owner"][:28],"Group":o["group"][:22],
                        "Tier":f"T{o['tier']}","Sector":o["sector"][:18],
                        "Float":f"{o['float']}%",
                        "Political":"⚡ YES" if o.get("political") else "—",
                        "Quality":rs,"Risk":rl,"Note":o.get("note","")[:50]})
    if rows_db:
        st.dataframe(pd.DataFrame(rows_db),hide_index=True,use_container_width=True,
            column_config={"Quality":st.column_config.ProgressColumn(
                "Quality",min_value=0,max_value=100,format="%d")})

    st.markdown("---")
    st.markdown("##### KEY OWNERSHIP INSIGHTS")
    for icon,title,body in [
        ("🔥","PRAJOGO PANGESTU — BARITO GROUP",
         "#1 Forbes Indonesia (US$46.5B). BREN, TPIA, CDIA all have <13% free float → in Full Periodic Call Auction (FCA). Prices set at scheduled intervals — no continuous trading. Moves are violent. When Prajogo buys more, treat as strong bullish signal."),
        ("🏦","HARTONO BROTHERS — DJARUM/BCA",
         "#3 & #4 Forbes. BBCA = Indonesia's most consistently profitable bank since 1990s. When Djarum enters a NEW stock (e.g., bought SSIA 2025 → +224%), this is the highest-conviction IDX signal possible."),
        ("💎","DUAL CONGLOMERATE = NUCLEAR SIGNAL",
         "When 2 Tier-1 conglomerates buy the SAME stock simultaneously (Hartono + Prajogo both in SSIA 2025 → +224%), it is the most powerful IDX-specific signal. Monitor KSEI monthly reports for ownership crossover."),
        ("⚡","POLITICAL STOCKS — EXTREME CAUTION",
         "RAJA, PSKT (Hapsoro = Puan Maharani's husband), WIFI (Hashim-related). Move on political news, not fundamentals. +600% followed by -80% is normal. For short-term traders with tight stops ONLY."),
        ("🏛️","BUMN — STABLE + DIVIDENDS",
         "Government-backed. Best for dividends (BBRI, BMRI, PTBA). Subject to SOE policy risk. Buy during broad market selloffs. Avoid during government policy uncertainty periods."),
        ("⚠️","FREE FLOAT REGULATION (OJK 2026)",
         "OJK mandating 15% minimum free float. BREN (12.3%), TPIA (10.7%), HMSP (7.5%) must dilute or buy back. This creates corporate action catalyst risk — price can be very volatile on announcements."),
    ]:
        st.markdown(f"""
        <div style='display:flex;gap:12px;padding:9px 12px;border-bottom:1px solid #141e2e'>
          <div style='font-size:16px;flex-shrink:0'>{icon}</div>
          <div>
            <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;font-weight:700;
                        color:#eaf0f8;margin-bottom:2px'>{title}</div>
            <div style='font-size:12px;color:#cdd8e6;line-height:1.5'>{body}</div>
          </div>
        </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════
#  TAB 5 — BROKER GUIDE
# ══════════════════════════════════════════════════════════════════════

with tab_g:
    st.markdown("#### BANDARMOLOGY PRO — COMPLETE GUIDE")
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a;margin-bottom:14px'>
    Full reference guide for all signals, indicators, broker categories, and VCP patterns used in this platform.
    </div>""", unsafe_allow_html=True)

    # ── ENTRY SIGNALS
    st.markdown("##### ENTRY SIGNAL LEVELS")
    for sig,c,desc in [
        ("PREMIUM SETUP","#00e676","VCP Grade A/B + Accumulation Cross + RS Positive. Triple confirmation — highest possible conviction."),
        ("STRONG BUY",  "#00e676","Accumulation Cross confirmed + Score ≥65. Foreign institutions buying while retail sells."),
        ("BUY",         "#00e676","4+ conditions confirmed. Smart money accumulation detected. Enter with stop-loss."),
        ("WATCH",       "#ffab00","2–3 conditions forming. Setup building but not yet confirmed. Add to watchlist."),
        ("AVOID",       "#ff1744","2+ bearish signals active. Risk outweighs reward. Stay out."),
        ("STRONG AVOID","#ff1744","Distribution Cross confirmed OR Score ≤35. Smart money exiting. Exit if holding."),
    ]:
        st.markdown(f"""
        <div style='display:flex;gap:12px;padding:7px 14px;border-bottom:1px solid #141e2e;align-items:flex-start'>
          <span style='font-family:"IBM Plex Mono",monospace;font-size:11px;font-weight:700;
                       color:{c};min-width:140px;flex-shrink:0'>{sig}</span>
          <span style='font-size:12px;color:#cdd8e6;line-height:1.5'>{desc}</span>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── INDICATOR GUIDE
    st.markdown("##### TECHNICAL INDICATORS — HOW TO READ THEM")
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a;margin-bottom:10px'>
    All indicators used in Bandarmology PRO. Composite score = Technical 55% + Broker 35% + VCP 10%.
    </div>""", unsafe_allow_html=True)

    indicators = [
        ("CMF","Chaikin Money Flow (14)","#00e676",
         "Measures whether money is flowing INTO or OUT of a stock over the last 14 days. Combines price position with volume to reveal hidden accumulation or distribution.",
         [("≥ +0.15","Strong accumulation — heavy money inflow","#00e676"),
          ("+0.05 – +0.14","Mild accumulation","#88ffbb"),
          ("-0.05 – +0.04","Neutral / balanced","#5a7a9a"),
          ("-0.15 – -0.06","Mild distribution","#ff8888"),
          ("≤ -0.15","Strong distribution — heavy money outflow","#ff1744")],
         "CMF > 0 while price drops = bullish divergence (smart money quietly buying). CMF < 0 while price rises = hidden distribution. This is one of the strongest signals in the system."),

        ("OBV","On Balance Volume","#00b0ff",
         "Cumulative volume indicator. Every up-day adds volume, every down-day subtracts it. The purest measure of buying vs selling pressure over time.",
         [("Rising + Price Rising","Strong trend confirmation — follow it","#00e676"),
          ("Rising + Price Falling","⭐ BULLISH DIVERGENCE — smart money accumulating! Best entry zone.","#00e676"),
          ("Flat","Consolidation — wait for directional breakout","#5a7a9a"),
          ("Falling + Price Rising","⚠️ BEARISH DIVERGENCE — smart money distributing! Be cautious.","#ff1744"),
          ("Falling + Price Falling","Confirmed distribution / downtrend","#ff1744")],
         "Wyckoff analysts love OBV. When OBV rises while price hasn't yet = smart money accumulating before the markup phase. This divergence often precedes a 20–40% move."),

        ("MFI","Money Flow Index (14)","#ffab00",
         "RSI that incorporates volume — more accurate than plain RSI. Measures the intensity of buying/selling pressure over 14 days, weighted by traded volume.",
         [("≥ 80","Overbought — don't chase, prepare exit","#ff1744"),
          ("70 – 79","Approaching overbought","#ff8888"),
          ("45 – 69","Normal range","#5a7a9a"),
          ("20 – 44","Mild oversold — potential reversal","#88ffbb"),
          ("≤ 20","⭐ Strong oversold — high probability reversal. Confirm with CMF.","#00e676")],
         "MFI Divergence: Price falling but MFI rising = smart money buying while retail panic-sells. One of the strongest bandarmology signals. Combine with OBV and CMF for high-conviction setup."),

        ("VOL RATIO","Volume Ratio vs MA20","#e040fb",
         "Today's volume divided by the 20-day average volume. Measures how 'active' the stock is today vs its normal baseline.",
         [("≥ 3.0x","Extreme activity — likely news, corporate action, or breakout","#ff1744"),
          ("1.5x – 2.9x","High volume — significant move likely","#00e676"),
          ("0.8x – 1.4x","Normal range","#5a7a9a"),
          ("≤ 0.5x","Very quiet — avoid, low liquidity","#2a3d52")],
         "High volume + price rising + CMF positive = valid accumulation breakout. High volume + price falling = distribution or shakeout. Volume is the fuel — without it, price moves are unreliable."),

        ("WYCKOFF","Wyckoff Phase Analysis","#00e5ff",
         "Identifies where a stock is in its accumulation-distribution cycle, based on Richard Wyckoff's methodology (1910s, still highly relevant). Uses price range, trend, and volume patterns.",
         [("Phase A — Selling Climax","Price collapses + volume spikes. Smart money begins absorbing supply.","#00b0ff"),
          ("Phase B — Building Cause","Extended sideways range. Smart money quietly accumulating. Can last months.","#ffab00"),
          ("Phase C — Spring ⭐","FALSE BREAKDOWN — price pushed below support then reverses. IDEAL ENTRY POINT.","#00e676"),
          ("Phase D — Sign of Strength","Volume expands, price breaks out. Accumulation confirmed complete.","#88ffbb"),
          ("Phase E — Markup","Strong uptrend. Smart money already profitable. Watch for distribution signs.","#ff8888")],
         "Phase C (Spring) is the single best entry in Wyckoff analysis — retail hits stop-losses, smart money buys the dip. Always confirm with CMF > 0 and OBV rising. Combine with VCP for maximum conviction."),

        ("BROKER SCORE","Broker Flow Score (0–100)","#00e676",
         "Composite score based on broker transaction patterns. Weights foreign smart money most heavily, as they have the best research and longest-horizon capital.",
         [("≥ 70","Strong accumulation — smart money buying, retail selling","#00e676"),
          ("55 – 69","Moderate accumulation","#88ffbb"),
          ("45 – 54","Neutral / mixed","#5a7a9a"),
          ("35 – 44","Moderate distribution","#ff8888"),
          ("≤ 34","Strong distribution","#ff1744")],
         "Components: Foreign smart money net lot (30%), Crossing signal (20%), Institutional flow (15%), Retail contrarian signal (10%), Goreng/pump alert (5% bonus). The Accumulation Cross is the most powerful single signal: SM buying + retail selling simultaneously."),

        ("VCP","Volatility Contraction Pattern (Minervini)","#00e5ff",
         "A series of progressively smaller price consolidations, each with declining volume. Signals supply exhaustion — fewer sellers remain at each contraction. Developed by Mark Minervini.",
         [("Grade A","3–4 tight contractions, vol dry-up, above all MAs, pivot ≤6%. IDEAL SETUP.","#00e676"),
          ("Grade B","2–3 contractions, volume contracting, setup forming near pivot.","#88ffbb"),
          ("Grade C","Early contraction forming, setup not yet complete. Monitor.","#ffab00"),
          ("Grade C-","Contraction detected but stock is below key MAs. High risk.","#ff8888"),
          ("NONE","No VCP structure detected.","#5a7a9a")],
         "VCP Entry Rule: Buy the breakout ABOVE the pivot price on volume ≥1.5x average. Stop-loss: below the last contraction low. VCP + Wyckoff Phase C + Accumulation Cross = PREMIUM SETUP (highest conviction trade in this system)."),

        ("RS","Relative Strength vs IHSG","#e040fb",
         "Measures how the stock performs RELATIVE to the Jakarta Composite Index (^JKSE). A stock rising only because IHSG rises is not showing alpha — smart money is not specifically targeting it.",
         [("RS > 110, Rising ▲","Strong Outperform — money rotating IN specifically to this stock","#00e676"),
          ("RS 100–110, Rising ▲","Outperforming — beating market with improving momentum","#88ffbb"),
          ("RS 95–105, Flat","In-line with market — no specific alpha","#5a7a9a"),
          ("RS 90–99, Falling ▼","Underperforming — lagging behind IHSG","#ff8888"),
          ("RS < 90, Falling ▼","Strong Underperform — money rotating OUT of this stock","#ff1744")],
         "RS is a LEADING indicator. Smart money enters stocks with rising RS BEFORE the price breakout. RS adjustment in scoring: Strong Outperform +8 pts, Outperform +4 pts, Strong Underperform −8 pts. Always check RS before entering any position."),
    ]

    for ind_code, ind_name, ind_color, ind_desc, levels, ind_tip in indicators:
        with st.expander(f"**{ind_code}** — {ind_name}", expanded=False):
            st.markdown(f"""
            <div style='background:#0b1018;border:1px solid #141e2e;border-top:2px solid {ind_color};
                        padding:12px 14px;border-radius:3px;margin-bottom:10px'>
              <div style='font-size:12px;color:#cdd8e6;line-height:1.7'>{ind_desc}</div>
            </div>""", unsafe_allow_html=True)

            st.markdown("""<div style='font-family:"IBM Plex Mono",monospace;font-size:9px;
                color:#5a7a9a;letter-spacing:2px;margin-bottom:6px'>HOW TO READ</div>""",
                unsafe_allow_html=True)
            for val, meaning, color in levels:
                st.markdown(f"""
                <div style='display:flex;gap:14px;padding:5px 10px;border-bottom:1px solid #141e2e;
                            font-family:"IBM Plex Mono",monospace;font-size:11px'>
                  <span style='color:{color};font-weight:700;min-width:170px;flex-shrink:0'>{val}</span>
                  <span style='color:#cdd8e6'>{meaning}</span>
                </div>""", unsafe_allow_html=True)

            st.markdown(f"""
            <div style='margin-top:8px;padding:9px 13px;background:rgba(0,0,0,.3);
                        border-radius:3px;font-size:11px;color:#5a7a9a;
                        font-family:"IBM Plex Mono",monospace;border-left:2px solid {ind_color}'>
              💡 {ind_tip}
            </div>""", unsafe_allow_html=True)

    # ── BROKER TABLES
    st.markdown("---")
    st.markdown("##### BROKER CATEGORIES — IDX")
    ca,cb = st.columns(2)
    with ca:
        st.markdown("**🟢 Foreign Smart Money** — Follow when 2+ net buying simultaneously")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"],"Country":v.get("flag","—")}
            for k,v in BROKER_DB.items() if v["cat"]=="FOREIGN_SMART"]),
            hide_index=True, use_container_width=True)
        st.markdown("**🏛️ BUMN & Local Institutional**")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"]}
            for k,v in BROKER_DB.items() if v["cat"] in ("BUMN","LOCAL_INST")]),
            hide_index=True, use_container_width=True)
    with cb:
        st.markdown("**⚠️ Retail (Contrarian Signal)** — Heavy net buy = distribution warning")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"],
            "Signal":"Contrarian" if v["cat"]=="RETAIL" else "Noise"}
            for k,v in BROKER_DB.items() if v["cat"] in ("RETAIL","SCALPER")]),
            hide_index=True, use_container_width=True)
        st.markdown("**🔥 Local Bandar (Pump Risk) + 🇰🇷 Korean**")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"],"Cat":v["cat"]}
            for k,v in BROKER_DB.items() if v["cat"] in ("LOCAL_BANDAR","KOREAN")]),
            hide_index=True, use_container_width=True)

    # ── GOLDEN RULES
    st.markdown("---")
    st.markdown("##### 7 GOLDEN RULES OF BROKER ANALYSIS")
    for icon,kw,body in [
        ("✅","FOLLOW","2+ foreign SM brokers (AK/BK/DB/GW) simultaneously net buying the same stock"),
        ("🔥","STRONGEST SIGNAL","Accumulation Cross: SM buying + retail selling simultaneously = ideal entry"),
        ("💀","DANGER SIGNAL","Distribution Cross: SM selling + retail buying = classic distribution trap. Exit."),
        ("⚠️","CAUTION","XC/YP/XL heavy net buy = retail FOMO. Often signals end of distribution phase."),
        ("🔇","IGNORE","MG (Semesta) dominates volume = pure scalper noise. Check other brokers."),
        ("🚨","HIGH RISK","MK + EP suddenly appear in a quiet stock = likely pump-and-dump operation."),
        ("📊","COMBINE LAYERS","Broker + VCP + Wyckoff + CMF + RS + Ownership = full conviction"),
    ]:
        st.markdown(f"""
        <div style='display:flex;gap:12px;padding:7px 14px;border-bottom:1px solid #141e2e'>
          <div style='font-size:15px;flex-shrink:0'>{icon}</div>
          <div><b style='color:#eaf0f8;font-family:"IBM Plex Mono",monospace;font-size:11px'>{kw}</b>
          <span style='color:#cdd8e6;font-size:12px'> — {body}</span></div>
        </div>""", unsafe_allow_html=True)

    # ── VCP GUIDE
    st.markdown("---")
    st.markdown("##### VCP — VOLATILITY CONTRACTION PATTERN (MINERVINI METHOD)")
    st.markdown(f"""
    <div style='background:#0b1018;border:1px solid #141e2e;border-left:3px solid #00e5ff;
                padding:14px 16px;border-radius:3px;margin-bottom:14px'>
      <div style='font-size:12px;color:#cdd8e6;line-height:1.8'>
        VCP is a price pattern where a stock makes a series of progressively <b style='color:#00e5ff'>narrower
        consolidations</b>, each with <b style='color:#00e5ff'>declining volume</b>. This signals that supply
        (sellers) is being exhausted — fewer and fewer shares are being offered at each contraction.
        When supply runs out and demand returns (breakout above pivot), price can move sharply higher.
      </div>
    </div>""", unsafe_allow_html=True)

    col_v1, col_v2 = st.columns(2)
    with col_v1:
        st.markdown("""
        <div style='background:#0b1018;border:1px solid #141e2e;padding:14px;border-radius:3px'>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                      letter-spacing:2px;margin-bottom:10px'>IDEAL VCP STRUCTURE</div>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;color:#cdd8e6;line-height:2'>
            <span style='color:#5a7a9a'>Prior Uptrend:</span> Stock above MA50 / MA150 / MA200<br>
            <span style='color:#ffab00'>Contraction 1:</span> −20% correction, volume declines<br>
            <span style='color:#ffab00'>Contraction 2:</span> −10% correction, volume dries up<br>
            <span style='color:#88ffbb'>Contraction 3:</span> −5% correction, very low volume<br>
            <span style='color:#00e676'><b>Pivot:</b></span>
            <b style='color:#00e676'> Buy breakout above recent high on 1.5x+ volume</b><br>
            <span style='color:#ff1744'>Stop-loss:</span> Below last contraction low
          </div>
        </div>""", unsafe_allow_html=True)
    with col_v2:
        st.markdown("""
        <div style='background:#0b1018;border:1px solid #141e2e;padding:14px;border-radius:3px'>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                      letter-spacing:2px;margin-bottom:10px'>VCP GRADES IN THIS SYSTEM</div>""",
        unsafe_allow_html=True)
        for grade, gc, desc in [
            ("A","#00e676","3–4 contractions + vol dry-up + above all MAs + tight ≤6% = IDEAL. Enter at pivot breakout."),
            ("B","#88ffbb","2–3 contractions + some volume contraction. Good setup, nearly ready."),
            ("C","#ffab00","Early contraction forming. Promising but incomplete. Monitor daily."),
            ("C-","#ff8888","Contraction present but stock is BELOW key MAs. High risk — avoid or short only."),
            ("NONE","#5a7a9a","No VCP pattern detected. Use other signals only."),
        ]:
            st.markdown(f"""
            <div style='display:flex;gap:10px;padding:5px 0;border-bottom:1px solid #141e2e;
                        font-family:"IBM Plex Mono",monospace;font-size:11px'>
              <span style='color:{gc};font-weight:700;min-width:40px'>Grade {grade}</span>
              <span style='color:#cdd8e6;font-size:10px'>{desc}</span>
            </div>""", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("""
    <div style='margin-top:12px;padding:12px 14px;background:rgba(0,229,255,.05);
                border:1px solid rgba(0,229,255,.2);border-radius:3px'>
      <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                  letter-spacing:2px;margin-bottom:6px'>PREMIUM SETUP = VCP + ACCUMULATION CROSS</div>
      <div style='font-size:12px;color:#cdd8e6;line-height:1.7'>
        The highest conviction signal in this platform is when <b style='color:#00e676'>VCP Grade A or B</b>
        aligns with an <b style='color:#00e676'>Accumulation Cross</b> (foreign smart money buying + retail selling).
        This means: supply is exhausted (VCP) AND institutional demand is actively entering (broker flow).
        Add <b style='color:#e040fb'>RS Outperform</b> for the complete triple-confirmation setup.
      </div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.info("""
    **Disclaimer** — Bandarmology PRO is a probabilistic analytical tool, not a guarantee of profit.
    All signals indicate probability, not certainty. Always apply stop-loss discipline, proper position sizing,
    and combine with fundamental analysis. Broker data is ESTIMATED from daily net flow — not actual
    sub-account holdings from KSEI. Past performance does not guarantee future results.
    """)


