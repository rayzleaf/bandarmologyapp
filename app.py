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
    a   = atr(df); lp  = float(df["close"].iloc[-1])
    la  = float(a.iloc[-1]) if not pd.isna(a.iloc[-1]) else lp*.02
    sup = float(df["low"].tail(10).min())
    res = float(df["high"].tail(20).max())
    sl  = round(sup*.97,0)
    el  = round(sup*1.005,0)
    eh  = round(min(lp*1.02,sup*1.03),0)
    t1  = round(res*.98,0)
    risk= lp-sl; reward=t1-lp
    return dict(el=el,eh=eh,sl=sl,t1=t1,
                risk_pct=round((lp-sl)/lp*100,1),
                rr=round(reward/risk,1) if risk>0 else 0,
                sup=sup,res=res)

@st.cache_data(ttl=3600, show_spinner=False)
def fundamentals(ticker):
    try:
        info = yf.Ticker(ticker.upper().replace(".JK","") + ".JK").info
        mc   = info.get("marketCap",0)
        so   = info.get("sharesOutstanding",0)
        return dict(
            market_cap  = f"Rp {mc/1e12:.1f}T" if mc>=1e12 else f"Rp {mc/1e9:.1f}B" if mc>=1e9 else "—",
            pe          = round(info.get("trailingPE",0),1) or "—",
            pb          = round(info.get("priceToBook",0),2) or "—",
            div         = f"{info.get('dividendYield',0)*100:.2f}%" if info.get("dividendYield") else "—",
            hi52        = info.get("fiftyTwoWeekHigh"),
            lo52        = info.get("fiftyTwoWeekLow"),
            curr        = info.get("currentPrice",info.get("regularMarketPrice")),
            shares_out  = so,
        )
    except: return {}


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
    Berapa hari trading untuk hitung posisi kumulatif broker</div>""", unsafe_allow_html=True)
    accu_days = st.select_slider("Trading days", [5,10,15,20,30,45,60,90], value=20)

    st.markdown("---")
    st.markdown("**SCREENER WATCHLIST**")
    st.markdown("""
    <div style='font-size:10px;color:#5a7a9a;font-family:"IBM Plex Mono",monospace;margin-bottom:6px'>
    Ketik kode saham IDX apa saja, pisahkan koma.<br>
    <b style='color:#00e676'>Tidak ada batasan jumlah</b> — bisa 5 sampai 100+ saham.
    </div>""", unsafe_allow_html=True)

    # Preset watchlist buttons
    col_p1, col_p2, col_p3 = st.columns(3)
    with col_p1:
        load_lq45   = st.button("LQ45",      use_container_width=True, key="btn_lq45")
    with col_p2:
        load_idx30  = st.button("IDX30",     use_container_width=True, key="btn_idx30")
    with col_p3:
        load_growth = st.button("GROWTH",    use_container_width=True, key="btn_growth")

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

    if load_lq45:
        st.session_state["wl_val"] = LQ45_WL
    elif load_idx30:
        st.session_state["wl_val"] = IDX30_WL
    elif load_growth:
        st.session_state["wl_val"] = GROWTH_WL

    wl_raw = st.text_area(
        "Tickers (comma-separated)",
        value=st.session_state.get("wl_val", DEFAULT_WL),
        height=130,
        key="wl_area",
    )

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
            vcp = detect_vcp(df)        # VCP detection
        with st.spinner("Fetching broker summary (today) ..."):
            bdf, src = get_broker_today(ticker_input, sb_token)
            if bdf is None:
                bdf = demo_broker(ticker_input, ts); src = "demo"
            br = analyze_broker(bdf)
        with st.spinner("Fetching fundamentals · ownership · shareholders ..."):
            fund   = fundamentals(ticker_input)
            own    = OWNER_DB.get(ticker_input.upper().replace(".JK",""))
            ksei   = fetch_ksei_composition(ticker_input)
            sh_list, sh_src, sh_date = fetch_shareholders(ticker_input)
        with st.spinner("Computing Relative Strength vs IHSG (^JKSE) ..."):
            ihsg_df = load_ihsg(period)
            rs_data = calc_rs(df, ihsg_df) if ihsg_df is not None else {"available": False}

        # ── COMPOSITE SCORE: Technical 55% + Broker 35% + VCP 10%
        vcp_score  = vcp.get("score", 0)
        final = int(np.clip(round(ts*.55 + br["score"]*.35 + vcp_score*.10), 0, 100))
        last  = df.iloc[-1]; prev = df.iloc[-2]
        chg   = (last["close"]-prev["close"]) / prev["close"] * 100
        av    = df["volume"].tail(20).mean()
        vr    = last["volume"] / av if av>0 else 1
        ob_up = o_.iloc[-1] > o_.iloc[-min(10,len(o_)-1)]
        ent   = entry_signal(final,ts,br["score"],wp,float(c_.iloc[-1]),
                              ob_up,float(m_.iloc[-1]),vr,br["crossing"],
                              br["goreng"],br["sm_buyers"],br["sm_sellers"],chg,own,
                              rs_data=rs_data, vcp=vcp)
        st.session_state["res"] = dict(
            df=df,c=c_,o=o_,m=m_,wp=wp,wn=wn,wd=wd,ts=ts,
            br=br,bdf=bdf,src=src,final=final,ent=ent,
            lp=float(last["close"]),chg=chg,vr=vr,
            cmf_v=float(c_.iloc[-1]),mfi_v=float(m_.iloc[-1]),
            obv_up=ob_up,ticker=ticker_input,
            fund=fund,own=own,ksei=ksei,
            sh_list=sh_list,sh_src=sh_src,sh_date=sh_date,
            ez=ez,rs_data=rs_data,vcp=vcp,
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

        # ── KEY METRICS — compute live statuses
        obv_status = "Rising ▲" if R["obv_up"] else "Falling ▼"
        obv_color  = "#00e676" if R["obv_up"] else "#ff1744"
        vr_now     = R["vr"]
        vr_color   = "#00e676" if vr_now>=1.5 else "#ffab00" if vr_now>=0.8 else "#5a7a9a"
        vr_label   = "Tinggi" if vr_now>=1.5 else "Normal" if vr_now>=0.8 else "Sepi"
        cmf_color  = "#00e676" if R["cmf_v"]>0.05 else "#ff1744" if R["cmf_v"]<-0.05 else "#ffab00"
        cmf_label  = ("Akumulasi kuat"  if R["cmf_v"]>0.15 else
                      "Akumulasi ringan" if R["cmf_v"]>0.05 else
                      "Distribusi kuat"  if R["cmf_v"]<-0.15 else
                      "Distribusi ringan" if R["cmf_v"]<-0.05 else "Netral")
        mfi_color  = "#ff1744" if R["mfi_v"]>70 else "#00e676" if R["mfi_v"]<30 else "#5a7a9a"
        mfi_label  = "Overbought ⚠️" if R["mfi_v"]>70 else "Oversold ✅" if R["mfi_v"]<30 else "Normal"
        wyck_color = {"A":"#00b0ff","B":"#ffab00","C":"#00e676","D":"#ffab00","E":"#ff8888"}.get(R["wp"],"#5a7a9a")
        comp_color = "#00e676" if R["final"]>=65 else "#ff1744" if R["final"]<=35 else "#ffab00"
        comp_label = "Akumulasi" if R["final"]>=65 else "Distribusi" if R["final"]<=35 else "Neutral"
        ts_color   = "#00e676" if R["ts"]>=65 else "#ff1744" if R["ts"]<=35 else "#ffab00"
        br_color   = "#00e676" if br["score"]>=65 else "#ff1744" if br["score"]<=35 else "#ffab00"

        st.markdown('<div class="sec" style="margin-top:12px">KEY METRICS & INDICATOR LEGEND</div>',
                    unsafe_allow_html=True)

        # ── Top score row (3 big numbers)
        st.markdown(f"""
        <div style='display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px'>

          <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                      padding:14px 16px;display:flex;justify-content:space-between;align-items:center'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                          letter-spacing:1.5px;margin-bottom:4px'>COMPOSITE SCORE</div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:2rem;
                          font-weight:700;color:{comp_color};line-height:1'>{R["final"]}/100</div>
              <div style='font-size:10px;color:{comp_color};margin-top:4px'>{comp_label}</div>
            </div>
            <div style='text-align:right;font-size:10px;color:#5a7a9a;line-height:2'>
              <span style='color:#00e676'>≥ 65</span> Akumulasi<br>
              <span style='color:#ffab00'>35–64</span> Neutral<br>
              <span style='color:#ff1744'>≤ 35</span> Distribusi
            </div>
          </div>

          <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                      padding:14px 16px;display:flex;justify-content:space-between;align-items:center'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                          letter-spacing:1.5px;margin-bottom:4px'>TECHNICAL SCORE</div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:2rem;
                          font-weight:700;color:{ts_color};line-height:1'>{R["ts"]}/100</div>
              <div style='font-size:10px;color:#5a7a9a;margin-top:4px'>
                CMF · OBV · MFI · Wyckoff · Volume</div>
            </div>
            <div style='text-align:right;font-size:10px;color:#5a7a9a;line-height:2'>
              Bobot 55%<br>dari skor akhir
            </div>
          </div>

          <div style='background:#0b1018;border:1px solid #141e2e;border-radius:4px;
                      padding:14px 16px;display:flex;justify-content:space-between;align-items:center'>
            <div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:9px;color:#5a7a9a;
                          letter-spacing:1.5px;margin-bottom:4px'>BROKER SCORE</div>
              <div style='font-family:"IBM Plex Mono",monospace;font-size:2rem;
                          font-weight:700;color:{br_color};line-height:1'>{br["score"]}/100</div>
              <div style='font-size:10px;color:#5a7a9a;margin-top:4px'>
                Net lot asing · Crossing · Kategori</div>
            </div>
            <div style='text-align:right;font-size:10px;color:#5a7a9a;line-height:2'>
              Bobot 35%<br>dari skor akhir
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

            # Entry zone
            st.markdown('<div class="sec">ENTRY ZONE</div>', unsafe_allow_html=True)
            if ent["sig"] in ("STRONG BUY","BUY","WATCH"):
                st.markdown(f"""
                <div class='entry-box'>
                  <div style='font-size:9px;color:#5a7a9a;letter-spacing:2px'>ENTRY RANGE</div>
                  <div style='color:#00e676;font-size:1rem;font-weight:700;margin-top:2px'>
                    Rp {ez["el"]:,.0f} – {ez["eh"]:,.0f}</div>
                  <div style='font-size:9px;color:#5a7a9a;margin-top:6px'>STOP-LOSS</div>
                  <div style='color:#ff1744;font-size:.95rem;font-weight:700'>
                    Rp {ez["sl"]:,.0f}
                    <span style='font-size:9px;color:#5a7a9a'> (−{ez["risk_pct"]}%)</span></div>
                  <div style='font-size:9px;color:#5a7a9a;margin-top:6px'>TARGET 1</div>
                  <div style='color:#00b0ff;font-size:.95rem;font-weight:700'>
                    Rp {ez["t1"]:,.0f}</div>
                  <div style='margin-top:6px;padding-top:6px;border-top:1px solid rgba(0,230,118,.15);
                              font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a'>
                    Risk/Reward: <span style='color:#00e676;font-weight:700'>{ez["rr"]}:1</span>
                  </div>
                </div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class='stop-box'>
                  <div style='font-size:10px;color:#ff1744;margin-bottom:5px'>DO NOT ENTER</div>
                  <div style='font-size:11px;color:#5a7a9a;line-height:1.6'>
                    Support: Rp {ez["sup"]:,.0f}<br>
                    Resistance: Rp {ez["res"]:,.0f}
                  </div>
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
        col_f, col_k = st.columns(2)

        with col_f:
            st.markdown('<div class="sec">FUNDAMENTAL SNAPSHOT</div>', unsafe_allow_html=True)
            if fund:
                st.markdown('<div style="background:#0b1018;border:1px solid #141e2e;padding:12px;border-radius:3px">', unsafe_allow_html=True)
                for k,v in [("Market Cap",fund.get("market_cap","—")),
                             ("P/E Ratio", fund.get("pe","—")),
                             ("P/B Ratio", fund.get("pb","—")),
                             ("Dividend Yield",fund.get("div","—")),
                             ("From 52W High","Rp {:,.0f}".format(fund["hi52"]) if fund.get("hi52") else "—"),
                             ("52W Low","Rp {:,.0f}".format(fund["lo52"]) if fund.get("lo52") else "—"),
                             ("Shares Outstanding",f"{fund.get('shares_out',0)/1e9:.2f}B" if fund.get('shares_out') else "—")]:
                    st.markdown(f'<div class="kv"><span class="kv-k">{k}</span><span class="kv-v">{v}</span></div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
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

        with col_k:
            st.markdown('<div class="sec">KSEI — FOREIGN / DOMESTIC COMPOSITION</div>', unsafe_allow_html=True)
            if ksei and ksei.get("foreign"):
                fp = ksei["foreign"]; dp = ksei.get("domestic",100-fp)
                fig_k = go.Figure(go.Pie(labels=["Foreign","Domestic"],values=[fp,dp],
                    hole=0.5,marker=dict(colors=["#00b0ff","#00e676"],
                    line=dict(color="#05080c",width=2)),
                    textfont=dict(color="#cdd8e6",size=11,family="IBM Plex Mono"),
                    hovertemplate="%{label}: %{value:.1f}%<extra></extra>"))
                fig_k.update_layout(**CL,height=220,showlegend=True,
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
                    st.info("📌 High foreign ownership (>55%) — stock is on MSCI/global fund radar. Foreign flow is the dominant price driver.")
                elif fp < 20:
                    st.info("📌 Low foreign ownership (<20%) — mostly domestic players. Monitor local institutional brokers (CC, DX, LG).")
            else:
                st.markdown("""
                <div style='padding:20px;text-align:center;font-family:"IBM Plex Mono",monospace;
                            font-size:11px;color:#2a3d52'>
                  KSEI composition unavailable.<br>
                  Check <a href="https://web.ksei.co.id" target="_blank" style="color:#5a7a9a">web.ksei.co.id</a>
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

    if st.button("▶  SCAN ALL STOCKS", use_container_width=False):
        if not wl:
            st.warning("Add tickers to the watchlist in the sidebar.")
        else:
            rows=[]; prog=st.progress(0)
            for i,tk in enumerate(wl):
                prog.progress((i+1)/len(wl), text=f"Analyzing {tk} ... ({i+1}/{len(wl)})")
                df_tk = load_price(tk, period)
                if df_tk is None or len(df_tk)<20: continue
                c_=cmf(df_tk); o_=obv(df_tk); m_=mfi(df_tk)
                wp_,wn_,_ = wyckoff(df_tk,c_,o_)
                ts_ = tech_score(df_tk,c_,o_,m_)
                vcp_ = detect_vcp(df_tk)        # VCP for each screener stock
                bdf_,src_ = get_broker_today(tk, sb_token)
                if bdf_ is None: bdf_ = demo_broker(tk,ts_)
                br_  = analyze_broker(bdf_)
                vcp_s_ = vcp_.get("score",0)
                fs   = int(np.clip(round(ts_*.55+br_["score"]*.35+vcp_s_*.10),0,100))
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
                rows.append({
                    "Ticker":tk, "Price":f"Rp {int(last_['close']):,}",
                    "Change":f"{'+'if chg_>=0 else ''}{chg_:.2f}%",
                    "Score":fs, "Signal":ent_["sig"], "Risk":ent_["risk"],
                    "VCP":vcp_grade_,
                    "Premium":"🔥" if ent_.get("premium_setup") else "",
                    "Owner":own_["owner"][:20] if own_ else "—",
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
                df_r = pd.DataFrame(rows).sort_values("_s",ascending=False)
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("STRONG BUY",len([r for r in rows if r["_sig"]=="STRONG BUY"]))
                c2.metric("BUY",        len([r for r in rows if r["_sig"]=="BUY"]))
                c3.metric("WATCH",      len([r for r in rows if r["_sig"]=="WATCH"]))
                c4.metric("AVOID",      len([r for r in rows if "AVOID" in r["_sig"]]))
                disp=["Ticker","Price","Change","Score","Signal","Risk",
                      "VCP","Premium","Owner","Tier","Float","Pol","Ph","CMF","Vol","Cross","Data"]
                st.dataframe(df_r[disp],hide_index=True,use_container_width=True,
                    column_config={"Score":st.column_config.ProgressColumn(
                        "Score",min_value=0,max_value=100,format="%d")})
                sb_=[r["Ticker"] for r in rows if r["_sig"]=="STRONG BUY"]
                b_= [r["Ticker"] for r in rows if r["_sig"]=="BUY"]
                pol=[r["Ticker"] for r in rows if r["Pol"]=="⚡"]
                vcp_a=[r["Ticker"] for r in rows if r["VCP"]=="A"]
                prem=[r["Ticker"] for r in rows if r["Premium"]=="🔥"]
                if sb_: st.success(f"🔥 **STRONG BUY**: {' · '.join(sb_)}")
                if b_:  st.success(f"✅ **BUY**: {' · '.join(b_)}")
                if vcp_a: st.success(f"⭐ **VCP Grade A** (ideal contraction): {' · '.join(vcp_a)}")
                if prem:  st.success(f"💎 **PREMIUM SETUP** (VCP + ACC Cross): {' · '.join(prem)}")
                if pol: st.warning(f"⚡ **Political exposure** (extra risk): {', '.join(pol)}")
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
    st.markdown("#### BROKER INTELLIGENCE GUIDE")

    st.markdown("##### SIGNAL LEVELS")
    for sig,c,desc in [
        ("STRONG BUY","#00e676","ACC Cross + Score ≥65. Highest conviction. Enter with full position."),
        ("BUY","#00e676","4+ conditions. Good R/R. Enter with proper stop-loss."),
        ("WATCH","#ffab00","2–3 conditions forming. Add to watchlist."),
        ("AVOID","#ff1744","2+ bearish. Don't enter."),
        ("STRONG AVOID","#ff1744","Distribution Cross / Score ≤35. Exit if holding."),
    ]:
        st.markdown(f"""
        <div style='display:flex;gap:12px;padding:6px 12px;border-bottom:1px solid #141e2e'>
          <div style='font-family:"IBM Plex Mono",monospace;font-size:11px;font-weight:700;
                      color:{c};min-width:130px'>{sig}</div>
          <div style='font-size:12px;color:#cdd8e6'>{desc}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    ca,cb = st.columns(2)
    with ca:
        st.markdown("##### 🟢 Foreign Smart Money")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"],"Country":v.get("flag","—")}
            for k,v in BROKER_DB.items() if v["cat"]=="FOREIGN_SMART"]),
            hide_index=True,use_container_width=True)
        st.markdown("##### 🏛️ BUMN & Local Institutional")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"]}
            for k,v in BROKER_DB.items() if v["cat"] in ("BUMN","LOCAL_INST")]),
            hide_index=True,use_container_width=True)
    with cb:
        st.markdown("##### ⚠️ Retail (Contrarian) + ⚡ Scalper")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"],
            "Signal":"Contrarian" if v["cat"]=="RETAIL" else "Noise"}
            for k,v in BROKER_DB.items() if v["cat"] in ("RETAIL","SCALPER")]),
            hide_index=True,use_container_width=True)
        st.markdown("##### 🔥 Local Bandar (Pump) + 🇰🇷 Korean")
        st.dataframe(pd.DataFrame([{"Code":k,"Broker":v["name"],"Cat":v["cat"]}
            for k,v in BROKER_DB.items() if v["cat"] in ("LOCAL_BANDAR","KOREAN")]),
            hide_index=True,use_container_width=True)

    st.markdown("---")
    st.markdown("##### 7 GOLDEN RULES")
    for icon,kw,body in [
        ("✅","FOLLOW","2+ foreign SM brokers (AK/BK/DB/GW) simultaneously net buying"),
        ("🔥","STRONGEST","ACC Cross: SM buying + retail selling = ideal entry"),
        ("💀","DANGER","DIS Cross: SM selling + retail buying = distribution trap"),
        ("⚠️","CAUTION","XC/YP/XL heavy net buy = retail FOMO, often distribution top"),
        ("🔇","IGNORE","MG dominates volume = scalper noise; look at other brokers"),
        ("🚨","HIGH RISK","MK+EP appear in quiet stock = likely pump-and-dump"),
        ("📊","COMBINE","Broker + Wyckoff + CMF + Ownership + Fundamentals = full picture"),
    ]:
        st.markdown(f"""
        <div style='display:flex;gap:10px;padding:7px 12px;border-bottom:1px solid #141e2e'>
          <div style='font-size:14px;flex-shrink:0'>{icon}</div>
          <div><b style='color:#eaf0f8'>{kw}</b> —
          <span style='color:#cdd8e6;font-size:12px'>{body}</span></div>
        </div>""", unsafe_allow_html=True)

    # ── INDICATOR GUIDE
    st.markdown("---")
    st.markdown("##### 📐 INDIKATOR TEKNIKAL — PENJELASAN LENGKAP")
    st.markdown("""
    <div style='font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5a7a9a;margin-bottom:10px'>
    Semua indikator yang digunakan dalam Bandarmology PRO beserta cara membacanya.
    </div>""", unsafe_allow_html=True)

    indicators = [
        ("CMF","Chaikin Money Flow (14)","#00e676",
         "Mengukur apakah uang mengalir masuk atau keluar dari saham selama 14 hari.",
         [("≥ +0.15","Akumulasi kuat — uang deras masuk","#00e676"),
          ("+0.05 – +0.14","Akumulasi ringan","#88ffbb"),
          ("-0.05 – +0.04","Netral / sideways","#5a7a9a"),
          ("-0.15 – -0.06","Distribusi ringan","#ff8888"),
          ("≤ -0.15","Distribusi kuat — uang deras keluar","#ff1744")],
         "CMF > 0 saat harga turun = divergensi bullish (bandar diam-diam beli). CMF < 0 saat harga naik = distribusi tersembunyi."),

        ("OBV","On Balance Volume","#00b0ff",
         "Akumulasi kumulatif volume beli vs jual. Indikator paling murni untuk deteksi smart money.",
         [("OBV Naik + Harga Naik","Konfirmasi trend kuat — ikuti","#00e676"),
          ("OBV Naik + Harga Turun","⭐ Divergensi Bullish — bandar BELI diam-diam! Entry zone.","#00e676"),
          ("OBV Datar","Konsolidasi — tunggu konfirmasi","#5a7a9a"),
          ("OBV Turun + Harga Naik","⚠️ Divergensi Bearish — bandar JUAL diam-diam! Waspadai.","#ff1744"),
          ("OBV Turun + Harga Turun","Distribusi terkonfirmasi","#ff1744")],
         "OBV adalah favorit Wyckoff analyst. Ketika OBV naik tapi harga belum naik = bandar sedang akumulasi sebelum markup."),

        ("MFI","Money Flow Index (14)","#ffab00",
         "Seperti RSI tapi memperhitungkan volume — lebih akurat untuk deteksi oversold/overbought.",
         [("≥ 80","Overbought — jangan beli baru, siapkan exit","#ff1744"),
          ("70 – 79","Mendekati overbought","#ff8888"),
          ("45 – 69","Normal range","#5a7a9a"),
          ("20 – 44","Oversold ringan — potensi reversal","#88ffbb"),
          ("≤ 20","⭐ Oversold kuat — potensi reversal besar, cek CMF konfirmasi","#00e676")],
         "MFI Divergence: Harga turun tapi MFI naik = bandar beli saat retail panik jual. Ini salah satu sinyal terkuat."),

        ("VOL RATIO","Volume Ratio (vs MA20)","#e040fb",
         "Volume hari ini dibagi rata-rata volume 20 hari. Mengukur seberapa 'ramai' aktivitas hari ini.",
         [("≥ 3x","Aktivitas ekstrem — kemungkinan news/aksi korporasi","#ff1744"),
          ("1.5x – 2.9x","Volume tinggi — sinyal pergerakan signifikan","#00e676"),
          ("0.8x – 1.4x","Volume normal","#5a7a9a"),
          ("≤ 0.5x","Volume sangat sepi — hindari, likuiditas rendah","#2a3d52")],
         "Volume tinggi + harga naik + CMF positif = akumulasi valid. Volume tinggi + harga turun = distribusi atau shakeout."),

        ("WYCKOFF","Wyckoff Phase Analysis","#00e5ff",
         "Mengidentifikasi posisi saham dalam siklus akumulasi-distribusi Richard Wyckoff (1910s, masih relevan).",
         [("Phase A — Selling Climax","Harga jatuh + volume meledak. Bandar mulai serap supply.","#00b0ff"),
          ("Phase B — Building Cause","Sideways panjang. Bandar kumpul diam-diam. Bisa berbulan-bulan.","#ffab00"),
          ("Phase C — Spring ⭐","FALSE BREAKDOWN — harga ditekan lalu balik. ENTRY IDEAL!","#00e676"),
          ("Phase D — Sign of Strength","Volume + harga breakout. Konfirmasi akumulasi selesai.","#88ffbb"),
          ("Phase E — Markup","Harga trending naik. Bandar sudah untung. Waspadai distribusi.","#ff8888")],
         "Phase C (Spring) adalah entry paling ideal dalam Wyckoff — retail terkena stop-loss, bandar lanjut beli. Selalu konfirmasi dengan CMF dan OBV."),

        ("BROKER SCORE","Broker Flow Score","#00e676",
         "Skor 0–100 berdasarkan aktivitas broker. Mengkombinasikan banyak faktor broker flow.",
         [("≥ 70","Akumulasi kuat — smart money beli, retail jual","#00e676"),
          ("55 – 69","Akumulasi moderat","#88ffbb"),
          ("45 – 54","Netral","#5a7a9a"),
          ("35 – 44","Distribusi moderat","#ff8888"),
          ("≤ 34","Distribusi kuat","#ff1744")],
         "Faktor: net lot foreign smart money (bobot 30%), crossing signal (bobot 20%), institutional flow (bobot 15%), retail kontrarian (bobot 10%), goreng alert (bonus 5%)."),
    ]

    for ind_code, ind_name, ind_color, ind_desc, levels, ind_tip in indicators:
        with st.expander(f"**{ind_code}** — {ind_name}", expanded=False):
            st.markdown(f"""
            <div style='background:#0b1018;border:1px solid #141e2e;border-top:2px solid {ind_color};
                        padding:12px;border-radius:3px;margin-bottom:10px'>
              <div style='font-size:12px;color:#cdd8e6;line-height:1.6'>{ind_desc}</div>
            </div>""", unsafe_allow_html=True)

            st.markdown('<div style="font-family:IBM Plex Mono,monospace;font-size:9px;color:#5a7a9a;letter-spacing:2px;margin-bottom:6px">CARA MEMBACA</div>', unsafe_allow_html=True)
            for val,meaning,color in levels:
                st.markdown(f"""
                <div style='display:flex;gap:12px;padding:5px 10px;border-bottom:1px solid #141e2e;
                            font-family:"IBM Plex Mono",monospace;font-size:11px'>
                  <span style='color:{color};font-weight:700;min-width:160px'>{val}</span>
                  <span style='color:#cdd8e6'>{meaning}</span>
                </div>""", unsafe_allow_html=True)

            st.markdown(f"""
            <div style='margin-top:8px;padding:8px 12px;background:rgba(0,0,0,.3);
                        border-radius:3px;font-size:11px;color:#5a7a9a;
                        font-family:"IBM Plex Mono",monospace;border-left:2px solid {ind_color}'>
              💡 {ind_tip}
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>",unsafe_allow_html=True)
    st.info("""
    **Important** — Bandarmology adalah probabilistic tool, bukan prediksi pasti.
    Selalu gunakan stop-loss, position sizing yang tepat, dan kombinasikan dengan analisis fundamental.
    Data broker = ESTIMASI berdasarkan net flow harian — bukan actual sub-account holdings KSEI.
    """)

