# ─────────────────────────────────────────────────────────────────────────────
# ZN Onboarding Tool Kit — Streamlit in Snowflake
# Packages (add in Snowsight → Packages panel): openpyxl, rapidfuzz
# ─────────────────────────────────────────────────────────────────────────────

import streamlit as st
st.set_page_config(page_title="ZN Onboarding Tool Kit", page_icon="🚢", layout="wide")

import pandas as pd
import json, io, re
import openpyxl          # noqa: F401
from typing import Dict, List, Tuple, Optional
from rapidfuzz import fuzz, process as fuzz_process

try:
    from snowflake.snowpark.context import get_active_session
    _session = get_active_session()
    IN_SF = True
except Exception:
    _session = None
    IN_SF = False

EXPLORER_SCHEMA = "prod_access.mda_reports"

# ─────────────────────────────────────────────────────────────────────────────
# MDA SCHEMA
# ─────────────────────────────────────────────────────────────────────────────
MDA: Dict[str, List[str]] = {
    "REPORTS": [
        "CUSTOMER_ID","CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","TYPE",
        "SOURCE_DATA_ARRIVED_AT","INGESTION_ID","UPDATED_AT","DELETED","REPORT_TYPE",
        "VOYAGE_NR","IMO","DATETIME_GMT","DATETIME_LOCAL","PERIOD_START_GMT","PERIOD_END_GMT",
        "VESSEL_CONDITION","CARGO_WEIGHT","DWT","LOGGED_DISTANCE","OBSERVED_DISTANCE",
        "ORIGIN_PORT","ORIGIN_PORT_UNLO_CODE","DESTINATION_PORT","DESTINATION_PORT_UNLO_CODE",
        "DESTINATION_PORT_PURPOSE","CURRENT_PORT","CURRENT_PORT_UNLO_CODE","CURRENT_PORT_PURPOSE",
        "TIME_SPENT_AT_ANCHOR","TIME_SPENT_IN_ICE","TIME_SPENT_MANOEUVERING","TIME_SPENT_DRIFTING",
        "SPEED_LOG_WORKING","PERIOD_DURATION_HOURS","WATER_COVERED_IN_ICE","DISTANCE_IN_ICE",
        "REPORT_MODE","OPERATING_CONDITION","SUB_OPERATING_CONDITION","SUB_REPORT_TYPE",
        "VOYAGE_NUMBER_FROM_REPORT","SHIP_TO_SHIP_OPERATION","TOTAL_CARGO_WEIGHT_ON_BOARD",
        "BUNKER_DELIVERY_NUMBER","BUNKER_DELIVERY_DATE","BUNKERING_PORT","CONDITION_PORT_STAY_REASON",
        "START_DATE_LOCAL_TIME","SOURCE_ID","ESTIMATED_TIME_DEPARTURE_FROM_BERTH_DATE",
        "VOYAGER_INSTRUCTION","INSTRUCTED_SPEED","INSTRUCTED_RPM",
        "INSTRUCTED_FUELCONSUMPTION_PER_24HOURS","FRESH_WATER_REMAINING_ON_BOARD",
        "BALLAST_WATER_QUANTITY","REMARKS","CAPTAIN_NAME","CHIEF_ENGINEER",
        "REMAINING_DISTANCE_TO_EOSP","CONDITION_ETD","IS_DQT_VALID","VESSEL_NAME",
        "APP_VERSION_NUMBER","OUTSIDE_PORT_LIMIT","NEXT_PORT_FOR_ORDER","NEXT_PORT_ETA",
        "OFF_HIRE_REASON","PORT_LATITUDE","PORT_LONGITUDE","DISPLACEMENT",
        "CONSUMPTION_IS_SYNTHETIC","DISTANCE_IS_SYNTHETIC",
    ],
    "BUNKER": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT","BUNKER_TYPE",
        "AMOUNT","DENSITY","FUEL_TEMPERATURE","LCV","ROB_AFTER_BUNKERING","ROB_BEFORE_BUNKERING",
        "SULPHUR","VISCOCITY","CO2CONVERSIONFACTOR","PERCENTAGEOFBIO","FUEL_LOSS_AMOUNT",
        "FUEL_LOSS_REASON","SLUDGE_AMOUNT","SLUDGE_LOSS_AMOUNT","SLUDGE_LOSS_REASON",
        "IS_SOUNDING","WELL_TO_TANK_EMISSIONS","BATCH_ID","TANK",
        "SUSTAINABILITY_CERTIFICATE_RECEIVED","FUEL_GRADE",
    ],
    "NAVIGATION": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT","TIME_SAILING",
        "SPEED_OVER_GROUND","SPEED_THROUGH_WATER","DRAFT_AFTER","DRAFT_FORE","LONGITUDE",
        "LATITUDE","COURSE","REMAINING_DISTANCE_TO_PILOT_STATION","AVERAGE_PITCH_ANGLE",
        "AVERAGE_ROLL_ANGLE","MAX_PITCH_ANGLE","MAX_ROLL_ANGLE","SPEED_IMPACT_BY_CURRENT",
    ],
    "CONSUMPTION": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "CONSUMPTION_AS_OF_DATE","FUEL_TYPE","UNIT","ENGINE_TYPE","CONSUMPTION","LCV_VALUE",
        "SULPHUR_PERCENT","ME_SFOC","CO2_CONVERSION_FACTOR","CONSUMPTION_VALUE_IN_ICE",
        "DENSITY","VISCOSITY","OPERATING_CONDITION","OPERATING_CONDITION_ID",
        "SLUDGE_INCINERATED_QTY","BATCH_ID","TANK","FUEL_GRADE","BASED_ON_FLOW_METER_READING",
    ],
    "SUB_CONSUMPTION": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "CONSUMPTION_AS_OF_DATE","ENGINE_TYPE","SUB_CONSUMER_NAME","SUB_CONSUMER_FUEL_TYPE",
        "SUB_CONSUMER_CONSUMPTION",
    ],
    "ELECTRICAL_CONSUMPTION": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "CONSUMPTION_AS_OF_DATE","CONSUMER","UNIT","CONSUMPTION","CONSUMER_CUSTOM_NAME",
    ],
    "MAIN_ENGINE": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT","NAME","RPM",
        "MAIN_ENGINE_OUTPUT","POWER_FROM_ME_INDICATOR","POWER_FROM_TORSIOMETER","TURBO_CHARGER",
        "FUEL_INDEX","MAIN_ENGINE_RUNNING_HOURS","MAIN_ENGINE_LOADING_PERCENT","ENGINE_DISTANCE",
        "PROPELLER_PITCH","PROPELLER_SHAFT_GEAR_RATIO","SLIP_PERCENT",
    ],
    "EQUIPMENT": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "REEFER_CONTAINER_SFOC","REEFER_CONTAINER_FUEL_TYPE","AUX_ENGINE_LOADING_PERCENT",
        "EQUIPMENT_NAME","ENERGY_PRODUCTION","RUNNING_HOURS","EQUIPMENT_TYPE",
        "EQUIPMENT_CUSTOM_NAME","STEAM_PRESSURE","FEED_WATER_TEMPERATURE",
    ],
    "FOROB": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT","FOROB_AS_OF_DATE",
        "FUEL_TYPE","ROB","SLUDGE_QTY","UNIT","PREV_ROB","BATCH_ID","TANK","FUEL_GRADE",
    ],
    "WEATHER": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT","WIND_FORCE",
        "WIND_DIRECTION","WAVE_HEIGHT","WAVE_DIRECTION","WAVE_LENGTH","CURRENT_DIRECTION",
        "CURRENT","WATER_TEMPERATURE","AIR_TEMPERATURE","SWELL_HEIGHT","SWELL_DIRECTION",
        "SWELL_PERIOD","WATER_DENSITY","BEAUFORT","TRUE_WIND_SPEED",
    ],
    "CARGO": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "TOTAL_NUMBER_OF_REEFERS","TIME_SPENT_LOADING","TIME_SPENT_UNLOADING",
        "NUMBER_OF_CHILLED_20FT_REEFERS","NUMBER_OF_CHILLED_40FT_REEFERS",
        "NUMBER_OF_FROZEN_20FT_REEFERS","NUMBER_OF_FROZEN_40FT_REEFERS",
        "DEADWEIGHT_CARRIED","VOLUME_OF_CARGO","NUMBER_OF_TEUS","CARGO_HEIGHT",
        "CHARTERER_OWN_CARGO_QUANTITY","TOTAL_QUANTITY_ON_BOARD","BILL_OF_LADING_DATE",
        "CARGO_DESCRIPTION","DENSITY",
    ],
    "EVENTS": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","EVENT","UPDATED_AT",
        "START_DATETIME","END_DATETIME","SUBJECT","TYPE","CURRENT_PORT",
    ],
    "MISC_INFO": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "CURRENT_PORT_COUNTRY_CODE","CURRENT_PORT_PORT_LIMITS","CURRENT_PORT_LATITUDE",
        "CURRENT_PORT_LONGITUDE","ORIGIN_PORT_COUNTRY_CODE","ORIGIN_PORT_PORT_LIMITS",
        "ORIGIN_PORT_LATITUDE","ORIGIN_PORT_LONGITUDE","DESTINATION_PORT_COUNTRY_CODE",
        "DESTINATION_PORT_PORT_LIMITS","DESTINATION_PORT_LATITUDE","DESTINATION_PORT_LONGITUDE",
    ],
    "OPERATING_CONDITION_INFO": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "OPERATING_CONDITION","PERIOD_START_GMT","PERIOD_END_GMT","DISTANCE",
        "ASSOCIATED_ACTIVITY","OPERATING_CONDITION_ID",
    ],
    "ACTIVITY_LOG": [
        "CUSTOMER_NAME","DATA_SOURCE","REPORT_ID","EXTERNAL_ID","UPDATED_AT",
        "ACTIVITY_TYPE","ACTIVITY_NAME","ACTIVITY_START_TIME","ACTIVITY_END_TIME",
        "REMARK","REASON",
    ],
}

ALL_MDA = ["(Skip / No Mapping)"] + [
    f"{t}.{f}" for t, fs in MDA.items() for f in fs
]

HINTS: Dict[str, Tuple[str, str]] = {
    "sog":("NAVIGATION","SPEED_OVER_GROUND"),"stw":("NAVIGATION","SPEED_THROUGH_WATER"),
    "lat":("NAVIGATION","LATITUDE"),"lon":("NAVIGATION","LONGITUDE"),
    "lng":("NAVIGATION","LONGITUDE"),"hdg":("NAVIGATION","COURSE"),
    "heading":("NAVIGATION","COURSE"),"draft_aft":("NAVIGATION","DRAFT_AFTER"),
    "draft_fwd":("NAVIGATION","DRAFT_FORE"),"rpm":("MAIN_ENGINE","RPM"),
    "me_rpm":("MAIN_ENGINE","RPM"),"me_power":("MAIN_ENGINE","MAIN_ENGINE_OUTPUT"),
    "shaft_power":("MAIN_ENGINE","POWER_FROM_TORSIOMETER"),
    "me_rh":("MAIN_ENGINE","MAIN_ENGINE_RUNNING_HOURS"),
    "me_running_hours":("MAIN_ENGINE","MAIN_ENGINE_RUNNING_HOURS"),
    "me_load":("MAIN_ENGINE","MAIN_ENGINE_LOADING_PERCENT"),
    "slip":("MAIN_ENGINE","SLIP_PERCENT"),"rob":("FOROB","ROB"),
    "remain_on_board":("FOROB","ROB"),"fuel_rob":("FOROB","ROB"),
    "forob":("FOROB","ROB"),"fo_rob":("FOROB","ROB"),"do_rob":("FOROB","ROB"),
    "wind_speed":("WEATHER","TRUE_WIND_SPEED"),"tws":("WEATHER","TRUE_WIND_SPEED"),
    "wind_dir":("WEATHER","WIND_DIRECTION"),"beaufort":("WEATHER","BEAUFORT"),
    "bf":("WEATHER","BEAUFORT"),"bft":("WEATHER","BEAUFORT"),
    "sea_state":("WEATHER","WAVE_HEIGHT"),"wave_ht":("WEATHER","WAVE_HEIGHT"),
    "swell_ht":("WEATHER","SWELL_HEIGHT"),"air_temp":("WEATHER","AIR_TEMPERATURE"),
    "sea_temp":("WEATHER","WATER_TEMPERATURE"),"imo":("REPORTS","IMO"),
    "imo_no":("REPORTS","IMO"),"imo_number":("REPORTS","IMO"),
    "vessel_name":("REPORTS","VESSEL_NAME"),"ship_name":("REPORTS","VESSEL_NAME"),
    "vessel":("REPORTS","VESSEL_NAME"),"report_date":("REPORTS","DATETIME_GMT"),
    "voyage_no":("REPORTS","VOYAGE_NR"),"voyage_number":("REPORTS","VOYAGE_NR"),
    "voy_no":("REPORTS","VOYAGE_NR"),"departure_port":("REPORTS","ORIGIN_PORT"),
    "destination_port":("REPORTS","DESTINATION_PORT"),"next_port":("REPORTS","DESTINATION_PORT"),
    "eta":("REPORTS","NEXT_PORT_ETA"),"etd":("REPORTS","CONDITION_ETD"),
    "report_type":("REPORTS","REPORT_TYPE"),"condition":("REPORTS","VESSEL_CONDITION"),
    "displacement":("REPORTS","DISPLACEMENT"),"dwt":("REPORTS","DWT"),
    "cargo_weight":("REPORTS","CARGO_WEIGHT"),"remarks":("REPORTS","REMARKS"),
    "current_port":("REPORTS","CURRENT_PORT"),"bunker_amount":("BUNKER","AMOUNT"),
    "bunkered":("BUNKER","AMOUNT"),"sulphur":("BUNKER","SULPHUR"),
    "density":("BUNKER","DENSITY"),"viscosity":("BUNKER","VISCOCITY"),
    "lcv":("BUNKER","LCV"),"rob_after":("BUNKER","ROB_AFTER_BUNKERING"),
    "rob_before":("BUNKER","ROB_BEFORE_BUNKERING"),
    "fuel_consumption":("CONSUMPTION","CONSUMPTION"),
    "me_consumption":("CONSUMPTION","CONSUMPTION"),
    "consumption":("CONSUMPTION","CONSUMPTION"),"sfoc":("CONSUMPTION","ME_SFOC"),
    "co2":("CONSUMPTION","CO2_CONVERSION_FACTOR"),
    "event_start":("EVENTS","START_DATETIME"),"event_end":("EVENTS","END_DATETIME"),
    "event_type":("EVENTS","TYPE"),
}

# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _norm(t: str) -> str:
    return re.sub(r"[\s\-]+", "_", str(t).lower().strip())

def flatten_json(data) -> pd.DataFrame:
    if isinstance(data, dict): data = [data]
    if not isinstance(data, list):
        raise ValueError("JSON must be an object or array of objects.")
    return pd.json_normalize(data, sep="_")

def to_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode()

def to_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    return buf.getvalue()

# ─────────────────────────────────────────────────────────────────────────────
# MAPPING ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _suggest_one(field: str) -> Tuple[str, int, str]:
    n = _norm(field)
    if n in HINTS:
        t, f = HINTS[n]
        return f"{t}.{f}", 98, "Domain"
    for hint, (t, f) in HINTS.items():
        if hint in n or n in hint:
            return f"{t}.{f}", 85, "Domain"
    keys = {_norm(f): f"{t}.{f}" for t, fs in MDA.items() for f in fs}
    res = fuzz_process.extractOne(n, list(keys.keys()), scorer=fuzz.token_sort_ratio)
    if res:
        best, score, _ = res
        if score >= 80: return keys[best], score, "Fuzzy"
        if score >= 55: return keys[best], score, "Fuzzy (weak)"
    return "(Skip / No Mapping)", 0, "No match"

def _ai_enhance(fields: List[str], current: Dict[str, str]) -> Dict[str, str]:
    if not IN_SF or _session is None: return {}
    unmapped = [f for f in fields if current.get(f) == "(Skip / No Mapping)"]
    if not unmapped: return {}
    summary = "\n".join(
        f"  {t}: {', '.join(fs[:8])}{'...' if len(fs)>8 else ''}"
        for t, fs in MDA.items()
    )
    prompt = (
        "You are a maritime data expert. Map these customer fields to MDA schema.\n"
        f"Customer fields: {', '.join(unmapped[:30])}\n\nMDA schema:\n{summary}\n\n"
        'Return ONLY JSON: {"customer_field":"TABLE.FIELD",...} — null for no match.'
    ).replace("'", "''")
    try:
        raw = _session.sql(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b','{prompt}')"
        ).collect()[0][0].strip()
        raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.MULTILINE).strip()
        res = json.loads(raw)
        return {k: v for k, v in res.items() if v and v in ALL_MDA}
    except Exception:
        return {}

def build_mapping(fields: List[str], use_ai: bool) -> pd.DataFrame:
    rows = []
    for f in fields:
        m, c, mth = _suggest_one(f)
        rows.append({"Customer Field": f, "MDA Mapping": m, "Confidence (%)": c, "Method": mth})
    df = pd.DataFrame(rows)
    if use_ai:
        cur = dict(zip(df["Customer Field"], df["MDA Mapping"]))
        for cust, mda in _ai_enhance(fields, cur).items():
            i = df[df["Customer Field"] == cust].index
            if len(i):
                df.at[i[0], "MDA Mapping"] = mda
                df.at[i[0], "Method"] = "AI"
                df.at[i[0], "Confidence (%)"] = 90
    return df

# ─────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def get_schema() -> Dict[str, List[str]]:
    if not IN_SF or _session is None:
        return MDA
    try:
        db, schema = EXPLORER_SCHEMA.split(".")
        rows = _session.sql(
            f"SELECT TABLE_NAME, COLUMN_NAME "
            f"FROM {db}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema.upper()}' "
            f"ORDER BY TABLE_NAME, ORDINAL_POSITION"
        ).collect()
        out: Dict[str, List[str]] = {}
        for r in rows:
            out.setdefault(r[0], []).append(r[1])
        return out if out else MDA
    except Exception:
        return MDA

def run_sql(sql: str) -> Tuple[Optional[pd.DataFrame], str]:
    if not IN_SF or _session is None:
        return None, "Not connected to Snowflake."
    try:
        return _session.sql(sql).to_pandas(), ""
    except Exception as e:
        return None, str(e)

def build_sql(selections: Dict[str, List[str]], filters: List[Dict],
              order_col: str, order_dir: str, limit: int) -> str:
    tables = [t for t, cols in selections.items() if cols]
    if not tables: return ""
    alias: Dict[str, str] = {}

    if len(tables) == 1:
        tbl = tables[0]
        alias[tbl] = ""
        cols_sql = ", ".join(f'"{c}"' for c in selections[tbl])
        sql = f"SELECT {cols_sql}\nFROM {EXPLORER_SCHEMA}.{tbl}"
    else:
        anchor = "REPORTS" if "REPORTS" in tables else tables[0]
        others = [t for t in tables if t != anchor]
        alias[anchor] = "a"
        a_cols = [f'a."{c}" AS "{anchor}__{c}"' for c in selections.get(anchor, [])]
        j_cols, joins = [], []
        for i, t in enumerate(others, 1):
            al = f"t{i}"; alias[t] = al
            j_cols += [f'{al}."{c}" AS "{t}__{c}"' for c in selections[t]]
            joins.append(
                f'LEFT JOIN {EXPLORER_SCHEMA}.{t} {al} '
                f'ON a."REPORT_ID" = {al}."REPORT_ID"'
            )
        sql = (
            f"SELECT {', '.join(a_cols + j_cols)}\n"
            f"FROM {EXPLORER_SCHEMA}.{anchor} a\n" + "\n".join(joins)
        )

    def ref(tc: str) -> str:
        if "." in tc:
            t, c = tc.split(".", 1)
            al = alias.get(t, "")
            return f'{al}."{c}"' if al else f'"{c}"'
        return f'"{tc}"'

    clauses = []
    for f in filters:
        if not (f.get("col") and f.get("op")): continue
        op, val = f["op"], f.get("val", "")
        if op in ("IS NULL", "IS NOT NULL"):
            clauses.append(f"{ref(f['col'])} {op}")
        elif val != "":
            sv = val.replace("'", "''")
            clauses.append(f"{ref(f['col'])} {'LIKE' if op=='LIKE' else op} "
                           f"{'%'+sv+'%' if op=='LIKE' else sv}".replace(
                               sv, f"'{sv}'") if op != "LIKE" else
                           f"{ref(f['col'])} LIKE '%{sv}%'")
    if clauses:
        sql += "\nWHERE " + "\n  AND ".join(clauses)
    if order_col:
        sql += f"\nORDER BY {ref(order_col)} {order_dir}"
    sql += f"\nLIMIT {limit}"
    return sql

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────

for k, v in {
    "page": "home", "src_df": None, "src_name": "",
    "map_df": None, "map_ready": False,
    "ex_sel": {}, "ex_filters": [], "ex_results": None,
    "ex_sql": "", "dv_results": None, "sql_results": None,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

def go(p: str):
    st.session_state.page = p
    st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# THEME
# ─────────────────────────────────────────────────────────────────────────────

def _theme():
    st.markdown("""<style>
    /* ── ZeroNorth dark palette ─────────────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    :root {
        --t:  #14B8A6;   /* teal          */
        --td: #0D9488;   /* teal dark     */
        --tdd:#0F766E;   /* teal darker   */
        --tl: rgba(20,184,166,.12); /* teal tint */
        --bg: #141824;   /* page bg       */
        --sb: #0D1117;   /* sidebar/bar   */
        --cd: #1A1F2E;   /* card bg       */
        --cd2:#1E2436;   /* card bg alt   */
        --br: rgba(255,255,255,0.07); /* border */
        --br2:rgba(255,255,255,0.12); /* border hover */
        --tx: #E2E8F0;   /* primary text  */
        --t2: #94A3B8;   /* muted text    */
        --t3: #64748B;   /* dimmed text   */
        --sh: 0 2px 8px rgba(0,0,0,.4);
        --sm: 0 4px 20px rgba(0,0,0,.5);
        --st: 0 4px 18px rgba(13,148,136,.35);
    }

    /* ── Font ── */
    html,body,.stApp,*{font-family:'Inter','Segoe UI',system-ui,-apple-system,sans-serif!important}

    /* ── App background ── */
    .stApp{background:var(--bg)!important;color:var(--tx)!important}
    .stApp > div{background:var(--bg)!important}
    section.main{background:var(--bg)!important}
    [data-testid="stAppViewContainer"]{background:var(--bg)!important}
    [data-testid="stSidebar"]{background:var(--sb)!important;border-right:1px solid var(--br)!important}

    /* ── Global text ── */
    p,span,li,td,th,div,label{color:var(--tx)!important}
    section.main *{color:var(--tx)}
    [data-testid="stMarkdown"] p,
    [data-testid="stMarkdown"] span,
    [data-testid="stMarkdown"] li{color:var(--tx)!important}
    [data-testid="stWidgetLabel"] p,
    [data-testid="stWidgetLabel"] label{color:var(--t2)!important;font-size:.82rem!important;font-weight:500!important;letter-spacing:.2px}
    .stRadio label,.stCheckbox label{color:var(--tx)!important;font-size:.88rem!important}
    .stSelectbox label,.stTextInput label,
    .stTextArea label,.stNumberInput label{color:var(--t2)!important;font-size:.82rem!important}

    /* ── Headings ── */
    h1{color:var(--tx)!important;font-weight:700!important;letter-spacing:-.4px;font-size:1.6rem!important}
    h2{color:var(--tx)!important;font-weight:600!important;font-size:1.25rem!important}
    h3{color:var(--tx)!important;font-weight:600!important;font-size:1.05rem!important}
    h4,h5,h6{color:var(--t2)!important;font-weight:500!important}

    /* ── Top header bar ── */
    [data-testid="stHeader"]{background:var(--sb)!important;border-bottom:1px solid var(--br)!important}
    [data-testid="stToolbar"]{background:var(--sb)!important}

    /* ── Fade-in ── */
    @keyframes fu{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
    section.main>div:first-child{animation:fu .25s ease-out}

    /* ── Primary button ── */
    .stButton>button[kind="primary"]{
        background:var(--td)!important;border:1px solid var(--td)!important;
        color:#fff!important;border-radius:8px!important;font-weight:600!important;
        font-size:.88rem!important;letter-spacing:.2px!important;
        transition:all .18s ease!important;box-shadow:var(--st)!important}
    .stButton>button[kind="primary"]:hover{
        background:var(--tdd)!important;border-color:var(--tdd)!important;
        transform:translateY(-2px)!important;box-shadow:0 6px 20px rgba(13,148,136,.45)!important}
    .stButton>button[kind="primary"]:active{transform:translateY(0)!important}

    /* ── Default button ── */
    .stButton>button{
        background:var(--cd)!important;border:1px solid var(--br2)!important;
        border-radius:8px!important;color:var(--tx)!important;
        font-size:.88rem!important;transition:all .15s ease!important}
    .stButton>button:hover:not([kind="primary"]){
        background:var(--cd2)!important;border-color:var(--td)!important;
        color:var(--t)!important;transform:translateY(-1px)!important}

    /* ── Download button ── */
    .stDownloadButton>button{
        background:transparent!important;border:1px solid var(--td)!important;
        color:var(--t)!important;border-radius:8px!important;
        font-weight:500!important;font-size:.88rem!important;transition:all .18s ease!important}
    .stDownloadButton>button:hover{
        background:var(--td)!important;color:#fff!important;
        transform:translateY(-2px)!important;box-shadow:var(--st)!important}

    /* ── Module cards ── */
    .zn-card{
        border:1px solid var(--br);border-radius:12px;padding:24px 26px;
        min-height:158px;background:var(--cd);
        transition:transform .2s ease,box-shadow .2s ease,border-color .2s ease}
    .zn-card:hover{
        transform:translateY(-4px);box-shadow:var(--st);
        border-color:rgba(20,184,166,.45)}
    .zn-card h3{margin:0 0 9px 0;color:var(--tx)!important;font-size:1rem;font-weight:600}
    .zn-card p{margin:0;color:var(--t2)!important;font-size:.83rem;line-height:1.65}
    .zn-card code{background:rgba(20,184,166,.12);color:var(--t)!important;
        padding:1px 5px;border-radius:4px;font-size:.78rem}

    /* ── Hero banner ── */
    .zn-hero{
        background:linear-gradient(135deg,#1A2744 0%,#0D1424 60%,#0A1628 100%);
        border:1px solid rgba(20,184,166,.2);
        border-radius:14px;padding:22px 30px;margin-bottom:26px;
        box-shadow:var(--sm);display:flex;align-items:center;gap:20px}
    .zn-hero h1{color:#fff!important;margin:0 0 3px 0!important;
        font-size:1.65rem!important;font-weight:700!important;letter-spacing:-.3px}
    .zn-hero p{color:#94A3B8!important;margin:0!important;font-size:.85rem}
    .zn-badge{
        display:inline-block;background:rgba(13,148,136,.25);
        border:1px solid rgba(20,184,166,.4);color:var(--t)!important;
        font-weight:700;font-size:.68rem;padding:2px 9px;border-radius:20px;
        letter-spacing:.8px;margin-bottom:7px;text-transform:uppercase}

    /* ── Metrics ── */
    [data-testid="metric-container"]{
        background:var(--cd)!important;border:1px solid var(--br)!important;
        border-radius:12px!important;padding:1rem 1.25rem!important;
        transition:transform .18s ease,border-color .18s ease!important}
    [data-testid="metric-container"]:hover{
        transform:translateY(-3px)!important;border-color:rgba(20,184,166,.4)!important}
    [data-testid="stMetricValue"]{color:var(--t)!important;font-weight:700!important;font-size:1.5rem!important}
    [data-testid="stMetricLabel"]{color:var(--t2)!important;font-size:.82rem!important}
    [data-testid="stMetricDelta"]{font-size:.8rem!important}

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"]{
        gap:0;border-bottom:1px solid var(--br)!important;
        background:transparent!important;padding:0 4px}
    .stTabs [data-baseweb="tab"]{
        color:var(--t2)!important;font-weight:500!important;
        border-radius:0!important;padding:.55rem 1.1rem!important;
        font-size:.88rem!important;transition:all .15s ease!important;
        border-bottom:2px solid transparent!important}
    .stTabs [aria-selected="true"]{
        color:var(--t)!important;border-bottom:2px solid var(--t)!important;
        background:transparent!important;font-weight:600!important}
    .stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]){
        color:var(--tx)!important;background:rgba(255,255,255,.03)!important}
    .stTabs [data-baseweb="tab-panel"]{padding-top:1.25rem!important}

    /* ── Expanders ── */
    [data-testid="stExpander"]{
        border:1px solid var(--br)!important;border-radius:10px!important;
        background:var(--cd)!important;margin-bottom:.5rem!important;
        transition:border-color .15s ease!important}
    [data-testid="stExpander"]:hover{border-color:rgba(20,184,166,.3)!important}
    [data-testid="stExpander"] summary p{color:var(--t2)!important;font-weight:500!important;font-size:.85rem!important}
    [data-testid="stExpander"] summary svg{fill:var(--t2)!important}

    /* ── Inputs ── */
    [data-testid="stTextInput"] input,
    [data-testid="stTextArea"] textarea,
    [data-testid="stNumberInput"] input{
        background:var(--cd)!important;border:1px solid var(--br2)!important;
        border-radius:8px!important;color:var(--tx)!important;font-size:.88rem!important}
    [data-testid="stTextInput"] input:focus,
    [data-testid="stTextArea"] textarea:focus{
        border-color:var(--td)!important;
        box-shadow:0 0 0 3px rgba(13,148,136,.2)!important}
    [data-baseweb="select"]>div{
        background:var(--cd)!important;border:1px solid var(--br2)!important;
        border-radius:8px!important;color:var(--tx)!important}
    [data-baseweb="select"]>div:focus-within{
        border-color:var(--td)!important;box-shadow:0 0 0 3px rgba(13,148,136,.2)!important}
    [data-baseweb="menu"]{background:var(--cd2)!important;border:1px solid var(--br2)!important}
    [data-baseweb="menu"] li{color:var(--tx)!important}
    [data-baseweb="menu"] li:hover{background:rgba(20,184,166,.12)!important}
    [data-baseweb="option"][aria-selected="true"]{background:rgba(20,184,166,.18)!important}

    /* ── Slider ── */
    [data-testid="stSlider"] [data-baseweb="slider"] [role="slider"]{
        background:var(--td)!important;border-color:var(--td)!important}

    /* ── Multiselect tags ── */
    [data-baseweb="tag"]{background:rgba(13,148,136,.25)!important;
        border:1px solid rgba(20,184,166,.35)!important;
        border-radius:6px!important;color:var(--t)!important}
    [data-baseweb="tag"] span{color:var(--t)!important}

    /* ── DataFrames ── */
    [data-testid="stDataFrame"],[data-testid="stDataEditor"]{
        border-radius:10px!important;border:1px solid var(--br)!important;overflow:hidden!important}
    [data-testid="stDataFrame"] th{
        background:var(--cd2)!important;color:var(--t2)!important;
        font-size:.78rem!important;font-weight:600!important;letter-spacing:.3px!important;text-transform:uppercase!important}
    [data-testid="stDataFrame"] td{color:var(--tx)!important;font-size:.85rem!important}

    /* ── Alerts ── */
    .stSuccess{background:rgba(20,184,166,.1)!important;
        border-left:3px solid var(--td)!important;border-radius:8px!important;color:var(--t)!important}
    .stInfo{background:rgba(99,179,237,.08)!important;
        border-left:3px solid #63B3ED!important;border-radius:8px!important}
    .stWarning{background:rgba(251,191,36,.08)!important;
        border-left:3px solid #FBBF24!important;border-radius:8px!important}
    .stError{background:rgba(248,113,113,.08)!important;
        border-left:3px solid #F87171!important;border-radius:8px!important}

    /* ── Divider ── */
    hr{border-color:var(--br)!important;margin:1.2rem 0!important}

    /* ── Captions ── */
    .stCaption,small,[data-testid="stCaptionContainer"]{color:var(--t3)!important;font-size:.78rem!important}

    /* ── Code ── */
    code{background:rgba(255,255,255,.06)!important;color:var(--t)!important;
        border-radius:4px;padding:1px 5px;font-size:.84em}
    [data-testid="stCodeBlock"]{background:rgba(0,0,0,.3)!important;
        border:1px solid var(--br)!important;border-radius:10px!important}
    [data-testid="stCodeBlock"] pre{background:transparent!important}

    /* ── Spinner ── */
    [data-testid="stSpinner"]>div{border-top-color:var(--td)!important}

    /* ── Radio ── */
    [data-testid="stRadio"] [role="radiogroup"]{gap:6px}
    [data-baseweb="radio"] div[data-checked="true"]{border-color:var(--td)!important;background:var(--td)!important}

    /* ── Scrollbar ── */
    ::-webkit-scrollbar{width:6px;height:6px}
    ::-webkit-scrollbar-track{background:var(--bg)}
    ::-webkit-scrollbar-thumb{background:rgba(255,255,255,.12);border-radius:3px}
    ::-webkit-scrollbar-thumb:hover{background:rgba(20,184,166,.4)}

    /* ── Field chips (Data Explorer) ── */
    .field-chip{
        display:inline-block;background:rgba(13,148,136,.15);
        color:var(--t)!important;border:1px solid rgba(20,184,166,.3);
        border-radius:6px;padding:2px 9px;font-size:.76rem;margin:2px 3px;font-weight:500}
    .table-label{font-weight:700;color:var(--tx)!important;
        font-size:.82rem;text-transform:uppercase;letter-spacing:.5px}

    /* ── Number input arrows ── */
    [data-testid="stNumberInput"] button{background:var(--cd2)!important;color:var(--tx)!important;border-color:var(--br)!important}
    </style>""", unsafe_allow_html=True)

def _logo(size=52):
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 60 60" width="{size}" height="{size}">
      <defs>
        <linearGradient id="znbg" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stop-color="#1D3557"/>
          <stop offset="100%" stop-color="#0A0F1E"/>
        </linearGradient>
        <linearGradient id="znfg" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%"   stop-color="#6EE7D8"/>
          <stop offset="45%"  stop-color="#14B8A6"/>
          <stop offset="100%" stop-color="#0D7A6E"/>
        </linearGradient>
        <linearGradient id="znfg2" x1="1" y1="0" x2="0" y2="1">
          <stop offset="0%"   stop-color="#2DD4BF"/>
          <stop offset="100%" stop-color="#0F766E"/>
        </linearGradient>
      </defs>
      <!-- rounded square background -->
      <rect width="60" height="60" rx="13" fill="url(#znbg)"/>
      <!-- subtle inner glow border -->
      <rect x=".75" y=".75" width="58.5" height="58.5" rx="12.5" fill="none"
            stroke="url(#znfg)" stroke-width=".8" opacity=".45"/>
      <!-- Z shape — geometric angular -->
      <polygon points="10,13 34,13 10,36 10,47 34,47 34,38 20,38 34,25 34,13"
               fill="url(#znfg)" opacity="0"/>
      <!-- Clean ZN text with tight kerning -->
      <text x="9" y="41" font-family="Arial Black,Impact,sans-serif" font-weight="900"
            font-size="28" fill="url(#znfg)" letter-spacing="-2">Z</text>
      <text x="31" y="41" font-family="Arial Black,Impact,sans-serif" font-weight="900"
            font-size="28" fill="url(#znfg2)" letter-spacing="0">N</text>
    </svg>"""

def _hero(sub="Maritime Data Onboarding Platform"):
    st.markdown(f"""
    <div class="zn-hero">
      <div style="flex-shrink:0">{_logo(54)}</div>
      <div>
        <div class="zn-badge">ZN PLATFORM</div>
        <h1>ZN Onboarding Tool Kit</h1>
        <p>ZeroNorth &nbsp;·&nbsp; {sub}</p>
      </div>
    </div>""", unsafe_allow_html=True)

def _back():
    if st.button("← Home"): go("home")
    st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# PAGE 0 — HOME
# ─────────────────────────────────────────────────────────────────────────────

def page_home():
    # ── Large centred logo + title ────────────────────────────────────────────
    st.markdown(f"""
    <div style="text-align:center;padding:36px 0 20px 0">
      <div style="display:inline-block;margin-bottom:18px">{_logo(80)}</div>
      <h1 style="color:#E2E8F0!important;font-size:2rem!important;
                 font-weight:700!important;margin:0 0 6px 0!important;
                 letter-spacing:-.5px">ZN Onboarding Tool Kit</h1>
      <p style="color:#64748B!important;font-size:.92rem;margin:0">
        ZeroNorth &nbsp;·&nbsp; Maritime Data Onboarding Platform
      </p>
      <div style="display:flex;align-items:center;justify-content:center;
                  gap:8px;margin-top:14px">
        <div style="height:1px;width:60px;background:rgba(20,184,166,.25)"></div>
        <span style="color:#0D9488!important;font-size:.72rem;font-weight:700;
                     letter-spacing:1px;text-transform:uppercase">Select a Module</span>
        <div style="height:1px;width:60px;background:rgba(20,184,166,.25)"></div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2 = st.columns(2, gap="large")
    c3, c4 = st.columns(2, gap="large")

    with c1:
        st.markdown("""<div class="zn-card">
          <h3>🔄 JSON to CSV Converter</h3>
          <p>Upload a customer JSON file. Every nested object and field is
             automatically flattened into tabular rows and columns.
             Download as CSV / Excel or proceed to field mapping.</p>
        </div>""", unsafe_allow_html=True)
        st.write("")
        if st.button("Open JSON Converter", key="b1", type="primary", use_container_width=True):
            go("json")

    with c2:
        st.markdown("""<div class="zn-card">
          <h3>📂 Upload CSV &amp; View Data</h3>
          <p>Upload a customer CSV or Excel file. Preview the data,
             inspect column statistics and null rates, then proceed
             to MDA field mapping.</p>
        </div>""", unsafe_allow_html=True)
        st.write("")
        if st.button("Open CSV Viewer", key="b2", type="primary", use_container_width=True):
            go("csv")

    with c3:
        st.markdown("""<div class="zn-card">
          <h3>🗺️ Field Mapping</h3>
          <p>AI-powered mapping of customer fields to MDA standard
             schema using fuzzy matching, maritime domain hints,
             and Snowflake Cortex AI. Edit and export as CSV.</p>
        </div>""", unsafe_allow_html=True)
        st.write("")
        if st.button("Open Field Mapping", key="b3", type="primary", use_container_width=True):
            go("mapping")

    with c4:
        st.markdown(f"""<div class="zn-card">
          <h3>🔍 Data Explorer</h3>
          <p>Visually select fields from any table in
             <code>{EXPLORER_SCHEMA}</code>.
             Field picker, live query builder, filters,
             sort, distinct value inspector — no SQL needed.</p>
        </div>""", unsafe_allow_html=True)
        st.write("")
        if st.button("Open Data Explorer", key="b4", type="primary", use_container_width=True):
            go("explorer")

    st.divider()
    env = "🟢 Snowflake" if IN_SF else "🟡 Local"
    st.caption(f"{env}   ·   🟢 openpyxl   ·   🟢 rapidfuzz   ·   ZeroNorth © 2024")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 — JSON CONVERTER
# ─────────────────────────────────────────────────────────────────────────────

def page_json():
    _hero("Module 1 — JSON to CSV Converter")
    _back()
    st.markdown("## 🔄 JSON to CSV Converter")
    st.caption("Nested JSON → flat tabular data. Every key becomes a column.")

    f = st.file_uploader("Upload JSON file", type=["json"], key="jf")
    if not f: return

    try:
        df = flatten_json(json.loads(f.read()))
    except Exception as e:
        st.error(f"Error: {e}"); return

    st.success(f"**{f.name}** → {len(df):,} rows × {len(df.columns)} columns")

    m1, m2, m3 = st.columns(3)
    m1.metric("Rows", f"{len(df):,}")
    m2.metric("Columns", len(df.columns))
    m3.metric("Missing cells", f"{df.isnull().sum().sum():,}")

    with st.expander("Preview (first 50 rows)", expanded=True):
        st.dataframe(df.head(50), use_container_width=True)

    with st.expander("All columns & types"):
        st.dataframe(pd.DataFrame({
            "Column": df.columns,
            "Type": [str(df[c].dtype) for c in df.columns],
            "Non-null": df.notnull().sum().values,
        }), use_container_width=True)

    st.divider()
    d1, d2, d3 = st.columns(3)
    with d1:
        st.download_button("⬇ Download CSV", data=to_csv(df),
            file_name=f.name.replace(".json","_flat.csv"), mime="text/csv",
            use_container_width=True)
    with d2:
        st.download_button("⬇ Download Excel", data=to_excel(df),
            file_name=f.name.replace(".json","_flat.xlsx"),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True)
    with d3:
        if st.button("Proceed to Field Mapping →", type="primary", use_container_width=True):
            st.session_state.src_df = df
            st.session_state.src_name = f.name
            st.session_state.map_df = None
            st.session_state.map_ready = False
            go("mapping")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 — CSV VIEWER
# ─────────────────────────────────────────────────────────────────────────────

def page_csv():
    _hero("Module 2 — Upload CSV & View Data")
    _back()
    st.markdown("## 📂 Upload CSV / Excel")

    f = st.file_uploader("Upload CSV or Excel file",
                         type=["csv","xlsx","xls"], key="cf")
    if not f: return

    try:
        df = pd.read_csv(f) if f.name.endswith(".csv") else pd.read_excel(f)
    except Exception as e:
        st.error(f"Could not read file: {e}"); return

    st.success(f"**{f.name}** — {len(df):,} rows × {len(df.columns)} columns")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Rows", f"{len(df):,}")
    m2.metric("Columns", len(df.columns))
    m3.metric("Missing", f"{df.isnull().sum().sum():,}")
    m4.metric("Null %", f"{df.isnull().mean().mean()*100:.1f}%")

    tab1, tab2, tab3 = st.tabs(["📋 Preview", "📊 Column Stats", "🔍 Null Analysis"])
    with tab1:
        n = st.slider("Rows to show", 10, min(500, len(df)), 50, step=10)
        st.dataframe(df.head(n), use_container_width=True)
    with tab2:
        try:
            st.dataframe(df.describe(include="all").T, use_container_width=True)
        except Exception:
            st.dataframe(df.dtypes.rename("dtype").to_frame(), use_container_width=True)
    with tab3:
        st.dataframe(pd.DataFrame({
            "Column": df.columns,
            "Null Count": df.isnull().sum().values,
            "Null %": (df.isnull().mean()*100).round(1).values,
        }), use_container_width=True)

    st.divider()
    d1, d2 = st.columns(2)
    with d1:
        st.download_button("⬇ Download CSV", data=to_csv(df),
            file_name=f.name.rsplit(".",1)[0]+"_clean.csv",
            mime="text/csv", use_container_width=True)
    with d2:
        if st.button("Proceed to Field Mapping →", type="primary", use_container_width=True):
            st.session_state.src_df = df
            st.session_state.src_name = f.name
            st.session_state.map_df = None
            st.session_state.map_ready = False
            go("mapping")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE 3 — FIELD MAPPING
# ─────────────────────────────────────────────────────────────────────────────

def page_mapping():
    _hero("Module 3 — MDA Field Mapping")
    _back()

    df = st.session_state.src_df
    if df is None:
        st.warning("No data loaded. Open **JSON Converter** or **CSV Viewer** first and upload a file.")
        return

    st.markdown(f"## 🗺️ Field Mapping")
    st.caption(f"Source: **{st.session_state.src_name}**  ·  {len(df.columns)} customer fields")

    if not st.session_state.map_ready:
        st.info("Click **Generate Mapping** to run domain hints → fuzzy matching → Cortex AI.")
        use_ai = IN_SF and st.checkbox("Use Snowflake Cortex AI for unmapped fields", value=True)
        if st.button("🚀 Generate Mapping", type="primary"):
            with st.spinner("Analysing fields…"):
                st.session_state.map_df = build_mapping(list(df.columns), use_ai)
                st.session_state.map_ready = True
                st.rerun()
        return

    mdf: pd.DataFrame = st.session_state.map_df
    total = len(mdf)
    mapped = (mdf["MDA Mapping"] != "(Skip / No Mapping)").sum()

    mc1,mc2,mc3,mc4,mc5 = st.columns(5)
    mc1.metric("Total", total)
    mc2.metric("Mapped", mapped)
    mc3.metric("Skipped", total-mapped)
    mc4.metric("High Conf (≥80%)", (mdf["Confidence (%)"]>=80).sum())
    mc5.metric("AI/Domain", mdf["Method"].isin(["AI","Domain"]).sum())

    st.divider()
    fc1, fc2 = st.columns([3,1])
    with fc1:
        show = st.radio("Show", ["All","Mapped only","Unmapped only","Low confidence (<60%)"],
                        horizontal=True)
    with fc2:
        srch = st.text_input("Search", placeholder="e.g. speed")

    view = mdf.copy()
    if show == "Mapped only":   view = view[view["MDA Mapping"]!="(Skip / No Mapping)"]
    elif show == "Unmapped only": view = view[view["MDA Mapping"]=="(Skip / No Mapping)"]
    elif show == "Low confidence (<60%)": view = view[view["Confidence (%)"]<60]
    if srch: view = view[view["Customer Field"].str.contains(srch, case=False, na=False)]

    st.caption(f"Showing **{len(view)}** of **{total}** — edit **MDA Mapping** dropdown to correct")

    edited = st.data_editor(view, column_config={
        "Customer Field": st.column_config.TextColumn(disabled=True, width="medium"),
        "MDA Mapping":    st.column_config.SelectboxColumn(options=ALL_MDA, required=True, width="large"),
        "Confidence (%)": st.column_config.NumberColumn(disabled=True, format="%d %%", width="small"),
        "Method":         st.column_config.TextColumn(disabled=True, width="small"),
    }, hide_index=True, use_container_width=True, key="med", num_rows="fixed")

    for _, row in edited.iterrows():
        i = mdf[mdf["Customer Field"]==row["Customer Field"]].index
        if len(i): mdf.at[i[0],"MDA Mapping"] = row["MDA Mapping"]
    st.session_state.map_df = mdf

    st.divider()
    b1,b2,b3,b4 = st.columns(4)
    with b1:
        exp = mdf.copy()
        exp[["MDA Table","MDA Field"]] = exp["MDA Mapping"].where(
            exp["MDA Mapping"]!="(Skip / No Mapping)", "."
        ).str.split(".",n=1,expand=True).fillna("")
        st.download_button("⬇ Full Mapping CSV",
            data=to_csv(exp[["Customer Field","MDA Table","MDA Field","Confidence (%)","Method"]]),
            file_name="mda_mapping_full.csv", mime="text/csv", use_container_width=True)
    with b2:
        mp = mdf[mdf["MDA Mapping"]!="(Skip / No Mapping)"].copy()
        mp[["MDA Table","MDA Field"]] = mp["MDA Mapping"].str.split(".",n=1,expand=True)
        st.download_button("⬇ Mapped Only CSV",
            data=to_csv(mp[["Customer Field","MDA Table","MDA Field","Confidence (%)","Method"]]),
            file_name="mda_mapped.csv", mime="text/csv", use_container_width=True)
    with b3:
        sk = mdf[mdf["MDA Mapping"]=="(Skip / No Mapping)"][["Customer Field"]]
        st.download_button("⬇ Skipped Fields",
            data=to_csv(sk), file_name="mda_skipped.csv",
            mime="text/csv", use_container_width=True)
    with b4:
        if st.button("↺ Re-generate", use_container_width=True):
            st.session_state.map_ready = False
            st.session_state.map_df = None
            st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# PAGE 4 — DATA EXPLORER
# ─────────────────────────────────────────────────────────────────────────────

def page_explorer():
    _hero(f"Module 4 — Data Explorer · {EXPLORER_SCHEMA}")
    _back()

    if not IN_SF:
        st.warning("Data Explorer requires a live Snowflake connection. Run this app inside Snowsight.")
        st.info("Schema browser below shows the hardcoded MDA schema for reference.")

    schema = get_schema()
    tables = sorted(schema.keys())

    tab_build, tab_sql = st.tabs(["🖱️ Visual Field Picker", "✏️ SQL Editor"])

    # ══ TAB: SQL EDITOR ══════════════════════════════════════════════════════
    with tab_sql:
        st.markdown("### SQL Editor")
        st.caption("Write any SQL against the schema. The Visual Picker auto-fills this when you run a query.")

        sql_val = st.session_state.ex_sql or f"SELECT *\nFROM {EXPLORER_SCHEMA}.REPORTS\nLIMIT 100"
        sql_in = st.text_area("SQL", value=sql_val, height=200,
                              key="sql_in", label_visibility="collapsed")

        rc1, rc2, _ = st.columns([1,1,5])
        run = rc1.button("▶ Execute", type="primary", use_container_width=True, key="run_sql")
        if rc2.button("Clear", use_container_width=True, key="clr_sql"):
            st.session_state.ex_sql = ""
            st.rerun()

        if run and sql_in.strip():
            st.session_state.ex_sql = sql_in
            with st.spinner("Running…"):
                res, err = run_sql(sql_in.strip())
            if err: st.error(err)
            else:   st.session_state.sql_results = res

        sr = st.session_state.sql_results
        if sr is not None:
            st.success(f"{len(sr):,} rows × {len(sr.columns)} columns")
            st.dataframe(sr, use_container_width=True, height=380)
            sc1, sc2 = st.columns(2)
            sc1.download_button("⬇ CSV", data=to_csv(sr),
                file_name="sql_results.csv", mime="text/csv",
                use_container_width=True, key="dl_sql_csv")
            sc2.download_button("⬇ Excel", data=to_excel(sr),
                file_name="sql_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="dl_sql_xl")

    # ══ TAB: VISUAL PICKER ═══════════════════════════════════════════════════
    with tab_build:
        left, right = st.columns([2, 3], gap="large")

        # ── LEFT: Schema browser ─────────────────────────────────────────────
        with left:
            st.markdown("### 📁 Schema Browser")
            st.caption("Select fields from any table below. Selected fields appear on the right.")

            search = st.text_input("Filter fields", placeholder="e.g. FUEL, SPEED, IMO",
                                   key="ex_search")

            for tbl in tables:
                cols = schema[tbl]
                if search:
                    cols = [c for c in cols if search.upper() in c.upper()]
                    if not cols: continue

                cur = st.session_state.ex_sel.get(tbl, [])
                sel_badge = (
                    f'<span style="background:rgba(13,148,136,.3);color:#2DD4BF!important;'
                    f'border-radius:4px;padding:1px 7px;font-size:.7rem;margin-left:7px;'
                    f'font-weight:700">{len(cur)} selected</span>'
                    if cur else ""
                )
                st.markdown(
                    f'<div style="margin:12px 0 4px 0;padding:5px 10px;'
                    f'background:rgba(20,184,166,.08);border-left:3px solid #14B8A6;'
                    f'border-radius:0 6px 6px 0">'
                    f'<span style="color:#14B8A6!important;font-weight:700;'
                    f'font-size:.76rem;letter-spacing:.07em;text-transform:uppercase">'
                    f'{tbl}</span>{sel_badge}</div>',
                    unsafe_allow_html=True
                )
                chosen = st.multiselect(
                    "Fields", options=cols, default=cur,
                    key=f"ms_{tbl}", label_visibility="collapsed",
                    placeholder="Click to add fields…"
                )
                st.session_state.ex_sel[tbl] = chosen

        # ── RIGHT: Query builder ─────────────────────────────────────────────
        with right:
            sel: Dict[str,List[str]] = {
                t: c for t, c in st.session_state.ex_sel.items() if c
            }
            n_fields = sum(len(v) for v in sel.values())

            st.markdown("### ✅ Selected Fields")
            if not sel:
                st.info("No fields selected yet. Expand a table on the left and tick the fields you need.")
            else:
                for tbl, cols in sel.items():
                    chips = "".join(
                        f'<span class="field-chip">{c}</span>' for c in cols
                    )
                    st.markdown(
                        f'<div style="margin-bottom:6px">'
                        f'<span class="table-label">{tbl}</span>&nbsp; {chips}</div>',
                        unsafe_allow_html=True
                    )
                st.write("")
                if st.button("🗑 Clear All Selections", key="clr_sel"):
                    st.session_state.ex_sel = {}
                    st.session_state.ex_results = None
                    st.rerun()

            st.divider()

            # ── Filters ───────────────────────────────────────────────────────
            st.markdown("### 🔎 Filters *(optional)*")
            flat_cols = [f"{t}.{c}" for t, cs in sel.items() for c in cs]
            ops = ["=","!=",">","<",">=","<=","LIKE","IS NULL","IS NOT NULL"]

            new_filters = []
            for i, flt in enumerate(st.session_state.ex_filters):
                fc1,fc2,fc3,fc4 = st.columns([3,2,3,1])
                flt["col"] = fc1.selectbox("Col", flat_cols,
                    index=flat_cols.index(flt["col"]) if flt["col"] in flat_cols else 0,
                    key=f"fc_{i}", label_visibility="collapsed")
                flt["op"] = fc2.selectbox("Op", ops,
                    index=ops.index(flt["op"]) if flt["op"] in ops else 0,
                    key=f"fo_{i}", label_visibility="collapsed")
                need_val = flt["op"] not in ("IS NULL","IS NOT NULL")
                flt["val"] = fc3.text_input("Val", value=flt.get("val",""),
                    key=f"fv_{i}", label_visibility="collapsed",
                    disabled=not need_val)
                if not fc4.button("✕", key=f"fr_{i}"):
                    new_filters.append(flt)
            st.session_state.ex_filters = new_filters

            if flat_cols and st.button("＋ Add Filter"):
                st.session_state.ex_filters.append({"col":flat_cols[0],"op":"=","val":""})
                st.rerun()

            st.divider()

            # ── Query options ─────────────────────────────────────────────────
            st.markdown("### ⚙️ Query Options")
            qc1,qc2,qc3 = st.columns([3,2,1])
            ord_col = qc1.selectbox("ORDER BY", ["(none)"]+flat_cols, key="ord_col")
            ord_dir = qc2.radio("Direction", ["ASC","DESC"], horizontal=True, key="ord_dir")
            lim     = qc3.number_input("LIMIT", 10, 5000, 100, 50, key="lim")

            ord_resolved = "" if ord_col == "(none)" else ord_col
            sql = build_sql(sel, st.session_state.ex_filters,
                            ord_resolved, ord_dir, int(lim))

            if sql:
                with st.expander("Generated SQL", expanded=False):
                    st.code(sql, language="sql")

            # ── Run ───────────────────────────────────────────────────────────
            if st.button("▶ Run Query", type="primary",
                         disabled=(n_fields == 0), use_container_width=True):
                with st.spinner("Querying Snowflake…"):
                    res, err = run_sql(sql)
                if err:
                    st.error(f"Query error: {err}")
                    st.session_state.ex_results = None
                else:
                    st.session_state.ex_results = res
                    st.session_state.ex_sql = sql  # push to SQL editor

        # ── Results ───────────────────────────────────────────────────────────
        res_df: Optional[pd.DataFrame] = st.session_state.ex_results
        if res_df is not None:
            st.divider()
            st.markdown(f"### Results — {len(res_df):,} rows × {len(res_df.columns)} columns")
            st.dataframe(res_df, use_container_width=True, height=400)
            rc1, rc2 = st.columns(2)
            rc1.download_button("⬇ Download CSV", data=to_csv(res_df),
                file_name="explorer_results.csv", mime="text/csv", use_container_width=True)
            rc2.download_button("⬇ Download Excel", data=to_excel(res_df),
                file_name="explorer_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)

        # ── Distinct Values Inspector ─────────────────────────────────────────
        st.divider()
        st.markdown("### 🔢 Distinct Values Inspector")
        st.caption("Pick any table and field to see all distinct values with counts.")

        dv1, dv2, dv3 = st.columns([2,2,1])
        dv_tbl = dv1.selectbox("Table", tables, key="dv_tbl")
        dv_cols = schema.get(dv_tbl, [])
        dv_col = dv2.selectbox("Field", dv_cols, key="dv_col") if dv_cols else None

        if dv3.button("Get Distinct", use_container_width=True) and dv_col:
            dv_sql = (
                f'SELECT "{dv_col}", COUNT(*) AS COUNT '
                f'FROM {EXPLORER_SCHEMA}.{dv_tbl} '
                f'GROUP BY "{dv_col}" ORDER BY COUNT DESC LIMIT 200'
            )
            with st.spinner("Fetching…"):
                dv_res, dv_err = run_sql(dv_sql)
            if dv_err: st.error(dv_err)
            else: st.session_state.dv_results = (dv_tbl, dv_col, dv_res)

        dv = st.session_state.dv_results
        if dv:
            dv_t, dv_c, dv_df = dv
            st.caption(f"`{dv_t}.{dv_c}` — {len(dv_df)} distinct values (top 200)")
            st.dataframe(dv_df, use_container_width=True, height=260)

# ─────────────────────────────────────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────────────────────────────────────

_PAGES = {
    "home":    page_home,
    "json":    page_json,
    "csv":     page_csv,
    "mapping": page_mapping,
    "explorer": page_explorer,
}

_theme()
_PAGES.get(st.session_state.page, page_home)()
