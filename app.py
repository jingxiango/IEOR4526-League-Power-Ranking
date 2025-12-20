# app.py
import glob
import json
from pathlib import Path
import pandas as pd
import streamlit as st

# -------------------------
# Page config + lightweight styling
# -------------------------
st.set_page_config(page_title="SPL Dashboard", layout="wide")

st.markdown(
    """
<style>
/* Keep header visible so the sidebar toggle exists */
header[data-testid="stHeader"] { position: sticky; top: 0; z-index: 999; }

/* Reduce top whitespace */
div[data-testid="stAppViewContainer"] > .main { padding-top: 0.25rem; }

.block-container { padding-top: 0.2rem; padding-bottom: 2rem; max-width: 1200px; }
h1, h2, h3 { letter-spacing: -0.02em; }

.section-title { font-size: 22px; font-weight: 800; margin: 6px 0 10px 0; }
.smallmuted { color: rgba(0,0,0,0.55); font-size: 12px; }
.rowcard { padding: 10px 10px; border-bottom: 1px solid rgba(0,0,0,0.07); }
.matchname { font-weight: 800; font-size: 14px; }

/* Sticky sidebar */
section[data-testid="stSidebar"] > div {
  position: sticky;
  top: 0;
  height: 100vh;
  overflow: auto;
}

.probbar {
  width: 180px; height: 14px;
  border-radius: 8px;
  background: #f0f1f3;
  overflow: hidden;
  border: 1px solid rgba(0,0,0,0.06);
}
.probbar > div { height: 100%; float: left; }
.win  { background: #f4b000; }
.draw { background: #ffe8b3; }
.loss { background: #ffffff; }

.table-wrap table { width: 100%; border-collapse: collapse; }
.table-wrap th { text-align: left; font-size: 12px; color: rgba(0,0,0,0.65); border-bottom: 1px solid rgba(0,0,0,0.12); padding: 8px 6px; }
.table-wrap td { padding: 8px 6px; border-bottom: 1px solid rgba(0,0,0,0.07); font-size: 13px; }
.right { text-align: right; }
</style>
""",
    unsafe_allow_html=True,
)


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"

DASH_DIR = DATA / "dashboard_table_csv"
FIXTURE_DIR = DATA / "match_level_fixtures"


# -------------------------
# Helpers
# -------------------------
def _latest_spark_part(folder: Path) -> Path | None:
    """
    Spark writes folder/part-0000... (sometimes without .csv extension),
    plus _SUCCESS and *.crc files. This returns the largest non-crc part file.
    """
    if not folder.exists():
        return None

    files = sorted(glob.glob(str(folder / "part-*")))
    files = [f for f in files if not f.endswith(".crc")]
    if not files:
        return None

    files_p = [Path(f) for f in files]
    return max(files_p, key=lambda p: p.stat().st_size)


@st.cache_data(show_spinner=False)
def load_dashboard() -> pd.DataFrame | None:
    fp = _latest_spark_part(DASH_DIR)
    if fp is None:
        return None
    return pd.read_csv(fp)


@st.cache_data(show_spinner=False)
def load_fixtures() -> pd.DataFrame | None:
    fp = _latest_spark_part(FIXTURE_DIR)
    if fp is None:
        return None
    return pd.read_csv(fp)


def to_pct(x):
    """Accepts 0-1 or 0-100. Returns 0-100 float."""
    if pd.isna(x):
        return None
    x = float(x)
    return x * 100 if x <= 1.0 else x


def prob_bar(pw, pdw, pl):
    pw = to_pct(pw) or 0.0
    pdw = to_pct(pdw) or 0.0
    pl = to_pct(pl) or 0.0
    s = max(pw + pdw + pl, 1e-9)
    pw, pdw, pl = pw / s * 100, pdw / s * 100, pl / s * 100

    return (
        f'<div class="probbar">'
        f'<div class="win" style="width:{pw:.2f}%"></div>'
        f'<div class="draw" style="width:{pdw:.2f}%"></div>'
        f'<div class="loss" style="width:{pl:.2f}%"></div>'
        f'</div>'
    )


def safe_num(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _parse_uploaded_json_files(files) -> list[dict]:
    out: list[dict] = []
    if not files:
        return out
    for f in files:
        try:
            payload = json.loads(f.getvalue().decode("utf-8"))
            if isinstance(payload, dict):
                out.append(payload)
            elif isinstance(payload, list):
                out.extend([x for x in payload if isinstance(x, dict)])
        except Exception:
            # ignore bad uploads
            continue
    return out


def _apply_eventmin_updates(df: pd.DataFrame, events: list[dict]) -> pd.DataFrame:
    """
    Apply EventMin-style updates to an in-memory table.
    Expected fields per event:
      event_id, home_team, away_team, home_score, away_score, status_code
    """
    if df is None or df.empty or not events:
        return df

    # require core cols
    needed = {"team", "pts", "gf", "ga"}
    if not needed.issubset(set(df.columns)):
        return df

    out = df.copy()

    # normalize team key
    out["team"] = out["team"].astype(str).str.strip()

    def pts(hs, as_):
        if hs > as_:
            return 3, 0
        if hs < as_:
            return 0, 3
        return 1, 1

    # de-dupe by event_id if present
    seen = set()
    for ev in events:
        eid = ev.get("event_id")
        if eid is not None:
            if eid in seen:
                continue
            seen.add(eid)

        try:
            if int(ev.get("status_code", 100)) != 100:
                continue
        except Exception:
            pass

        ht = str(ev.get("home_team", "")).strip()
        at = str(ev.get("away_team", "")).strip()
        try:
            hs = int(ev.get("home_score"))
            aas = int(ev.get("away_score"))
        except Exception:
            continue

        hp, ap = pts(hs, aas)

        # apply home
        m_h = out["team"] == ht
        if m_h.any():
            out.loc[m_h, "pts"] = pd.to_numeric(out.loc[m_h, "pts"], errors="coerce").fillna(0) + hp
            out.loc[m_h, "gf"] = pd.to_numeric(out.loc[m_h, "gf"], errors="coerce").fillna(0) + hs
            out.loc[m_h, "ga"] = pd.to_numeric(out.loc[m_h, "ga"], errors="coerce").fillna(0) + aas

        # apply away
        m_a = out["team"] == at
        if m_a.any():
            out.loc[m_a, "pts"] = pd.to_numeric(out.loc[m_a, "pts"], errors="coerce").fillna(0) + ap
            out.loc[m_a, "gf"] = pd.to_numeric(out.loc[m_a, "gf"], errors="coerce").fillna(0) + aas
            out.loc[m_a, "ga"] = pd.to_numeric(out.loc[m_a, "ga"], errors="coerce").fillna(0) + hs

    return out


def _apply_statsmin_to_fixtures(fixtures: pd.DataFrame, stats: list[dict]) -> pd.DataFrame:
    """
    Apply StatsMin-style xG overrides to the exported fixtures table (team/opponent rows).
    Expected fields per stats record:
      event_id, home_xg, away_xg

    If fixtures has columns: event_id, venue (H/A), xg_for, xg_against, we override:
      - venue == H: xg_for=home_xg, xg_against=away_xg
      - venue == A: xg_for=away_xg, xg_against=home_xg
    """
    if fixtures is None or fixtures.empty or not stats:
        return fixtures

    needed = {"event_id", "venue", "xg_for", "xg_against"}
    if not needed.issubset(set(fixtures.columns)):
        return fixtures

    # build lookup
    lut: dict[int, tuple[float | None, float | None]] = {}
    for s in stats:
        try:
            eid = int(s.get("event_id"))
        except Exception:
            continue
        hxg = s.get("home_xg")
        axg = s.get("away_xg")
        try:
            hxg = None if hxg is None else float(hxg)
            axg = None if axg is None else float(axg)
        except Exception:
            continue
        lut[eid] = (hxg, axg)

    if not lut:
        return fixtures

    out = fixtures.copy()
    out["event_id"] = pd.to_numeric(out["event_id"], errors="coerce")
    out["venue"] = out["venue"].astype(str).str.strip().str.upper()

    for eid, (hxg, axg) in lut.items():
        if hxg is None or axg is None:
            continue
        m = out["event_id"] == eid
        if not m.any():
            continue
        m_h = m & (out["venue"] == "H")
        m_a = m & (out["venue"] == "A")
        out.loc[m_h, "xg_for"] = hxg
        out.loc[m_h, "xg_against"] = axg
        out.loc[m_a, "xg_for"] = axg
        out.loc[m_a, "xg_against"] = hxg

    return out


# -------------------------
# Sidebar navigation
# -------------------------
page = st.sidebar.radio("View", ["Power Ranking", "Fixtures"], index=0)

with st.sidebar.expander("Demo input (Community Cloud) — upload JSON", expanded=False):
    st.caption(
        "Streamlit Community Cloud cannot see your local `data/stream_in/...` folders. "
        "Upload EventMin JSON here to see table impact immediately."
    )
    uploaded_events = st.file_uploader(
        "Upload EventMin JSON file(s)",
        type=["json"],
        accept_multiple_files=True,
        key="upload_events",
    )
    uploaded_stats = st.file_uploader(
        "Upload StatsMin JSON file(s)",
        type=["json"],
        accept_multiple_files=True,
        key="upload_stats",
    )
    if st.button("Reset uploaded demo inputs", use_container_width=True):
        st.session_state.pop("demo_events", None)
        st.session_state.pop("upload_events", None)
        st.session_state.pop("demo_stats", None)
        st.session_state.pop("upload_stats", None)
        st.rerun()

    if uploaded_events:
        st.session_state["demo_events"] = _parse_uploaded_json_files(uploaded_events)
        st.write(f"Loaded {len(st.session_state['demo_events'])} event records.")
    if uploaded_stats:
        st.session_state["demo_stats"] = _parse_uploaded_json_files(uploaded_stats)
        st.write(f"Loaded {len(st.session_state['demo_stats'])} stats records.")

# -------------------------
# Power Ranking page
# -------------------------
if page == "Power Ranking":
    st.markdown('<div class="section-title">SPL Power Ranking</div>', unsafe_allow_html=True)

    df = load_dashboard()
    if df is None:
        st.warning("No dashboard output found. Run the Spark export first.")
        st.stop()

    # numeric casting for expected columns
    for c in ["pts", "spi", "exp_pts_mc", "win_league_pct", "make_acl_pct"]:
        df = safe_num(df, c)

    # Apply uploaded demo events (Community Cloud friendly)
    demo_events = st.session_state.get("demo_events") or []
    if demo_events:
        st.info("Demo mode: applying uploaded EventMin JSON to update pts/gf/ga (model metrics are not recomputed).")
        df = _apply_eventmin_updates(df, demo_events)

    # Sort by current league position (points, GD, GF)
    if {"pts", "gf", "ga"}.issubset(set(df.columns)):
        df["gd"] = pd.to_numeric(df["gf"], errors="coerce").fillna(0) - pd.to_numeric(df["ga"], errors="coerce").fillna(0)
        df = df.sort_values(["pts", "gd", "gf"], ascending=[False, False, False])
    elif "pts" in df.columns:
        df = df.sort_values("pts", ascending=False)

    # Add league position (1, 2, 3, ...)
    df["league_pos"] = range(1, len(df) + 1)

    # Rename to pretty
    df_show = df.rename(
        columns={
            "league_pos": "Pos",
            "team": "Club",
            "pts": "Current Pts",
            "spi": "Power Index",
            "exp_pts_mc": "Expected Final Pts",
            "win_league_pct": "Win League (%)",
            "make_acl_pct": "Make ACL Top 2 (%)",
        }
    ).copy()

    # Convert pct columns to 0-100 if needed
    for c in ["Win League (%)", "Make ACL Top 2 (%)"]:
        if c in df_show.columns:
            df_show[c] = df_show[c].apply(lambda x: to_pct(x) if pd.notna(x) else None)

    # Keep a clean column order
    preferred = [
        "Pos",
        "Club",
        "Current Pts",
        "Power Index",
        "Expected Final Pts",
        "Make ACL Top 2 (%)",
        "Win League (%)",
    ]
    cols = [c for c in preferred if c in df_show.columns] + [c for c in df_show.columns if c not in preferred]
    df_show = df_show[cols]

    # Rounding: Power Index and Expected Final Pts to integer; others nice display
    if "Power Index" in df_show.columns:
        df_show["Power Index"] = pd.to_numeric(df_show["Power Index"], errors="coerce").round(0)
    if "Expected Final Pts" in df_show.columns:
        df_show["Expected Final Pts"] = pd.to_numeric(df_show["Expected Final Pts"], errors="coerce").round(0)
    for c in ["Pos", "Current Pts", "Win League (%)", "Make ACL Top 2 (%)"]:
        if c in df_show.columns:
            df_show[c] = pd.to_numeric(df_show[c], errors="coerce").round(0)

    # Display as compact HTML table
    def fmt(v):
        if pd.isna(v):
            return ""
        return str(v)

    header = "".join([f"<th>{c}</th>" for c in df_show.columns])

    rows_html = []
    for _, r in df_show.iterrows():
        tds = []
        for c in df_show.columns:
            v = r[c]
            cls = "right" if c != "Club" else ""

            if c.endswith("(%)") and pd.notna(v):
                v = f"{float(v):.0f}%"
            elif c in ["Pos", "Current Pts", "Power Index", "Expected Final Pts"] and pd.notna(v):
                v = f"{float(v):.0f}"

            tds.append(f'<td class="{cls}">{fmt(v)}</td>')
        rows_html.append("<tr>" + "".join(tds) + "</tr>")

    st.markdown(
        f"""
<div class="table-wrap">
  <table>
    <thead><tr>{header}</tr></thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</div>
""",
        unsafe_allow_html=True,
    )

# -------------------------
# Fixtures page
# -------------------------
else:
    st.markdown('<div class="section-title">Remaining Fixtures — Match Level W/D/L</div>', unsafe_allow_html=True)

    fixtures = load_fixtures()
    if fixtures is None:
        st.info("No fixture output found. Run the Spark export first.")
        st.stop()

    # numeric columns
    for c in ["p_win", "p_draw", "p_loss", "exp_pts", "xg_for", "xg_against"]:
        fixtures = safe_num(fixtures, c)

    # Apply uploaded stats (Community Cloud demo): override xg_for/xg_against when event_id matches.
    demo_stats = st.session_state.get("demo_stats") or []
    if demo_stats:
        st.info("Demo mode: applying uploaded StatsMin JSON to override xG values where event_id matches.")
        fixtures = _apply_statsmin_to_fixtures(fixtures, demo_stats)

    if "team" not in fixtures.columns:
        st.error("Fixtures file must contain a `team` column (team-opponent rows).")
        st.stop()

    teams = sorted(fixtures["team"].dropna().astype(str).unique().tolist())
    selected_team = st.selectbox("Select team", teams, index=0)

    team_df = fixtures[fixtures["team"].astype(str) == str(selected_team)].copy()

    # Parse + sort by match_date (assume it exists)
    team_df["match_date"] = pd.to_datetime(team_df["match_date"], errors="coerce")
    team_df = team_df.sort_values("match_date", ascending=True)

    st.markdown(
        """
<div class="smallmuted" style="margin-top:6px; margin-bottom:4px;">
Each row is a future match for the selected club, with Win/Draw/Loss probabilities and expected points.
</div>
""",
        unsafe_allow_html=True,
    )

    # Required columns
    missing = [c for c in ["opponent", "venue", "p_win", "p_draw", "p_loss", "exp_pts"] if c not in team_df.columns]
    if missing:
        st.error(f"Fixtures output is missing columns: {missing}")
        st.stop()

    # Render fixture cards
    for _, r in team_df.iterrows():
        opp = r.get("opponent", "")
        venue = r.get("venue", "")
        match_date = r["match_date"].strftime("%Y-%m-%d") if pd.notna(r["match_date"]) else ""
        pw, pdw, pl = r.get("p_win", None), r.get("p_draw", None), r.get("p_loss", None)
        exp_pts = r.get("exp_pts", None)

        pwp = to_pct(pw) or 0.0
        pdwp = to_pct(pdw) or 0.0
        plp = to_pct(pl) or 0.0

        # xG line (no indentation -> renders as HTML)
        xg_for = r.get("xg_for", None)
        xg_against = r.get("xg_against", None)
        xg_line = ""
        if "xg_for" in team_df.columns and "xg_against" in team_df.columns:
            if pd.notna(xg_for) and pd.notna(xg_against):
                xg_line = (
                    f'<div class="smallmuted">'
                    f'xG {float(xg_for):.2f} – {float(xg_against):.2f}'
                    f'</div>'
                )

        title = f"{selected_team} vs {opp}"
        # Pretty venue
        venue_txt = venue
        if isinstance(venue, str):
            if venue.upper() == "H":
                venue_txt = "Home"
            elif venue.upper() == "A":
                venue_txt = "Away"

        exp_txt = "" if pd.isna(exp_pts) else f"{float(exp_pts):.2f}"

        st.markdown(
            f"""
<div class="rowcard">
  <div class="smallmuted">{match_date} • {venue_txt}</div>
  <div class="matchname">{title}</div>
  {xg_line}

  <div style="display:flex; gap:14px; align-items:center; margin-top:8px;">
    {prob_bar(pw, pdw, pl)}
    <div class="smallmuted" style="min-width:220px;">
      Win {pwp:.0f}% • Draw {pdwp:.0f}% • Loss {plp:.0f}%
    </div>
    <div style="flex:1;"></div>
    <div>
      <div class="smallmuted">Expected Pts</div>
      <div style="font-weight:800;">{exp_txt}</div>
    </div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )
st.markdown("<div style='height:220px;'></div>", unsafe_allow_html=True)

