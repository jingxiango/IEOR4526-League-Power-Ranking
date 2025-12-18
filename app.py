# app.py
import glob
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
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1200px; }
h1, h2, h3 { letter-spacing: -0.02em; }

.section-title { font-size: 22px; font-weight: 800; margin: 6px 0 10px 0; }
.smallmuted { color: rgba(0,0,0,0.55); font-size: 12px; }
.rowcard { padding: 10px 10px; border-bottom: 1px solid rgba(0,0,0,0.07); }
.matchname { font-weight: 800; font-size: 14px; }

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
    plus _SUCCESS and *.crc files. This returns the newest non-crc part file.
    """
    if not folder.exists():
        return None

    files = sorted(glob.glob(str(folder / "part-*")))
    files = [f for f in files if not f.endswith(".crc")]
    if not files:
        return None

    # pick largest (usually the real data) as a robust default
    files_p = [Path(f) for f in files]
    return max(files_p, key=lambda p: p.stat().st_size)


@st.cache_data(show_spinner=False)
def load_dashboard() -> pd.DataFrame | None:
    fp = _latest_spark_part(DASH_DIR)
    if fp is None:
        return None
    try:
        return pd.read_csv(fp)
    except Exception:
        # sometimes Spark writes without header; fallback try
        return pd.read_csv(fp, header=0)


@st.cache_data(show_spinner=False)
def load_fixtures() -> pd.DataFrame | None:
    fp = _latest_spark_part(FIXTURE_DIR)
    if fp is None:
        return None
    try:
        return pd.read_csv(fp)
    except Exception:
        return pd.read_csv(fp, header=0)


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


def safe_num(df, col):
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# -------------------------
# Sidebar navigation
# -------------------------
page = st.sidebar.radio("View", ["Power Ranking", "Fixtures"], index=0)

st.sidebar.markdown("---")
st.sidebar.caption("Data folders expected:")
st.sidebar.code(f"{DASH_DIR}\n{FIXTURE_DIR}", language="text")


# -------------------------
# Power Ranking page
# -------------------------
if page == "Power Ranking":
    st.markdown('<div class="section-title">SPL Power Ranking</div>', unsafe_allow_html=True)

    df = load_dashboard()
    if df is None:
        st.warning(
            "No dashboard output found.\n\n"
            "Expected Spark output under `data/dashboard_table_csv/` as `part-*` files."
        )
        st.stop()

    # expected columns (flexible): team, pts, spi, exp_pts_mc, win_league_pct, make_acl_pct
    for c in ["pts", "Strength Index", "exp_pts_mc", "win_league_pct", "make_acl_pct"]:
        df = safe_num(df, c)

    # Sort priority: win league prob then SPI then current pts
    sort_cols = [c for c in ["win_league_pct", "spi", "pts"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[False] * len(sort_cols))

    # rename to pretty
    df_show = df.rename(
        columns={
            "team": "Club",
            "pts": "Current Pts",
            "spi": "SPI",
            "exp_pts_mc": "Expected Final Pts (MC)",
            "win_league_pct": "Win League (%)",
            "make_acl_pct": "Make ACL Top 2 (%)",
        }
    ).copy()

    # convert pct columns to 0-100 if needed
    for c in ["Win League (%)", "Make ACL Top 2 (%)"]:
        if c in df_show.columns:
            df_show[c] = df_show[c].apply(lambda x: to_pct(x) if pd.notna(x) else None)

    # keep a clean column order if present
    preferred = [
        "Club",
        "Current Pts",
        "SPI",
        "Expected Final Pts (MC)",
        "Make ACL Top 2 (%)",
        "Win League (%)",
    ]
    cols = [c for c in preferred if c in df_show.columns] + [
        c for c in df_show.columns if c not in preferred
    ]
    df_show = df_show[cols]

    # rounding
    for c in ["SPI", "Expected Final Pts (MC)", "Win League (%)", "Make ACL Top 2 (%)", "Current Pts"]:
        if c in df_show.columns:
            df_show[c] = pd.to_numeric(df_show[c], errors="coerce").round(2)

    # display as a compact HTML table (more 538-ish than st.dataframe)
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
            # show % nicely
            if c.endswith("(%)") and pd.notna(v):
                v = f"{float(v):.0f}%"
            elif c in ["Current Pts", "Expected Final Pts (MC)"] and pd.notna(v):
                v = f"{float(v):.0f}"
            elif c == "SPI" and pd.notna(v):
                v = f"{float(v):.1f}"
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
        st.info(
            "No fixture output found.\n\n"
            "Expected Spark output under `data/match_level_fixtures/` as `part-*` files."
        )
        st.stop()

    # expected minimal columns: team, opponent, venue, p_win, p_draw, p_loss, exp_pts
    # try to normalize common alternatives
    rename_map = {}
    if "home_team" in fixtures.columns and "away_team" in fixtures.columns and "team" not in fixtures.columns:
        # if you stored fixtures as home/away per match, you can keep this,
        # but the UI below expects team/opponent rows
        pass

    fixtures = fixtures.rename(columns=rename_map).copy()

    # numeric columns
    for c in ["p_win", "p_draw", "p_loss", "exp_pts", "xg_for", "xg_against"]:
        fixtures = safe_num(fixtures, c)

    # build team dropdown
    if "team" not in fixtures.columns:
        st.error("Fixtures file must contain a `team` column (team-opponent rows).")
        st.stop()

    teams = sorted(fixtures["team"].dropna().astype(str).unique().tolist())
    if not teams:
        st.error("No teams found in fixtures output.")
        st.stop()

    selected_team = st.selectbox("Select team", teams, index=0)

    team_df = fixtures[fixtures["team"].astype(str) == str(selected_team)].copy()

    # If kickoff exists, sort by time; else sort by exp_pts desc
    if "kickoff" in team_df.columns:
        # don't assume perfect datetime
        team_df["kickoff_sort"] = pd.to_datetime(team_df["kickoff"], errors="coerce")
        team_df = team_df.sort_values(["kickoff_sort"], ascending=True)
    elif "exp_pts" in team_df.columns:
        team_df = team_df.sort_values("exp_pts", ascending=False)

    # header row labels
    st.markdown(
        """
<div class="smallmuted" style="margin-top:6px; margin-bottom:4px;">
Each row is a future match for the selected club, with Win/Draw/Loss probabilities and expected points.
</div>
""",
        unsafe_allow_html=True,
    )

    # Render fixture rows like 538 “cards”
    # Required columns: opponent, venue, p_win, p_draw, p_loss, exp_pts
    missing = [c for c in ["opponent", "venue", "p_win", "p_draw", "p_loss", "exp_pts"] if c not in team_df.columns]
    if missing:
        st.error(f"Fixtures output is missing columns: {missing}")
        st.stop()

    for _, r in team_df.iterrows():
        opp = r.get("opponent", "")
        venue = r.get("venue", "")
        kickoff = r.get("kickoff", "")

        pw, pdw, pl = r.get("p_win", None), r.get("p_draw", None), r.get("p_loss", None)
        exp_pts = r.get("exp_pts", None)

        pwp = to_pct(pw)
        pdwp = to_pct(pdw)
        plp = to_pct(pl)

        # xG (optional)
        xg_for = r.get("xg_for", None)
        xg_against = r.get("xg_against", None)
        xg_txt = ""
        if "xg_for" in team_df.columns and "xg_against" in team_df.columns:
            if pd.notna(xg_for) and pd.notna(xg_against):
                xg_txt = f" • xG {float(xg_for):.2f}–{float(xg_against):.2f}"

        # pretty venue
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
  <div class="smallmuted">{kickoff} • {venue_txt}</div>
  <div class="matchname">{selected_team} vs {opp}{xg_txt}</div>

  <div style="display:flex; gap:14px; align-items:center; margin-top:6px;">
    {prob_bar(pw, pdw, pl)}
    <div class="smallmuted" style="min-width:220px;">
      Win {0 if pwp is None else pwp:.0f}% •
      Draw {0 if pdwp is None else pdwp:.0f}% •
      Loss {0 if plp is None else plp:.0f}%
    </div>
    <div style="flex:1;"></div>
    <div>
      <div class="smallmuted">Expected Pts</div>
      <div style="font-weight:800; font-size:14px;">{exp_txt}</div>
    </div>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

    # Optional: show a raw table below for debugging / transparency
    with st.expander("Show raw fixtures table for this team"):
        show_cols = [c for c in ["kickoff", "opponent", "venue", "p_win", "p_draw", "p_loss", "exp_pts"] if c in team_df.columns]
        st.dataframe(team_df[show_cols], use_container_width=True)
