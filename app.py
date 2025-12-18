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
/* Keep header visible so the sidebar toggle exists */
header[data-testid="stHeader"] { position: sticky; top: 0; z-index: 999; }

/* Reduce top whitespace */
div[data-testid="stAppViewContainer"] > .main { padding-top: 0.25rem; }
/* Prevent bottom-right Streamlit overlay from covering last rows */
div[data-testid="stAppViewContainer"] .main {
  padding-bottom: 120px;
}

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


# -------------------------
# Sidebar navigation
# -------------------------
page = st.sidebar.radio("View", ["Power Ranking", "Fixtures"], index=0)

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

    # Sort by current league position (table order)
    if "pts" in df.columns:
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

    if "team" not in fixtures.columns:
        st.error("Fixtures file must contain a `team` column (team-opponent rows).")
        st.stop()

    teams = sorted(fixtures["team"].dropna().astype(str).unique().tolist())
    selected_team = st.selectbox("Select team", teams, index=0)

    team_df = fixtures[fixtures["team"].astype(str) == str(selected_team)].copy()

    # Sort
    if "kickoff" in team_df.columns:
        team_df["kickoff_sort"] = pd.to_datetime(team_df["kickoff"], errors="coerce")
        team_df = team_df.sort_values(["kickoff_sort"], ascending=True)
    elif "exp_pts" in team_df.columns:
        team_df = team_df.sort_values("exp_pts", ascending=False)

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
        kickoff = r.get("kickoff", "")

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

        # Home/away ordering
        if isinstance(venue, str) and venue.upper() == "A":
            title = f"{opp} vs {selected_team}"
        else:
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
  <div class="smallmuted">{kickoff} • {venue_txt}</div>
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
