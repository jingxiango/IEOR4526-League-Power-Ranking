import glob
import pandas as pd
import streamlit as st

st.set_page_config(page_title="SPL Dashboard", layout="wide")

DATA_DIR = "streamlit_out/dashboard_table_csv"  # same folder Spark writes to

def load_latest_csv(folder):
    files = sorted(glob.glob(f"{folder}/part-*.csv"))
    if not files:
        return None
    return pd.read_csv(files[-1])

st.title("SPL Forecast Dashboard")

df = load_latest_csv(DATA_DIR)
if df is None:
    st.warning("No dashboard output found yet. Run the Spark export first.")
    st.stop()

# clean formatting
for c in ["pts", "spi", "exp_pts_mc", "win_league_pct", "make_acl_pct"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

df = df.sort_values("win_league_pct", ascending=False)

# pretty columns
df_show = df.rename(columns={
    "team": "Club",
    "pts": "Current Pts",
    "spi": "SPI",
    "exp_pts_mc": "Expected Final Pts (MC)",
    "win_league_pct": "Win League (%)",
    "make_acl_pct": "Make ACL Top 2 (%)",
})

# round for display
for col in ["SPI", "Expected Final Pts (MC)", "Win League (%)", "Make ACL Top 2 (%)", "Current Pts"]:
    if col in df_show.columns:
        df_show[col] = df_show[col].round(2)

st.dataframe(df_show, use_container_width=True)

# optional quick visuals
c1, c2 = st.columns(2)
with c1:
    st.subheader("Win League (%)")
    st.bar_chart(df_show.set_index("Club")["Win League (%)"])
with c2:
    st.subheader("Make ACL Top 2 (%)")
    st.bar_chart(df_show.set_index("Club")["Make ACL Top 2 (%)"])

import glob
import pandas as pd
import streamlit as st

FIXTURE_DIR = "streamlit_out/match_level_fixtures"

@st.cache_data(ttl=2)
def load_fixtures():
    files = sorted(glob.glob(f"{FIXTURE_DIR}/part-*.csv"))
    if not files:
        return None
    return pd.read_csv(files[-1])

st.subheader("Remaining Fixtures — Match Level W/D/L")

fixtures = load_fixtures()
if fixtures is None:
    st.info("Match-level probabilities not available yet.")
    st.stop()

teams = sorted(fixtures["team"].unique())
selected_team = st.selectbox("Select team", teams)

team_df = (
    fixtures[fixtures["team"] == selected_team]
      .sort_values("exp_pts", ascending=False)
)

# Pretty column names
team_df = team_df.rename(columns={
    "team": "Club",
    "opponent": "Opponent",
    "venue": "Venue",
    "xg_for": "xG For",
    "xg_against": "xG Against",
    "p_win": "Win %",
    "p_draw": "Draw %",
    "p_loss": "Loss %",
    "exp_pts": "Expected Points"
})

st.dataframe(
    team_df[
        ["Opponent", "Venue", "xG For", "xG Against", "Win %", "Draw %", "Loss %", "Expected Points"]
    ],
    use_container_width=True
)

st.subheader("Win / Draw / Loss Breakdown")

wdl_chart = team_df.set_index("Opponent")[["Win %", "Draw %", "Loss %"]]
st.bar_chart(wdl_chart)
