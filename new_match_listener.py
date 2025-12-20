import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


# -----------------------------
# Config
# -----------------------------
CSV_PATH = Path("data/spl_match_links_merged.csv")

STREAM_EVENT_DIR = Path("data/stream_in/event")
STREAM_STATS_DIR = Path("data/stream_in/stats")

EVENT_URL = "https://www.sofascore.com/api/v1/event/{event_id}"
STATS_URL = "https://www.sofascore.com/api/v1/event/{event_id}/statistics"

FINISHED_STATUS_CODE = 100

MAX_WAIT_SECONDS = 12
SLEEP_SECONDS = 0.15

FORCE = os.environ.get("FORCE", "0") == "1"

EVENT_ID_RE = re.compile(r"/event/(\d+)")


# -----------------------------
# IO helpers
# -----------------------------
def ensure_dirs() -> None:
    STREAM_EVENT_DIR.mkdir(parents=True, exist_ok=True)
    STREAM_STATS_DIR.mkdir(parents=True, exist_ok=True)


def write_json_atomic(path: Path, payload: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    tmp.replace(path)


def write_csv_atomic(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def out_paths(event_id: int) -> Tuple[Path, Path]:
    ts = int(time.time() * 1000)
    event_out = STREAM_EVENT_DIR / f"event_{event_id}_{ts}.json"
    stats_out = STREAM_STATS_DIR / f"stats_{event_id}_{ts}.json"
    return event_out, stats_out


def already_backfilled(event_id: int) -> bool:
    has_event = any(STREAM_EVENT_DIR.glob(f"event_{event_id}_*.json"))
    has_stats = any(STREAM_STATS_DIR.glob(f"stats_{event_id}_*.json"))
    return has_event and has_stats


# -----------------------------
# CSV helpers
# -----------------------------
def normalize_flag_series(s: pd.Series) -> pd.Series:
    """
    Turn various representations into 0/1 ints:
    - 1/0
    - True/False
    - "1"/"0"
    - "true"/"false"
    """
    if s.dtype == bool:
        return s.astype(int)

    # strings / mixed -> normalize
    ss = s.astype(str).str.strip().str.lower()
    return ss.map(lambda x: 1 if x in ("1", "true", "t", "yes", "y") else 0)


def extract_event_id_from_row(row: pd.Series) -> Optional[int]:
    # Prefer explicit event_id column if present
    for col in row.index:
        if str(col).lower() in ("event_id", "eventid", "sofascore_event_id"):
            try:
                v = int(float(row[col]))
                return v
            except Exception:
                pass

    # Else regex scan all cells
    for col in row.index:
        v = row[col]
        if pd.isna(v):
            continue
        m = EVENT_ID_RE.search(str(v))
        if m:
            return int(m.group(1))

    return None


# -----------------------------
# Selenium helpers
# -----------------------------
def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver


def read_json_from_page(driver) -> Optional[Dict[str, Any]]:
    """
    API endpoint returns JSON, usually displayed in <pre> or body text.
    Poll until JSON parses or timeout.
    """
    t0 = time.time()
    last_txt = ""

    while time.time() - t0 < MAX_WAIT_SECONDS:
        try:
            pre = driver.find_elements(By.TAG_NAME, "pre")
            if pre:
                txt = pre[0].text.strip()
            else:
                txt = driver.find_element(By.TAG_NAME, "body").text.strip()

            if txt:
                last_txt = txt

            try:
                return json.loads(last_txt)
            except Exception:
                time.sleep(0.3)
        except Exception:
            time.sleep(0.3)

    try:
        return json.loads(last_txt)
    except Exception:
        return None


def get_status_code(event_json: Dict[str, Any]) -> Optional[int]:
    try:
        return event_json.get("event", {}).get("status", {}).get("code")
    except Exception:
        return None


# -----------------------------
# Parsers (min schema for Spark)
# -----------------------------
def parse_event_min(ev_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Convert SofaScore event payload -> the schema expected by Load.ipynb:
      event_id, season_year, home_team, away_team, home_score, away_score, status_code, start_timestamp
    """
    try:
        ev = ev_json["event"]
        return {
            "event_id": ev["id"],
            "season_year": ev["season"]["year"],
            "home_team": ev["homeTeam"]["name"],
            "away_team": ev["awayTeam"]["name"],
            "home_score": ev["homeScore"]["current"],
            "away_score": ev["awayScore"]["current"],
            "status_code": ev["status"]["code"],
            "start_timestamp": ev["startTimestamp"],
        }
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.replace("%", "").strip())
        except Exception:
            return None
    return None


def parse_stats_min(stats_json: Dict[str, Any], event_id: int) -> Optional[Dict[str, Any]]:
    """
    Convert SofaScore stats payload -> the schema expected by Load.ipynb:
      event_id, home_xg, away_xg
    """
    try:
        home_xg = None
        away_xg = None

        groups = stats_json["statistics"][0]["groups"]
        for g in groups:
            for it in g.get("statisticsItems", []):
                if it.get("key") == "expectedGoals":
                    home_xg = _to_float(it.get("homeValue"))
                    away_xg = _to_float(it.get("awayValue"))
                    break
            if home_xg is not None:
                break

        return {"event_id": int(event_id), "home_xg": home_xg, "away_xg": away_xg}
    except Exception:
        return None


# -----------------------------
# Main logic
# -----------------------------
def main() -> None:
    ensure_dirs()

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    if "is_future_fixture" not in df.columns:
        raise ValueError("spl_match_links_merged.csv must contain a column named 'is_future_fixture'.")

    # normalize to 0/1
    df["is_future_fixture"] = normalize_flag_series(df["is_future_fixture"])

    # build a list of row indices for future fixtures only
    future_idx = df.index[df["is_future_fixture"] == 1].tolist()
    if not future_idx:
        print("[INFO] No future fixtures flagged (is_future_fixture == 1). Nothing to do.")
        return

    # extract event_id per future row
    idx_and_eids = []
    missing = 0
    for idx in future_idx:
        eid = extract_event_id_from_row(df.loc[idx])
        if eid is None:
            missing += 1
            continue
        idx_and_eids.append((idx, eid))

    if not idx_and_eids:
        raise ValueError("Could not extract any event_id from rows where is_future_fixture == 1.")

    print(f"[INFO] future fixtures flagged: {len(future_idx)}")
    print(f"[INFO] extracted event_ids: {len(idx_and_eids)} (missing event_id in {missing} rows)")

    driver = build_driver()

    written = 0
    flipped = 0
    skipped_backfilled = 0
    finished_seen = 0

    try:
        # warm up session
        driver.get("https://www.sofascore.com")
        time.sleep(1.0)

        for k, (row_idx, eid) in enumerate(idx_and_eids, start=1):
            if (not FORCE) and already_backfilled(eid):
                skipped_backfilled += 1
                continue

            # 1) event json first
            driver.get(EVENT_URL.format(event_id=eid))
            ev_json = read_json_from_page(driver)
            time.sleep(SLEEP_SECONDS)

            if ev_json is None:
                continue

            if get_status_code(ev_json) != FINISHED_STATUS_CODE:
                # still future / not ended -> keep flag as is_future_fixture=1
                continue

            finished_seen += 1

            # 2) stats json (only if finished)
            driver.get(STATS_URL.format(event_id=eid))
            st_json = read_json_from_page(driver)
            time.sleep(SLEEP_SECONDS)

            if st_json is None:
                continue

            # 3) parse + write minified schema to streaming folders
            ev_min = parse_event_min(ev_json)
            st_min = parse_stats_min(st_json, eid)
            if ev_min is None or st_min is None:
                continue

            event_out, stats_out = out_paths(eid)
            write_json_atomic(event_out, ev_min)
            write_json_atomic(stats_out, st_min)
            written += 1

            # 4) flip flag in original csv row
            if int(df.at[row_idx, "is_future_fixture"]) == 1:
                df.at[row_idx, "is_future_fixture"] = 0
                flipped += 1

            if written % 20 == 0:
                print(f"[INFO] written={written}, flipped={flipped}, scanned_future={k}/{len(idx_and_eids)}")

    finally:
        driver.quit()

    # Persist CSV updates if we flipped anything
    if flipped > 0:
        write_csv_atomic(CSV_PATH, df)
        print(f"[INFO] updated CSV (flipped {flipped} rows to is_future_fixture=0): {CSV_PATH}")

    print("[DONE]")
    print(f"  finished seen (status=100):  {finished_seen}")
    print(f"  written to stream folders:   {written}")
    print(f"  skipped (already backfilled): {skipped_backfilled}")
    print(f"  event dir: {STREAM_EVENT_DIR}")
    print(f"  stats dir: {STREAM_STATS_DIR}")
    if FORCE:
        print("  FORCE=1 was set (re-dropping even if already backfilled).")


if __name__ == "__main__":
    main()
