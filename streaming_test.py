import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


# =============================
# CONFIG
# =============================
EVENT_ID = 14195502  # <- change if you want

EVENT_URL = f"https://api.sofascore.com/api/v1/event/{EVENT_ID}/"
STATS_URL = f"https://www.sofascore.com/api/v1/event/{EVENT_ID}/statistics"

OUT_EVENT = Path(f"data/event_{EVENT_ID}.json")
OUT_STATS = Path(f"data/stats_{EVENT_ID}.json")

MAX_WAIT_SECONDS = 10
SLEEP_SECONDS = 0.2


# =============================
# SELENIUM HELPERS
# =============================
def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver


def read_json_from_page(driver) -> Optional[Dict[str, Any]]:
    """
    SofaScore API pages return raw JSON rendered in <pre> or <body>.
    """
    start = time.time()
    last_txt = ""

    while time.time() - start < MAX_WAIT_SECONDS:
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


# =============================
# PARSERS (FINAL, CONFIRMED)
# =============================
def parse_event_min(ev_json: Dict[str, Any]) -> Dict[str, Any]:
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


def _to_float(x):
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        return float(x.replace("%", ""))
    return None


def parse_stats_min(stats_json: Dict[str, Any], event_id: int) -> Dict[str, Any]:
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

    return {
        "event_id": event_id,
        "home_xg": home_xg,
        "away_xg": away_xg,
    }


# =============================
# MAIN TEST
# =============================
def main():
    driver = build_driver()

    try:
        # warm up
        driver.get("https://www.sofascore.com")
        time.sleep(1)

        # ---------------------
        # EVENT
        # ---------------------
        driver.get(EVENT_URL)
        ev_json = read_json_from_page(driver)
        if ev_json is None:
            raise RuntimeError("Failed to load event JSON")

        event_min = parse_event_min(ev_json)

        # ---------------------
        # STATS
        # ---------------------
        driver.get(STATS_URL)
        st_json = read_json_from_page(driver)
        if st_json is None:
            raise RuntimeError("Failed to load stats JSON")

        stats_min = parse_stats_min(st_json, EVENT_ID)

        # ---------------------
        # WRITE OUTPUT
        # ---------------------
        OUT_EVENT.write_text(json.dumps(event_min, indent=2), encoding="utf-8")
        OUT_STATS.write_text(json.dumps(stats_min, indent=2), encoding="utf-8")

        print("SUCCESS")
        print("Event JSON written to:", OUT_EVENT)
        print("Stats JSON written to:", OUT_STATS)
        print("\nEvent payload:")
        print(event_min)
        print("\nStats payload:")
        print(stats_min)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
