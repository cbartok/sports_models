"""
Data loaders & daily cache for the MLB Win-Probability Model.
Sources:
  1. Official MLB Stats API (schedules, boxscores, rosters, L/R splits, handedness, projected lineups)
  2. Baseball Savant CSV leaderboards (xwOBA, xERA, Baserunning, OAA, Catcher Framing)
  3. Optional local FanGraphs CSV exports (Steamer / ATC / ZiPS) if placed in ./projections/
"""

import io
import time
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://statsapi.mlb.com/api/v1"
CACHE_DIR = Path(__file__).parent / "cache"
PROJ_DIR = Path(__file__).parent / "projections"

SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(max_retries=Retry(total=4, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])),
)


def _fetch_csv_cached(url: str, cache_name: str) -> pd.DataFrame:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / cache_name
    if path.exists() and (time.time() - path.stat().st_mtime) < 12 * 3600:
        return pd.read_csv(path)
    resp = SESSION.get(url, timeout=20)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    df.to_csv(path, index=False)
    return df


def load_savant_data(season: int) -> dict[str, dict]:
    urls = {
        "bat_x": f"https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=batter&year={season}&position=&team=&min=1&csv=true",
        "pit_x": f"https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=pitcher&year={season}&position=&team=&min=1&csv=true",
        "bsr": f"https://baseballsavant.mlb.com/leaderboard/baserunning?year={season}&min=1&csv=true",
        "oaa": f"https://baseballsavant.mlb.com/leaderboard/outs_above_average?type=Fielder&startYear={season}&endYear={season}&split=yes&team=&range=year&min=1&pos=&roles=&viz=hide&csv=true",
        "framing": f"https://baseballsavant.mlb.com/leaderboard/catcher-framing?year={season}&csv=true",
    }

    def _load_or_prev(key, name):
        try:
            df = _fetch_csv_cached(urls[key], f"savant_{name}_{season}.csv")
            if len(df) >= 50:
                return df
        except Exception:
            pass
        prev_url = urls[key].replace(f"year={season}", f"year={season - 1}").replace(
            f"startYear={season}&endYear={season}", f"startYear={season - 1}&endYear={season - 1}"
        )
        return _fetch_csv_cached(prev_url, f"savant_{name}_{season - 1}.csv")

    bat_df = _load_or_prev("bat_x", "bat_x")
    pit_df = _load_or_prev("pit_x", "pit_x")
    bsr_df = _load_or_prev("bsr", "bsr")
    oaa_df = _load_or_prev("oaa", "oaa")
    frm_df = _load_or_prev("framing", "framing")

    bat_xwoba = {}
    for _, r in bat_df.iterrows():
        if pd.notna(r.get("player_id")) and pd.notna(r.get("est_woba")):
            bat_xwoba[int(r["player_id"])] = {
                "pa": float(r.get("pa", 0)),
                "xwoba": float(r["est_woba"]),
                "woba": float(r["woba"]) if pd.notna(r.get("woba")) else float(r["est_woba"]),
            }

    pit_xera = {}
    for _, r in pit_df.iterrows():
        if pd.notna(r.get("player_id")) and pd.notna(r.get("xera")):
            pit_xera[int(r["player_id"])] = {
                "pa": float(r.get("pa", 0)),
                "xera": float(r["xera"]),
                "xwoba_allowed": float(r["est_woba"]) if pd.notna(r.get("est_woba")) else 0.312,
            }

    baserunning = {}
    for _, r in bsr_df.iterrows():
        if pd.notna(r.get("entity_id")) and pd.notna(r.get("runner_runs")):
            baserunning[int(r["entity_id"])] = float(r["runner_runs"])

    fielding_oaa = {}
    for _, r in oaa_df.iterrows():
        if pd.notna(r.get("player_id")) and pd.notna(r.get("fielding_runs_prevented")):
            pid = int(r["player_id"])
            fielding_oaa[pid] = fielding_oaa.get(pid, 0.0) + float(r["fielding_runs_prevented"])

    catcher_framing = {}
    for _, r in frm_df.iterrows():
        if pd.notna(r.get("id")) and pd.notna(r.get("rv_tot")):
            catcher_framing[int(r["id"])] = {
                "pitches": float(r.get("pitches", 0)),
                "rv_tot": float(r["rv_tot"]),
            }

    return {
        "bat_xwoba": bat_xwoba,
        "pit_xera": pit_xera,
        "baserunning": baserunning,
        "fielding_oaa": fielding_oaa,
        "catcher_framing": catcher_framing,
    }


def load_fangraphs_projections() -> dict[str, dict]:
    bat_proj, pit_proj = {}, {}
    bat_path = PROJ_DIR / "fangraphs_bat.csv"
    pit_path = PROJ_DIR / "fangraphs_pit.csv"

    if bat_path.exists():
        df = pd.read_csv(bat_path)
        id_col = "MLBAMID" if "MLBAMID" in df.columns else "xMLBAMID"
        if id_col in df.columns:
            for _, r in df.iterrows():
                if pd.notna(r.get(id_col)):
                    bat_proj[int(r[id_col])] = {
                        "wrc_plus": float(r["wRC+"]) if pd.notna(r.get("wRC+")) else 100.0,
                        "bsr": float(r["BsR"]) if pd.notna(r.get("BsR")) else 0.0,
                        "pa": float(r["PA"]) if pd.notna(r.get("PA")) else 400.0,
                    }

    if pit_path.exists():
        df = pd.read_csv(pit_path)
        id_col = "MLBAMID" if "MLBAMID" in df.columns else "xMLBAMID"
        if id_col in df.columns:
            for _, r in df.iterrows():
                if pd.notna(r.get(id_col)):
                    siera = r.get("SIERA") if pd.notna(r.get("SIERA")) else r.get("FIP")
                    pit_proj[int(r[id_col])] = {
                        "siera": float(siera) if pd.notna(siera) else 4.10,
                        "ip": float(r["IP"]) if pd.notna(r.get("IP")) else 0.0,
                        "gs": float(r["GS"]) if pd.notna(r.get("GS")) else 0.0,
                    }

    return {"bat": bat_proj, "pit": pit_proj}


def fetch_people_platoon_and_meta(player_ids: list[int], season: int) -> dict[int, dict]:
    unique_ids = sorted(set(player_ids))
    result: dict[int, dict] = {}
    chunk_size = 80

    for i in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[i : i + chunk_size]
        ids_str = ",".join(str(x) for x in chunk)
        url = (
            f"{BASE_URL}/people?personIds={ids_str}"
            f"&hydrate=stats(group=[hitting],type=[statSplits],sitCodes=[vl,vr],season={season})"
        )
        data = SESSION.get(url, timeout=20).json()
        for p in data.get("people", []):
            pid = p["id"]
            splits_map = {}
            for st in p.get("stats", []):
                for sp in st.get("splits", []):
                    code = sp.get("split", {}).get("code")
                    if code in ("vl", "vr"):
                        splits_map[code] = sp.get("stat", {})
            result[pid] = {
                "fullName": p.get("fullName", "Unknown"),
                "batSide": p.get("batSide", {}).get("code", "R"),
                "pitchHand": p.get("pitchHand", {}).get("code", "R"),
                "splits": splits_map,
            }
    return result


def get_all_recent_pitch_loads(target_date: str) -> dict[int, dict]:
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    start = (dt - timedelta(days=2)).strftime("%Y-%m-%d")
    end = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"{BASE_URL}/schedule?sportId=1&startDate={start}&endDate={end}"
    sched = SESSION.get(url, timeout=15).json()

    pitches_by_pid: dict[int, int] = {}
    dates_by_pid: dict[int, set[str]] = {}
    seen_pks = set()
    for d in sched.get("dates", []):
        game_date = d.get("date", "")
        for g in d.get("games", []):
            if g.get("status", {}).get("abstractGameState") != "Final":
                continue
            pk = g["gamePk"]
            if pk in seen_pks:
                continue
            seen_pks.add(pk)
            box = SESSION.get(f"{BASE_URL}/game/{pk}/boxscore", timeout=15).json()
            for side in ("home", "away"):
                t_box = box["teams"][side]
                for pid in t_box.get("pitchers", []):
                    p_info = t_box["players"].get(f"ID{pid}", {})
                    pitches = p_info.get("stats", {}).get("pitching", {}).get("numberOfPitches", 0)
                    if pitches > 0:
                        pitches_by_pid[pid] = pitches_by_pid.get(pid, 0) + pitches
                        dates_by_pid.setdefault(pid, set()).add(game_date)

    return {
        pid: {"pitches": pitches_by_pid[pid], "days_pitched": len(dates_by_pid.get(pid, set()))}
        for pid in pitches_by_pid
    }


def get_projected_lineup_for_team(
    team_id: int,
    target_date: str,
    opp_hand: str = "R",
    lookback_days: int = 10,
) -> tuple[list[int], dict]:
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    start = (dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"{BASE_URL}/schedule?sportId=1&teamId={team_id}&startDate={start}&endDate={end}&hydrate=probablePitcher"
    sched = SESSION.get(url, timeout=15).json()

    recent_games = []
    for d in sched.get("dates", []):
        for g in d.get("games", []):
            if g.get("status", {}).get("abstractGameState") == "Final":
                recent_games.append(g)

    recent_games.reverse()
    fallback_lineup: tuple[list[int], dict] | None = None

    for g in recent_games:
        pk = g["gamePk"]
        box = SESSION.get(f"{BASE_URL}/game/{pk}/boxscore", timeout=15).json()
        side = "home" if box["teams"]["home"]["team"]["id"] == team_id else "away"
        opp_side = "away" if side == "home" else "home"

        t_box = box["teams"][side]
        order = t_box.get("battingOrder", [])[:9]
        if len(order) < 9:
            continue

        players = t_box.get("players", {})
        if fallback_lineup is None:
            fallback_lineup = (order, players)

        opp_pitchers = box["teams"][opp_side].get("pitchers", [])
        if opp_pitchers:
            opp_sp_id = opp_pitchers[0]
            p_resp = SESSION.get(f"{BASE_URL}/people/{opp_sp_id}", timeout=10).json()
            opp_sp_hand = p_resp.get("people", [{}])[0].get("pitchHand", {}).get("code", "R")
            if opp_sp_hand == opp_hand:
                return order, players

    return fallback_lineup if fallback_lineup is not None else ([], {})


def get_starter_game_log_ip(pid: int, season: int, before_date: str | None = None) -> tuple[float, int]:
    """Fetch exact innings pitched and count of starts (gamesStarted == 1) from MLB gameLog."""
    try:
        url = f"{BASE_URL}/people/{pid}/stats?stats=gameLog&group=pitching&season={season}"
        data = SESSION.get(url, timeout=12).json()
        splits = data.get("stats", [{}])[0].get("splits", [])
    except Exception:
        return 0.0, 0

    total_ip = 0.0
    starts = 0
    for sp in splits:
        if before_date and sp.get("date", "") >= before_date:
            continue
        st = sp.get("stat", {})
        if st.get("gamesStarted") == 1:
            ip_str = str(st.get("inningsPitched", "0"))
            if "." in ip_str:
                w, f = ip_str.split(".")
                total_ip += int(w) + int(f) / 3.0
            else:
                total_ip += float(ip_str)
            starts += 1
    return total_ip, starts
