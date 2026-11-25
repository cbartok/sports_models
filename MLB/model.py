"""
Upgraded MLB Win-Probability & Fair-Odds Model (v2)
"""

import argparse
import math
from data_sources import (
    BASE_URL,
    SESSION,
    fetch_people_platoon_and_meta,
    get_all_recent_pitch_loads,
    load_fangraphs_projections,
    load_savant_data,
)

LG_RUNS_PER_GAME = 4.45
LG_RUNS_PER_PA = 0.118
LG_WOBA = 0.312
WOBA_SCALE = 1.18
LG_ERA_FIP = 4.10
FIP_CONSTANT = 3.15
HFA_LOG_ODDS = 0.1254  # Empirical 2019-2026 HFA (~53.13% neutral win%)

PA_BY_SPOT = [4.65, 4.55, 4.45, 4.35, 4.25, 4.15, 4.05, 3.95, 3.85]

PLATOON_PRIOR_WOBA = {
    ("L", "R"): +0.012,
    ("L", "L"): -0.024,
    ("R", "L"): +0.012,
    ("R", "R"): -0.008,
    ("S", "L"): +0.004,
    ("S", "R"): +0.004,
}

PARK_FACTORS = {
    "Coors Field": 1.13,
    "Great American Ball Park": 1.07,
    "Fenway Park": 1.05,
    "Globe Life Field": 1.02,
    "Wrigley Field": 1.02,
    "Yankee Stadium": 1.01,
    "Citizens Bank Park": 1.02,
    "Rogers Centre": 1.01,
    "Truist Park": 1.01,
    "Chase Field": 1.01,
    "Minute Maid Park": 1.00,
    "Daikin Park": 1.00,
    "Target Field": 1.00,
    "Nationals Park": 1.00,
    "Oriole Park at Camden Yards": 0.99,
    "Busch Stadium": 0.98,
    "Progressive Field": 0.98,
    "Kauffman Stadium": 0.99,
    "Dodger Stadium": 0.99,
    "Rate Field": 1.00,
    "Guaranteed Rate Field": 1.00,
    "loanDepot park": 0.97,
    "PNC Park": 0.97,
    "Comerica Park": 0.96,
    "Citi Field": 0.96,
    "Angel Stadium": 0.99,
    "Oracle Park": 0.94,
    "Petco Park": 0.95,
    "T-Mobile Park": 0.92,
}


def parse_ip(ip_str) -> float:
    if not ip_str:
        return 0.0
    s = str(ip_str)
    if "." not in s:
        return float(s)
    whole, frac = s.split(".")
    return int(whole) + int(frac) / 3.0


def calc_raw_woba(bat_stats: dict) -> tuple[float, int]:
    if not bat_stats:
        return LG_WOBA, 0
    ab = bat_stats.get("atBats", 0)
    bb = bat_stats.get("baseOnBalls", 0)
    ibb = bat_stats.get("intentionalWalks", 0)
    hbp = bat_stats.get("hitByPitch", 0)
    sf = bat_stats.get("sacFlies", 0)
    h = bat_stats.get("hits", 0)
    doubles = bat_stats.get("doubles", 0)
    triples = bat_stats.get("triples", 0)
    hr = bat_stats.get("homeRuns", 0)
    singles = max(0, h - doubles - triples - hr)

    denom = ab + bb - ibb + sf + hbp
    if denom <= 0:
        return LG_WOBA, 0
    ubb = max(0, bb - ibb)
    num = 0.69 * ubb + 0.72 * hbp + 0.88 * singles + 1.24 * doubles + 1.56 * triples + 2.00 * hr
    return num / denom, denom


def calc_regressed_fip(pitch_stats: dict, prior_ip: float = 40.0) -> tuple[float, float]:
    if not pitch_stats:
        return LG_ERA_FIP, 0.0
    ip = parse_ip(pitch_stats.get("inningsPitched", "0"))
    if ip <= 0:
        return LG_ERA_FIP, 0.0
    hr = pitch_stats.get("homeRuns", 0)
    bb = pitch_stats.get("baseOnBalls", 0)
    hbp = pitch_stats.get("hitByPitch", 0)
    so = pitch_stats.get("strikeOuts", 0)

    raw_fip = ((13 * hr + 3 * (bb + hbp) - 2 * so) / ip) + FIP_CONSTANT
    reg_fip = (ip * raw_fip + prior_ip * LG_ERA_FIP) / (ip + prior_ip)
    return reg_fip, ip


def estimate_batter_raa_per_pa(
    pid: int,
    bat_stats: dict,
    opp_sp_hand: str,
    people_meta: dict[int, dict],
    savant: dict,
    fg_proj: dict,
) -> float:
    if pid in fg_proj["bat"]:
        fg = fg_proj["bat"][pid]
        base_bat_raa_pa = ((fg["wrc_plus"] / 100.0) - 1.0) * LG_RUNS_PER_PA
        base_woba = LG_WOBA + base_bat_raa_pa * WOBA_SCALE
        bsr_per_pa = fg["bsr"] / max(100.0, fg["pa"])
    else:
        raw_woba, denom_pa = calc_raw_woba(bat_stats)
        sav = savant["bat_xwoba"].get(pid)
        if sav and sav["pa"] > 0:
            blended_raw = 0.60 * sav["xwoba"] + 0.40 * sav["woba"]
            pa = sav["pa"]
            base_woba = (pa * blended_raw + 150.0 * LG_WOBA) / (pa + 150.0)
        else:
            base_woba = (denom_pa * raw_woba + 150.0 * LG_WOBA) / (denom_pa + 150.0)

        runner_runs = savant["baserunning"].get(pid, 0.0)
        eff_pa = max(denom_pa, sav["pa"] if sav else 0.0)
        bsr_per_pa = runner_runs / (eff_pa + 300.0)

    meta = people_meta.get(pid, {})
    bat_side = meta.get("batSide", "R")
    prior_delta = PLATOON_PRIOR_WOBA.get((bat_side, opp_sp_hand), 0.0)

    split_key = "vl" if opp_sp_hand == "L" else "vr"
    split_stat = meta.get("splits", {}).get(split_key)
    if split_stat:
        split_woba, split_pa = calc_raw_woba(split_stat)
        obs_delta = split_woba - base_woba
        platoon_delta = (split_pa * obs_delta + 500.0 * prior_delta) / (split_pa + 500.0)
    else:
        platoon_delta = prior_delta

    adj_woba = base_woba + 0.65 * platoon_delta
    bat_raa_pa = (adj_woba - LG_WOBA) / WOBA_SCALE
    return bat_raa_pa + bsr_per_pa


def estimate_pitcher_run_rate(
    pid: int,
    pitch_stats: dict,
    savant: dict,
    fg_proj: dict,
    prior_ip: float = 40.0,
) -> float:
    if pid in fg_proj["pit"]:
        return fg_proj["pit"][pid]["siera"]

    reg_fip, ip = calc_regressed_fip(pitch_stats, prior_ip=prior_ip)
    sav = savant["pit_xera"].get(pid)
    if sav and sav["pa"] > 0:
        sav_ip_equiv = sav["pa"] / 4.25
        reg_xera = (sav_ip_equiv * sav["xera"] + prior_ip * LG_ERA_FIP) / (sav_ip_equiv + prior_ip)
        return 0.50 * reg_xera + 0.50 * reg_fip
    return reg_fip


def estimate_sp_expected_innings(pid: int, pitch_stats: dict, fg_proj: dict) -> float:
    prior_ip_per_gs = 5.30
    prior_gs = 5.0

    if pid in fg_proj["pit"] and fg_proj["pit"][pid]["gs"] >= 1:
        fg = fg_proj["pit"][pid]
        return max(3.0, min(6.6, fg["ip"] / fg["gs"]))

    if not pitch_stats:
        return prior_ip_per_gs
    gp = pitch_stats.get("gamesPlayed", 0)
    gs = pitch_stats.get("gamesStarted", 0)
    ip = parse_ip(pitch_stats.get("inningsPitched", "0"))
    if gs <= 0 or ip <= 0:
        return prior_ip_per_gs

    relief_apps = max(0, gp - gs)
    est_start_ip = max(gs * 2.5, ip - 1.25 * relief_apps)
    reg_ip_gs = (est_start_ip + prior_gs * prior_ip_per_gs) / (gs + prior_gs)
    return max(3.0, min(6.6, reg_ip_gs))


def estimate_team_defense_raa(t_box: dict, savant: dict) -> float:
    batting_order = t_box.get("battingOrder", [])[:9]
    def_raa_game = 0.0

    for pid in batting_order:
        p_data = t_box["players"].get(f"ID{pid}", {})
        pos = p_data.get("position", {}).get("abbreviation", "")
        if pos == "DH":
            continue

        oaa_runs = savant["fielding_oaa"].get(pid, 0.0)
        def_raa_game += (oaa_runs / 145.0) * 0.75

        if pos == "C" and pid in savant["catcher_framing"]:
            frm = savant["catcher_framing"][pid]
            if frm["pitches"] > 0:
                rv_per_game = (frm["rv_tot"] / (frm["pitches"] + 3000.0)) * 145.0
                def_raa_game += rv_per_game

    return def_raa_game


def get_team_bullpen_rate(
    team_id: int,
    season: int,
    fatigued_loads: dict[int, int],
    savant: dict,
    fg_proj: dict,
) -> float:
    url = f"{BASE_URL}/teams/{team_id}/roster?rosterType=active&hydrate=person(stats(type=season,season={season},group=pitching))"
    data = SESSION.get(url, timeout=15).json()

    weighted_sum = 0.0
    total_weight = 0.0

    for entry in data.get("roster", []):
        if entry.get("position", {}).get("abbreviation") != "P":
            continue
        person = entry.get("person", {})
        pid = person.get("id")
        stats_list = person.get("stats", [])
        if not stats_list or not stats_list[0].get("splits"):
            continue
        p_stat = stats_list[0]["splits"][0].get("stat", {})

        gp = p_stat.get("gamesPlayed", 0)
        gs = p_stat.get("gamesStarted", 0)
        if gp == 0 or gs > gp * 0.5:
            continue

        rate = estimate_pitcher_run_rate(pid, p_stat, savant, fg_proj, prior_ip=25.0)
        recent_pitches = fatigued_loads.get(pid, 0)
        avail_weight = 0.25 if recent_pitches >= 30 else (0.60 if recent_pitches >= 15 else 1.0)
        ip = max(5.0, parse_ip(p_stat.get("inningsPitched", "0")))
        w = ip * avail_weight
        weighted_sum += rate * w
        total_weight += w

    return (weighted_sum / total_weight) if total_weight > 0 else LG_ERA_FIP


def to_american_odds(prob: float) -> str:
    prob = max(0.01, min(0.99, prob))
    if prob >= 0.5:
        return f"{-round(100 * prob / (1 - prob))}"
    return f"+{round(100 * (1 - prob) / prob)}"


def run_date(target_date: str):
    season = int(target_date[:4])
    print(f"Loading Baseball Savant leaderboards & FanGraphs projections (if present) for {season}...")
    savant = load_savant_data(season)
    fg_proj = load_fangraphs_projections()
    fg_status = (
        f"Loaded ({len(fg_proj['bat'])} bat / {len(fg_proj['pit'])} pit)"
        if (fg_proj["bat"] or fg_proj["pit"])
        else "Not found (using Statcast xwOBA/xERA + MLB API splits)"
    )
    print(f"  FanGraphs CSVs: {fg_status}")

    sched = SESSION.get(
        f"{BASE_URL}/schedule?sportId=1&date={target_date}&hydrate=venue", timeout=20
    ).json()
    games = sched.get("dates", [{}])[0].get("games", [])
    recent_loads = get_all_recent_pitch_loads(target_date)

    boxes = []
    all_pids = []
    for g in games:
        pk = g["gamePk"]
        box = SESSION.get(f"{BASE_URL}/game/{pk}/boxscore", timeout=20).json()
        boxes.append((g, box))
        for side in ("away", "home"):
            t_box = box["teams"][side]
            all_pids.extend(t_box.get("battingOrder", [])[:9])
            if t_box.get("pitchers"):
                all_pids.append(t_box["pitchers"][0])

    people_meta = fetch_people_platoon_and_meta(all_pids, season)

    print(f"\n=== MLB Model v2 Projections for {target_date} ({len(games)} games) ===\n")
    print(
        f"{'Matchup':<22} {'SPs (Exp IP)':<34} {'Off(A/H)':<13} {'Def(A/H)':<13} "
        f"{'Exp R (A-H)':<13} {'Home%':<7} {'Fair Odds (A/H)':<16} {'Actual'}"
    )
    print("-" * 136)

    bp_cache: dict[int, float] = {}

    for g, box in boxes:
        venue_name = g.get("venue", {}).get("name", "")
        pf = PARK_FACTORS.get(venue_name, 1.00)

        sp_hands = {}
        for side in ("away", "home"):
            pitchers = box["teams"][side].get("pitchers", [])
            sp_id = pitchers[0] if pitchers else None
            sp_hands[side] = people_meta.get(sp_id, {}).get("pitchHand", "R") if sp_id else "R"

        team_metrics = {}
        for side in ("away", "home"):
            opp_side = "home" if side == "away" else "away"
            t_box = box["teams"][side]
            tid = t_box["team"]["id"]
            t_abbr = t_box["team"].get("abbreviation", t_box["team"]["name"][:3].upper())

            batting_order = t_box.get("battingOrder", [])[:9]
            off_raa = 0.0
            for idx, pid in enumerate(batting_order):
                p_data = t_box["players"].get(f"ID{pid}", {})
                bat_stats = p_data.get("seasonStats", {}).get("batting", {})
                raa_pa = estimate_batter_raa_per_pa(
                    pid, bat_stats, sp_hands[opp_side], people_meta, savant, fg_proj
                )
                pa_weight = PA_BY_SPOT[idx] if idx < 9 else 3.85
                off_raa += pa_weight * raa_pa

            pitchers = t_box.get("pitchers", [])
            sp_name, sp_rate, sp_ip = "TBD", LG_ERA_FIP, 5.30
            if pitchers:
                sp_id = pitchers[0]
                sp_data = t_box["players"].get(f"ID{sp_id}", {})
                sp_name = sp_data.get("person", {}).get("fullName", "TBD").split()[-1]
                sp_stats = sp_data.get("seasonStats", {}).get("pitching", {})
                sp_rate = estimate_pitcher_run_rate(sp_id, sp_stats, savant, fg_proj, prior_ip=40.0)
                sp_ip = estimate_sp_expected_innings(sp_id, sp_stats, fg_proj)

            if tid not in bp_cache:
                bp_cache[tid] = get_team_bullpen_rate(tid, season, recent_loads, savant, fg_proj)
            bp_rate = bp_cache[tid]
            bp_ip = 9.0 - sp_ip

            pitch_raa_allowed = (sp_ip / 9.0) * (sp_rate - LG_ERA_FIP) + (bp_ip / 9.0) * (bp_rate - LG_ERA_FIP)
            def_raa = estimate_team_defense_raa(t_box, savant)
            actual_runs = t_box.get("teamStats", {}).get("batting", {}).get("runs", "-")

            team_metrics[side] = {
                "abbr": t_abbr,
                "off_raa": off_raa,
                "pitch_raa_allowed": pitch_raa_allowed,
                "def_raa": def_raa,
                "sp_name": sp_name,
                "sp_hand": sp_hands[side],
                "sp_ip": sp_ip,
                "actual_runs": actual_runs,
            }

        away_m, home_m = team_metrics["away"], team_metrics["home"]

        rs_away = max(
            1.5,
            (LG_RUNS_PER_GAME + away_m["off_raa"] + home_m["pitch_raa_allowed"] - home_m["def_raa"]) * pf,
        )
        rs_home = max(
            1.5,
            (LG_RUNS_PER_GAME + home_m["off_raa"] + away_m["pitch_raa_allowed"] - away_m["def_raa"]) * pf,
        )

        gamma = (rs_home + rs_away) ** 0.287
        p_home_neutral = (rs_home ** gamma) / ((rs_home ** gamma) + (rs_away ** gamma))
        logit_home = math.log(p_home_neutral / (1.0 - p_home_neutral)) + HFA_LOG_ODDS
        p_home = 1.0 / (1.0 + math.exp(-logit_home))
        p_away = 1.0 - p_home

        matchup_str = f"{away_m['abbr']} @ {home_m['abbr']} ({pf:.2f})"
        sp_str = f"{away_m['sp_name']}({away_m['sp_ip']:.1f}) v {home_m['sp_name']}({home_m['sp_ip']:.1f})"
        off_str = f"{away_m['off_raa']:+.2f}/{home_m['off_raa']:+.2f}"
        def_str = f"{away_m['def_raa']:+.2f}/{home_m['def_raa']:+.2f}"
        exp_r_str = f"{rs_away:.2f} - {rs_home:.2f}"
        odds_str = f"{to_american_odds(p_away)} / {to_american_odds(p_home)}"
        actual_str = f"{away_m['actual_runs']} - {home_m['actual_runs']}"

        print(
            f"{matchup_str:<22} {sp_str:<34} {off_str:<13} {def_str:<13} "
            f"{exp_r_str:<13} {p_home*100:>5.1f}%  {odds_str:<16} {actual_str}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MLB Win-Probability Model v2 for a given date.")
    parser.add_argument("--date", default="2026-07-04", help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    run_date(args.date)
