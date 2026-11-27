"""
Incremental daily MLB runner & GitHub Pages JSON exporter.

Uses docs/data/mlb/latest.json (keyed by MLB game_pk) as a lightweight per-game
state machine so GitHub Actions can run every 30 minutes during MLB season while:
  1. Modeling each game at most twice: once with projected lineups, and once when
     both teams' official 9-man starting lineups are released.
  2. Never deleting Live or Final games on today's slate (and freezing their
     pregame predictions & closing pregame odds once first pitch occurs).
  3. Refreshing ESPN DraftKings moneylines & recomputing EV/Kelly for Preview
     games without re-running the MLB model (skipping Savant/bullpen loads).
"""

import argparse
import json
import math
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from data_sources import (
    BASE_URL,
    SESSION,
    fetch_people_platoon_and_meta,
    get_all_recent_pitch_loads,
    get_projected_lineup_for_team,
    load_fangraphs_projections,
    load_savant_data,
)
from model import (
    HFA_LOG_ODDS,
    LG_ERA_FIP,
    LG_RUNS_PER_GAME,
    PA_BY_SPOT,
    PARK_FACTORS,
    estimate_batter_raa_per_pa,
    estimate_pitcher_run_rate,
    estimate_sp_expected_innings,
    estimate_team_defense_raa,
    fetch_team_pitching_roster,
    find_bulk_pitcher,
    get_team_bullpen_rate,
    is_opener_pitcher,
    to_american_odds,
)
from odds import calc_ev, calc_kelly_fraction, fetch_espn_mlb_odds

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


def _format_pt_time(iso_utc: str | None) -> str:
    if not iso_utc:
        return ""
    try:
        dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00")).astimezone(PACIFIC_TZ)
        return dt.strftime("%-I:%M %p PT")
    except Exception:
        return ""


def _apply_odds(rec: dict, mkt: dict | None, kelly_mult: float = 0.25) -> None:
    """Attach/refresh ESPN DraftKings moneylines and recompute EV & Kelly."""
    if not mkt:
        return
    away_ml, provider = mkt["best_away"]
    home_ml, _ = mkt["best_home"]
    p_away = float(rec["p_away"])
    p_home = float(rec["p_home"])

    ev_away = calc_ev(p_away, away_ml)
    ev_home = calc_ev(p_home, home_ml)
    kelly_away = calc_kelly_fraction(p_away, away_ml, kelly_mult=kelly_mult)
    kelly_home = calc_kelly_fraction(p_home, home_ml, kelly_mult=kelly_mult)

    if ev_away >= ev_home:
        best_side, best_ml, best_ev, best_kelly = rec["away_abbr"], away_ml, ev_away, kelly_away
    else:
        best_side, best_ml, best_ev, best_kelly = rec["home_abbr"], home_ml, ev_home, kelly_home

    rec.update(
        {
            "odds_provider": provider,
            "book_away_ml": int(away_ml),
            "book_home_ml": int(home_ml),
            "no_vig_away": round(float(mkt["no_vig_away"]), 4),
            "no_vig_home": round(float(mkt["no_vig_home"]), 4),
            "ev_away": round(ev_away, 4),
            "ev_home": round(ev_home, 4),
            "kelly_away": round(kelly_away, 4),
            "kelly_home": round(kelly_home, 4),
            "best_side": best_side,
            "best_ml": int(best_ml),
            "best_ev": round(best_ev, 4),
            "best_kelly": round(best_kelly, 4),
        }
    )


def _model_games(
    target_date: str,
    season: int,
    games_with_boxes: list[tuple[dict, dict]],
) -> dict[int, dict]:
    """Run the MLB RAA + Pythagenpat + HFA model for only the requested games."""
    if not games_with_boxes:
        return {}

    savant = load_savant_data(season)
    fg_proj = load_fangraphs_projections()
    recent_loads = get_all_recent_pitch_loads(target_date)

    all_pids = []
    for g, box in games_with_boxes:
        for side in ("away", "home"):
            t_box = box["teams"][side]
            all_pids.extend(t_box.get("battingOrder", [])[:9])
            if t_box.get("pitchers"):
                all_pids.append(t_box["pitchers"][0])
            else:
                prob_sp = g.get("teams", {}).get(side, {}).get("probablePitcher", {})
                if prob_sp.get("id"):
                    all_pids.append(prob_sp["id"])

    people_meta = fetch_people_platoon_and_meta(all_pids, season)
    roster_cache: dict[int, list[dict]] = {}
    proj_lineup_cache: dict[tuple[int, str], tuple[list[int], dict]] = {}
    modeled: dict[int, dict] = {}

    for g, box in games_with_boxes:
        pk = int(g["gamePk"])
        venue_name = g.get("venue", {}).get("name", "")
        pf = PARK_FACTORS.get(venue_name, 1.00)

        pitching_plans = {}
        for side in ("away", "home"):
            t_box = box["teams"][side]
            tid = t_box["team"]["id"]
            t_abbr = t_box["team"].get("abbreviation", t_box["team"]["name"][:3].upper())
            t_name = t_box["team"].get("name", t_abbr)

            if tid not in roster_cache:
                roster_cache[tid] = fetch_team_pitching_roster(tid, season)
            roster = roster_cache[tid]
            roster_by_id = {p["id"]: p for p in roster}

            pitchers = t_box.get("pitchers", [])
            prob_sp = g.get("teams", {}).get(side, {}).get("probablePitcher", {})
            sp_id = pitchers[0] if pitchers else prob_sp.get("id")

            sp_name, sp_rate, sp_ip, is_opener = "TBD", LG_ERA_FIP, 5.30, False
            sp_hand = people_meta.get(sp_id, {}).get("pitchHand", "R") if sp_id else "R"

            if sp_id:
                sp_data = t_box["players"].get(f"ID{sp_id}", {})
                if sp_data and sp_data.get("seasonStats", {}).get("pitching"):
                    sp_name = sp_data.get("person", {}).get("fullName", "TBD").split()[-1]
                    sp_stats = sp_data["seasonStats"]["pitching"]
                elif sp_id in roster_by_id:
                    sp_name = roster_by_id[sp_id]["fullName"].split()[-1]
                    sp_stats = roster_by_id[sp_id]["stat"]
                else:
                    sp_name = prob_sp.get("fullName", "TBD").split()[-1]
                    sp_stats = {}

                is_opener = is_opener_pitcher(sp_stats)
                sp_age = people_meta.get(sp_id, {}).get('currentAge')
                sp_rate = estimate_pitcher_run_rate(sp_id, sp_stats, savant, fg_proj, prior_ip=60.0, age=sp_age)
                sp_ip = estimate_sp_expected_innings(sp_id, sp_stats, fg_proj, season=season, before_date=target_date)

            bulk = None
            if is_opener:
                bulk = find_bulk_pitcher(roster, sp_id, recent_loads, savant, fg_proj, None)

            exclude_ids = {pid for pid in (sp_id, bulk["id"] if bulk else None) if pid is not None}
            bp_rate = get_team_bullpen_rate(roster, recent_loads, savant, fg_proj, exclude_ids=exclude_ids)

            if bulk:
                bulk_ip = bulk["exp_ip"]
                bp_ip = max(0.0, 9.0 - sp_ip - bulk_ip)
                pitch_raa_allowed = (
                    (sp_ip / 9.0) * (sp_rate - LG_ERA_FIP)
                    + (bulk_ip / 9.0) * (bulk["rate"] - LG_ERA_FIP)
                    + (bp_ip / 9.0) * (bp_rate - LG_ERA_FIP)
                )
                primary_hand = bulk["pitchHand"]
                sp_label = f"{sp_name}(OP)+{bulk['name']}({bulk_ip:.1f})"
            else:
                bp_ip = 9.0 - sp_ip
                pitch_raa_allowed = (sp_ip / 9.0) * (sp_rate - LG_ERA_FIP) + (bp_ip / 9.0) * (
                    bp_rate - LG_ERA_FIP
                )
                primary_hand = sp_hand
                sp_label = f"{sp_name}({sp_ip:.1f}/OP)" if is_opener else f"{sp_name}({sp_ip:.1f})"

            pitching_plans[side] = {
                "tid": tid,
                "abbr": t_abbr,
                "name": t_name,
                "pitch_raa_allowed": pitch_raa_allowed,
                "primary_hand": primary_hand,
                "sp_label": sp_label,
            }

        team_metrics = {}
        for side in ("away", "home"):
            opp_side = "home" if side == "away" else "away"
            opp_hand = pitching_plans[opp_side]["primary_hand"]
            t_box = box["teams"][side]
            tid = pitching_plans[side]["tid"]

            official_order = t_box.get("battingOrder", [])[:9]
            if len(official_order) >= 9:
                is_official = True
                batting_order = official_order
                players_dict = t_box.get("players", {})
            else:
                is_official = False
                cache_key = (tid, opp_hand)
                if cache_key not in proj_lineup_cache:
                    proj_lineup_cache[cache_key] = get_projected_lineup_for_team(tid, target_date, opp_hand)
                batting_order, players_dict = proj_lineup_cache[cache_key]

                missing_pids = [pid for pid in batting_order if pid not in people_meta]
                if missing_pids:
                    people_meta.update(fetch_people_platoon_and_meta(missing_pids, season))

            off_raa = 0.0
            for idx, pid in enumerate(batting_order):
                p_data = players_dict.get(f"ID{pid}", {})
                bat_stats = p_data.get("seasonStats", {}).get("batting", {})
                raa_pa = estimate_batter_raa_per_pa(pid, bat_stats, opp_hand, people_meta, savant, fg_proj)
                pa_weight = PA_BY_SPOT[idx] if idx < 9 else 3.85
                off_raa += pa_weight * raa_pa

            def_raa = estimate_team_defense_raa(batting_order, players_dict, savant)
            team_metrics[side] = {
                "abbr": pitching_plans[side]["abbr"],
                "name": pitching_plans[side]["name"],
                "off_raa": off_raa,
                "pitch_raa_allowed": pitching_plans[side]["pitch_raa_allowed"],
                "def_raa": def_raa,
                "sp_label": pitching_plans[side]["sp_label"],
                "is_official": is_official,
            }

        away_m, home_m = team_metrics["away"], team_metrics["home"]
        both_official = away_m["is_official"] and home_m["is_official"]

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

        modeled[pk] = {
            "game_pk": pk,
            "away_abbr": away_m["abbr"],
            "home_abbr": home_m["abbr"],
            "away_name": away_m["name"],
            "home_name": home_m["name"],
            "away_sp": away_m["sp_label"],
            "home_sp": home_m["sp_label"],
            "venue": venue_name,
            "park_factor": round(pf, 2),
            "lineup_status": "official" if both_official else "projected",
            "rs_away": round(rs_away, 2),
            "rs_home": round(rs_home, 2),
            "p_away": round(p_away, 4),
            "p_home": round(p_home, 4),
            "fair_away": to_american_odds(p_away),
            "fair_home": to_american_odds(p_home),
            "odds_provider": None,
            "book_away_ml": None,
            "book_home_ml": None,
            "no_vig_away": None,
            "no_vig_home": None,
            "ev_away": None,
            "ev_home": None,
            "kelly_away": None,
            "kelly_home": None,
            "best_side": None,
            "best_ml": None,
            "best_ev": None,
            "best_kelly": None,
        }

    return modeled


def sync_mlb_slate(
    target_date: str | None = None,
    json_path: str = "../docs/data/mlb/latest.json",
    kelly_mult: float = 0.25,
) -> dict:
    now_pt = datetime.now(PACIFIC_TZ)
    slate_date = target_date or now_pt.strftime("%Y-%m-%d")
    season = int(slate_date[:4])

    existing_by_pk: dict[int, dict] = {}
    prev_games_snapshot: list[dict] | None = None
    prev_slate_date: str | None = None

    if os.path.exists(json_path):
        try:
            with open(json_path, encoding="utf-8") as f:
                prev_payload = json.load(f)
            prev_slate_date = prev_payload.get("slate_date")
            if prev_slate_date == slate_date:
                prev_games_snapshot = prev_payload.get("games", [])
                for rec in prev_games_snapshot:
                    existing_by_pk[int(rec["game_pk"])] = dict(rec)
        except Exception as exc:
            print(f"Warning: could not read existing {json_path} ({exc}); starting fresh.")

    sched = SESSION.get(
        f"{BASE_URL}/schedule?sportId=1&date={slate_date}&hydrate=venue,probablePitcher",
        timeout=20,
    ).json()
    raw_games = sched.get("dates", [{}])[0].get("games", []) if sched.get("dates") else []

    if not raw_games and not existing_by_pk:
        payload = {
            "slate_date": slate_date,
            "updated_at": now_pt.strftime("%Y-%m-%d %I:%M %p %Z"),
            "games": [],
        }
        if prev_slate_date != slate_date or prev_games_snapshot != []:
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        print(f"No MLB games scheduled for {slate_date}.")
        return payload

    games_needing_model: list[tuple[dict, dict]] = []
    live_meta_by_pk: dict[int, dict] = {}
    preview_pks: set[int] = set()
    new_pks: set[int] = set()

    for g in raw_games:
        pk = int(g["gamePk"])
        status = g.get("status", {})
        game_state = status.get("abstractGameState", "Preview")
        detailed_state = status.get("detailedState", game_state)
        away_score = g.get("teams", {}).get("away", {}).get("score")
        home_score = g.get("teams", {}).get("home", {}).get("score")

        live_meta_by_pk[pk] = {
            "game_state": game_state,
            "detailed_state": detailed_state,
            "game_time_utc": g.get("gameDate", ""),
            "game_time_pt": _format_pt_time(g.get("gameDate")),
            "away_score": away_score,
            "home_score": home_score,
        }

        prev = existing_by_pk.get(pk)

        if prev is None:
            box = SESSION.get(f"{BASE_URL}/game/{pk}/boxscore", timeout=20).json()
            games_needing_model.append((g, box))
            new_pks.add(pk)
            if game_state == "Preview":
                preview_pks.add(pk)
            continue

        if game_state != "Preview":
            # Live or Final: never delete, never re-model, never overwrite pregame odds
            continue

        preview_pks.add(pk)

        if prev.get("lineup_status") != "official":
            box = SESSION.get(f"{BASE_URL}/game/{pk}/boxscore", timeout=20).json()
            away_ord = box["teams"]["away"].get("battingOrder", [])[:9]
            home_ord = box["teams"]["home"].get("battingOrder", [])[:9]
            if len(away_ord) >= 9 and len(home_ord) >= 9:
                games_needing_model.append((g, box))

    print(
        f"MLB {slate_date}: {len(raw_games)} scheduled | "
        f"{len(games_needing_model)} needing model run | "
        f"{len(preview_pks)} Preview (odds refresh) | "
        f"{len(raw_games) - len(preview_pks)} Live/Final (locked)"
    )

    if games_needing_model:
        modeled = _model_games(slate_date, season, games_needing_model)
        for pk, rec in modeled.items():
            prev = existing_by_pk.get(pk)
            if prev and prev.get("book_away_ml") is not None:
                for k in (
                    "odds_provider", "book_away_ml", "book_home_ml",
                    "no_vig_away", "no_vig_home", "ev_away", "ev_home",
                    "kelly_away", "kelly_home", "best_side", "best_ml",
                    "best_ev", "best_kelly",
                ):
                    rec[k] = prev.get(k)
            existing_by_pk[pk] = rec

    for pk, meta in live_meta_by_pk.items():
        if pk in existing_by_pk:
            existing_by_pk[pk].update(meta)

    odds_target_pks = preview_pks | new_pks
    if odds_target_pks:
        odds_map = fetch_espn_mlb_odds(slate_date)
        for pk in odds_target_pks:
            rec = existing_by_pk.get(pk)
            if not rec:
                continue
            mkt = odds_map.get((rec["away_abbr"], rec["home_abbr"]))
            _apply_odds(rec, mkt, kelly_mult=kelly_mult)

    games_out = sorted(
        existing_by_pk.values(),
        key=lambda r: (r.get("game_time_utc", ""), r.get("game_pk", 0)),
    )

    if prev_slate_date == slate_date and prev_games_snapshot == games_out:
        print("No MLB lineup, odds, or score changes; skipping JSON write.")
        return {"slate_date": slate_date, "games": games_out}

    payload = {
        "slate_date": slate_date,
        "updated_at": now_pt.strftime("%Y-%m-%d %I:%M %p %Z"),
        "games": games_out,
    }
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {len(games_out)} MLB games to {json_path}.")
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync daily MLB slate (projected -> official -> locked) + odds.")
    parser.add_argument("--date", default=None, help="Optional date (YYYY-MM-DD); defaults to today (PT).")
    parser.add_argument("--json-path", default="../docs/data/mlb/latest.json", help="Output latest.json path.")
    parser.add_argument("--kelly", type=float, default=0.25, help="Fractional Kelly multiplier (default: 0.25).")
    args = parser.parse_args()

    sync_mlb_slate(target_date=args.date, json_path=args.json_path, kelly_mult=args.kelly)
