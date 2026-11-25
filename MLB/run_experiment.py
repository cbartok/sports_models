"""
Empirical study on 17,006 MLB games (2019, 2021-2026) to calibrate HFA
and evaluate rest/travel impact on out-of-sample win probabilities.
"""
import math
import time
import numpy as np
import pandas as pd
import requests
import statsmodels.api as sm
from sklearn.metrics import brier_score_loss, log_loss

SEASONS = [2019, 2021, 2022, 2023, 2024, 2025, 2026]
BASE_URL = "https://statsapi.mlb.com/api/v1/schedule"


def haversine_miles(lat1, lon1, lat2, lon2):
    if any(pd.isna(v) for v in (lat1, lon1, lat2, lon2)):
        return 0.0
    R = 3958.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def fetch_season(season: int) -> list[dict]:
    params = {
        "sportId": 1,
        "season": season,
        "gameType": "R",
        "hydrate": "venue(location,timezone)",
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    seen_pks = set()
    for date_entry in data.get("dates", []):
        for g in date_entry.get("games", []):
            pk = g.get("gamePk")
            if pk in seen_pks:
                continue
            status = g.get("status", {})
            if status.get("abstractGameState") != "Final":
                continue

            teams = g.get("teams", {})
            home = teams.get("home", {})
            away = teams.get("away", {})
            if "score" not in home or "score" not in away:
                continue

            venue = g.get("venue", {})
            loc = venue.get("location", {})
            coords = loc.get("defaultCoordinates", {})
            tz = venue.get("timeZone", {})

            seen_pks.add(pk)
            rows.append({
                "game_pk": pk,
                "season": season,
                "official_date": g.get("officialDate"),
                "game_date_utc": g.get("gameDate"),
                "home_id": home["team"]["id"],
                "home_name": home["team"]["name"],
                "home_score": int(home["score"]),
                "away_id": away["team"]["id"],
                "away_name": away["team"]["name"],
                "away_score": int(away["score"]),
                "venue_id": venue.get("id"),
                "lat": coords.get("latitude"),
                "lon": coords.get("longitude"),
                "tz_offset": tz.get("offset") if tz.get("offset") is not None else -5,
            })
    return rows


def main():
    all_rows = []
    for s in SEASONS:
        t0 = time.time()
        rows = fetch_season(s)
        print(f"Season {s}: {len(rows)} final regular-season games ({time.time() - t0:.1f}s)")
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    df["dt_utc"] = pd.to_datetime(df["game_date_utc"], utc=True)
    df["date_dt"] = pd.to_datetime(df["official_date"])
    df["local_hour"] = (df["dt_utc"].dt.hour + df["tz_offset"]) % 24
    df = df.sort_values(["dt_utc", "game_pk"]).reset_index(drop=True)

    print(f"\nTotal games loaded: {len(df)}")

    team_state = {}
    prev_season_rd = {}
    feat_rows = []
    current_season = None

    for idx, row in df.iterrows():
        season = row["season"]
        if season != current_season:
            if current_season is not None:
                for tid, st in team_state.items():
                    if st["gp"] > 0:
                        prev_season_rd[tid] = (st["rs"] - st["ra"]) / st["gp"]
            team_state = {}
            current_season = season

        def get_team_feats(tid, is_home):
            st = team_state.get(tid)
            prior_rd = prev_season_rd.get(tid, 0.0)
            if st is None:
                return {
                    "rd_shrunk": 0.25 * prior_rd,
                    "days_rest": 3,
                    "had_off_day": 1,
                    "quick_turnaround": 0,
                    "east_tz_no_rest": 0.0,
                    "travel_1000mi_no_rest": 0.0,
                    "consec_games": 1,
                }

            rd_shrunk = ((st["rs"] - st["ra"]) + 30.0 * 0.25 * prior_rd) / (st["gp"] + 30.0)
            days_diff = (row["date_dt"] - st["prev_date"]).days
            days_rest = max(0, days_diff - 1)
            had_off_day = 1 if days_rest >= 1 else 0

            quick_turnaround = 1 if (days_rest == 0 and st["prev_local_hour"] >= 18 and row["local_hour"] < 16) else 0
            tz_shift = row["tz_offset"] - st["prev_tz"]
            east_tz_no_rest = max(0.0, float(tz_shift)) if days_rest == 0 else 0.0
            miles = haversine_miles(st["prev_lat"], st["prev_lon"], row["lat"], row["lon"])
            travel_1000mi_no_rest = (miles / 1000.0) if days_rest == 0 else 0.0
            consec_games = 1 if days_rest >= 1 else (st["consec_games"] + 1)

            return {
                "rd_shrunk": rd_shrunk,
                "days_rest": min(days_rest, 3),
                "had_off_day": had_off_day,
                "quick_turnaround": quick_turnaround,
                "east_tz_no_rest": east_tz_no_rest,
                "travel_1000mi_no_rest": travel_1000mi_no_rest,
                "consec_games": consec_games,
            }

        hf = get_team_feats(row["home_id"], True)
        af = get_team_feats(row["away_id"], False)

        feat_rows.append({
            "season": season,
            "home_win": 1 if row["home_score"] > row["away_score"] else 0,
            "run_diff": row["home_score"] - row["away_score"],
            "talent_diff": hf["rd_shrunk"] - af["rd_shrunk"],
            "off_day_diff": hf["had_off_day"] - af["had_off_day"],
            "quick_turnaround_diff": hf["quick_turnaround"] - af["quick_turnaround"],
            "east_tz_no_rest_diff": hf["east_tz_no_rest"] - af["east_tz_no_rest"],
            "travel_1000mi_no_rest_diff": hf["travel_1000mi_no_rest"] - af["travel_1000mi_no_rest"],
            "consec_games_diff": hf["consec_games"] - af["consec_games"],
        })

        for tid, rs, ra in [
            (row["home_id"], row["home_score"], row["away_score"]),
            (row["away_id"], row["away_score"], row["home_score"]),
        ]:
            if tid not in team_state:
                team_state[tid] = {
                    "gp": 1,
                    "rs": rs,
                    "ra": ra,
                    "prev_date": row["date_dt"],
                    "prev_local_hour": row["local_hour"],
                    "prev_tz": row["tz_offset"],
                    "prev_lat": row["lat"],
                    "prev_lon": row["lon"],
                    "consec_games": 1,
                }
            else:
                st = team_state[tid]
                days_diff = (row["date_dt"] - st["prev_date"]).days
                consec = 1 if days_diff >= 2 else st["consec_games"] + 1
                st["gp"] += 1
                st["rs"] += rs
                st["ra"] += ra
                st["prev_date"] = row["date_dt"]
                st["prev_local_hour"] = row["local_hour"]
                st["prev_tz"] = row["tz_offset"]
                st["prev_lat"] = row["lat"]
                st["prev_lon"] = row["lon"]
                st["consec_games"] = consec

    fdf = pd.DataFrame(feat_rows)

    print("\n--- Summary Stats ---")
    print(f"Raw Home Win %: {fdf['home_win'].mean():.4f}")
    print(f"Raw Avg Home Run Diff: {fdf['run_diff'].mean():+.4f} runs/game")

    feature_cols = [
        "talent_diff",
        "off_day_diff",
        "quick_turnaround_diff",
        "east_tz_no_rest_diff",
        "travel_1000mi_no_rest_diff",
        "consec_games_diff",
    ]

    X = sm.add_constant(fdf[feature_cols])
    logit_res = sm.Logit(fdf["home_win"], X).fit(disp=False)
    print("\n--- Logistic Regression (Predicting Home Win) ---")
    print(logit_res.summary2().tables[1][["Coef.", "Std.Err.", "z", "P>|z|"]])

    hfa_prob = 1.0 / (1.0 + math.exp(-logit_res.params["const"]))
    print(f"\nImplied Neutral-Rest/Equal-Team Home Win%: {hfa_prob:.4f}")

    train = fdf[fdf["season"] <= 2023]
    test = fdf[fdf["season"] >= 2024]
    m_base = sm.Logit(train["home_win"], sm.add_constant(train[["talent_diff"]])).fit(disp=False)
    p_base = m_base.predict(sm.add_constant(test[["talent_diff"]]))
    m_full = sm.Logit(train["home_win"], sm.add_constant(train[feature_cols])).fit(disp=False)
    p_full = m_full.predict(sm.add_constant(test[feature_cols]))

    print(f"\nOut-of-Sample LogLoss (Baseline Talent+HFA): {log_loss(test['home_win'], p_base):.6f}")
    print(f"Out-of-Sample LogLoss (Full + Rest/Travel): {log_loss(test['home_win'], p_full):.6f}")


if __name__ == "__main__":
    main()
