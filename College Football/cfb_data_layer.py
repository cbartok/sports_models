import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}
ESPN_CFB_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football"
CFBD_BASE_URL = "https://api.collegefootballdata.com"


def _cfb_season_for_date(dt):
    dt = pd.to_datetime(dt)
    return dt.year - 1 if dt.month <= 2 else dt.year


def _format_short_date(dt):
    dt = pd.to_datetime(dt)
    return f"{dt.strftime('%b')} {dt.day}"


class CfbDataLayer:
    def __init__(self, cfbd_api_key=None, odds_api_key=None, max_workers=10):
        self.cfbd_api_key = cfbd_api_key or os.getenv("CFBD_API_KEY")
        self.odds_api_key = odds_api_key or os.getenv("THE_ODDS_API_KEY")
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.team_replace = {
            "Bowling Green": "Bowling Green State", "Bowling Grn": "Bowling Green State",
            "BYU": "Brigham Young", "UL-Lafayette": "Louisiana", "ULLafayette": "Louisiana",
            "LA Lafayette": "Louisiana", "Louisiana-Lafayette": "Louisiana", "Lafayette": "Louisiana",
            "UL-Monroe": "Louisiana-Monroe", "ULMonroe": "Louisiana-Monroe", "UL Monroe": "Louisiana-Monroe",
            "LA Monroe": "Louisiana-Monroe", "ULM": "Louisiana-Monroe", "Miami-FL": "Miami (FL)",
            "Miami FL": "Miami (FL)", "Miami": "Miami (FL)", "Miami Florida": "Miami (FL)",
            "Miami-OH": "Miami (OH)", "Miami OH": "Miami (OH)", "Miami (Ohio)": "Miami (OH)",
            "Miami Ohio": "Miami (OH)", "Middle Tennessee": "Middle Tennessee State",
            "MTSU": "Middle Tennessee State", "Middle Tenn": "Middle Tennessee State",
            "Middle Tenn St": "Middle Tennessee State", "Mid Tennessee State": "Middle Tennessee State",
            "NC State": "North Carolina State", "N.C. State": "North Carolina State", "NCState": "North Carolina State",
            "Pittsburgh": "Pitt", "Pittsburgh U": "Pitt", "Southern Miss": "Southern Mississippi",
            "So Mississippi": "Southern Mississippi", "S Mississippi": "Southern Mississippi",
            "TCU": "Texas Christian", "TX Christian": "Texas Christian", "Central Florida": "UCF",
            "Central FL": "UCF", "UT San Antonio": "UTSA", "Tex San Antonio": "UTSA", "TX-San Ant": "UTSA",
            "Kent": "Kent State", "N Illinois": "Northern Illinois", "No Illinois": "Northern Illinois",
            "C Michigan": "Central Michigan", "Central Mich": "Central Michigan",
            "FL Atlantic": "Florida Atlantic", "Fla Atlantic": "Florida Atlantic", "FAU": "Florida Atlantic",
            "Coastal Car": "Coastal Carolina", "Mississippi": "Ole Miss", "WKU": "Western Kentucky",
            "W Kentucky": "Western Kentucky", "W Michigan": "Western Michigan",
            "Florida Intl": "Florida International", "FIU": "Florida International",
            "E Michigan": "Eastern Michigan", "Ga Southern": "Georgia Southern", "GA Southern": "Georgia Southern",
            "Georgia So": "Georgia Southern", "App State": "Appalachian State", "Appalachian St": "Appalachian State",
            "U Mass": "Massachusetts", "UMass": "Massachusetts", "Connecticut": "UConn",
            "N Carolina": "North Carolina", "UNC": "North Carolina", "S Methodist": "SMU",
            "Southern Methodist": "SMU", "LA Tech": "Louisiana Tech", "W Virginia": "West Virginia",
            "Wash State": "Washington State", "N Mex State": "New Mexico State",
            "Miss State": "Mississippi State", "Mississippi St": "Mississippi State", "TX El Paso": "UTEP",
            "S Alabama": "South Alabama", "GA Tech": "Georgia Tech", "S Florida": "South Florida",
            "USF": "South Florida", "Boston Col": "Boston College", "E Carolina": "East Carolina",
            "S Carolina": "South Carolina", "VA Tech": "Virginia Tech", "Sam Houston": "Sam Houston State",
            "Sam Hous State": "Sam Houston State", "J Madison": "James Madison", "James Mad": "James Madison",
            "Jacksonville St": "Jacksonville State", "Jksnville State": "Jacksonville State",
            "Kennesaw": "Kennesaw State", "Hawai'i": "Hawaii", "San José State": "San Jose State",
            "Southern California": "USC", "Louisiana State": "LSU", "Alabama-Birmingham": "UAB",
            "Nevada-Las Vegas": "UNLV", "Army West Point": "Army", "N Texas": "North Texas",
        }

    def normalize_team_name(self, series):
        s = series.astype(str).str.strip().str.replace(r"\bSt\.?$", "State", regex=True)
        return s.replace(self.team_replace)

    def pull_teams_information(self):
        resp = self.session.get(f"{ESPN_CFB_BASE_URL}/teams", params={"groups": "80", "limit": 1000}, timeout=15)
        resp.raise_for_status()
        rows = [
            {"team_id": str(e["team"]["id"]), "name": e["team"].get("location") or e["team"].get("displayName"), "display_name": e["team"].get("displayName"), "abbreviation": e["team"].get("abbreviation", "").lower()}
            for e in resp.json()["sports"][0]["leagues"][0]["teams"]
        ]
        self.all_team_data = pd.DataFrame(rows)
        self.all_team_data["name"] = self.normalize_team_name(self.all_team_data["name"])
        self.team_list = list(self.all_team_data["name"].unique())
        self.team_abbreviations = list(self.all_team_data["abbreviation"].unique())
        return self.all_team_data

    def _fetch_espn_scoreboard_day(self, day_dt):
        date_str = pd.to_datetime(day_dt).strftime("%Y%m%d")
        resp = self.session.get(f"{ESPN_CFB_BASE_URL}/scoreboard", params={"dates": date_str, "groups": "80", "limit": 500}, timeout=15)
        resp.raise_for_status()
        games = []
        for ev in resp.json().get("events", []):
            comp = ev["competitions"][0]
            competitors = {c["homeAway"]: c for c in comp.get("competitors", [])}
            if "home" not in competitors or "away" not in competitors:
                continue
            home, away = competitors["home"], competitors["away"]
            game_dt = pd.to_datetime(ev["date"], utc=True).tz_convert("US/Eastern")
            completed = comp.get("status", {}).get("type", {}).get("completed", False)
            spread = np.nan
            odds_list = comp.get("odds") or []
            if odds_list and odds_list[0].get("spread") is not None:
                try:
                    spread = -float(odds_list[0]["spread"])
                except (TypeError, ValueError):
                    spread = np.nan
            games.append({
                "game_id": str(ev["id"]), "datetime": game_dt.tz_localize(None), "date": _format_short_date(game_dt),
                "completed": completed, "neutral": int(bool(comp.get("neutralSite", False))),
                "away_id": str(away["team"]["id"]), "away_abbr": away["team"].get("abbreviation", "").lower(),
                "away_name": away["team"].get("location") or away["team"].get("displayName"),
                "away_score": pd.to_numeric(away.get("score"), errors="coerce") if completed else np.nan,
                "home_id": str(home["team"]["id"]), "home_abbr": home["team"].get("abbreviation", "").lower(),
                "home_name": home["team"].get("location") or home["team"].get("displayName"),
                "home_score": pd.to_numeric(home.get("score"), errors="coerce") if completed else np.nan,
                "espn_spread": spread,
            })
        return games

    def _fetch_espn_team_schedule_dates(self, team_id, season):
        resp = self.session.get(f"{ESPN_CFB_BASE_URL}/teams/{team_id}/schedule", params={"season": season}, timeout=15)
        resp.raise_for_status()
        rows = [
            {"team_id": str(team_id), "game_id": str(ev["id"]), "game_day": pd.to_datetime(ev["date"], utc=True).tz_convert("US/Eastern").tz_localize(None).normalize()}
            for ev in resp.json().get("events", [])
        ]
        if not rows:
            return pd.DataFrame(columns=["team_id", "game_id", "days_between_games"])
        df = pd.DataFrame(rows).sort_values("game_day").drop_duplicates("game_id")
        df["days_between_games"] = df["game_day"].diff().dt.days
        return df[["team_id", "game_id", "days_between_games"]]

    @staticmethod
    def _compute_rest_from_season_games(games_df):
        df = games_df.copy()
        df["game_day"] = pd.to_datetime(df["datetime"]).dt.normalize()
        appearances = (
            pd.concat([
                df[["game_id", "game_day", "away_name"]].rename(columns={"away_name": "team"}),
                df[["game_id", "game_day", "home_name"]].rename(columns={"home_name": "team"}),
            ], ignore_index=True)
            .sort_values(["team", "game_day", "game_id"])
            .drop_duplicates(["team", "game_id"])
        )
        appearances["off_days"] = appearances.groupby("team")["game_day"].diff().dt.days
        df = df.merge(appearances[["game_id", "team", "off_days"]].rename(columns={"team": "away_name", "off_days": "away_off_days"}), on=["game_id", "away_name"], how="left")
        df = df.merge(appearances[["game_id", "team", "off_days"]].rename(columns={"team": "home_name", "off_days": "home_off_days"}), on=["game_id", "home_name"], how="left")
        df["away_short_rest"] = np.where(df["away_off_days"] < 6, 1, 0)
        df["away_long_rest"] = np.where(df["away_off_days"] > 10, 1, 0)
        df["home_short_rest"] = np.where(df["home_off_days"] < 6, 1, 0)
        df["home_long_rest"] = np.where(df["home_off_days"] > 10, 1, 0)
        return df

    def pull_schedule(self, start_day, end_day, include_completed=False):
        days = pd.date_range(pd.to_datetime(start_day).normalize(), pd.to_datetime(end_day).normalize(), freq="D")
        all_games = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for f in as_completed([pool.submit(self._fetch_espn_scoreboard_day, d) for d in days]):
                all_games.extend(f.result())
        if not all_games:
            return pd.DataFrame(columns=["away_abbr", "away_name", "home_abbr", "home_name", "date", "neutral", "away_short_rest", "away_long_rest", "home_short_rest", "home_long_rest"])
        full = pd.DataFrame(all_games).drop_duplicates("game_id")
        full = full[full["away_id"] != full["home_id"]]
        if not include_completed:
            full = full[~full["completed"]]
        if full.empty:
            return full

        season = _cfb_season_for_date(start_day)
        team_ids = list(set(full["away_id"]).union(set(full["home_id"])))
        rest_frames = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for f in as_completed([pool.submit(self._fetch_espn_team_schedule_dates, tid, season) for tid in team_ids]):
                try:
                    rest_frames.append(f.result())
                except Exception:
                    continue
        if rest_frames:
            rest_df = pd.concat(rest_frames, ignore_index=True)
            full = full.merge(rest_df.rename(columns={"team_id": "away_id", "days_between_games": "away_off_days"}), on=["away_id", "game_id"], how="left")
            full = full.merge(rest_df.rename(columns={"team_id": "home_id", "days_between_games": "home_off_days"}), on=["home_id", "game_id"], how="left")
        else:
            full["away_off_days"] = np.nan
            full["home_off_days"] = np.nan

        full["away_short_rest"] = np.where(full["away_off_days"] < 6, 1, 0)
        full["away_long_rest"] = np.where(full["away_off_days"] > 10, 1, 0)
        full["home_short_rest"] = np.where(full["home_off_days"] < 6, 1, 0)
        full["home_long_rest"] = np.where(full["home_off_days"] > 10, 1, 0)
        full["away_name"] = self.normalize_team_name(full["away_name"])
        full["home_name"] = self.normalize_team_name(full["home_name"])
        return full[["away_abbr", "away_name", "home_abbr", "home_name", "date", "neutral", "away_short_rest", "away_long_rest", "home_short_rest", "home_long_rest"]].reset_index(drop=True)

    def pull_historical_schedule(self, year):
        days = pd.date_range(datetime(year, 8, 20), datetime(year + 1, 1, 25), freq="D")
        all_games = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for f in as_completed([pool.submit(self._fetch_espn_scoreboard_day, d) for d in days]):
                try:
                    all_games.extend(f.result())
                except Exception:
                    continue
        df = pd.DataFrame(all_games).drop_duplicates("game_id")
        df = df[df["completed"] & (df["away_id"] != df["home_id"])].copy()
        df["year"] = year
        df["away_name"] = self.normalize_team_name(df["away_name"])
        df["home_name"] = self.normalize_team_name(df["home_name"])
        df = self._compute_rest_from_season_games(df)
        return df[["away_abbr", "away_name", "away_score", "home_abbr", "home_name", "home_score", "date", "year", "neutral", "away_short_rest", "away_long_rest", "home_short_rest", "home_long_rest"]].reset_index(drop=True)

    def pull_current_odds_espn(self, start_date, end_date=None):
        end_date = end_date or start_date
        days = pd.date_range(pd.to_datetime(start_date).normalize(), pd.to_datetime(end_date).normalize(), freq="D")
        rows = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for f in as_completed([pool.submit(self._fetch_espn_scoreboard_day, d) for d in days]):
                for g in f.result():
                    if pd.notna(g["espn_spread"]):
                        rows.append({"away_name": g["away_name"], "home_name": g["home_name"], "spread": g["espn_spread"]})
        if not rows:
            return pd.DataFrame(columns=["away_name", "home_name", "spread"])
        df = pd.DataFrame(rows).drop_duplicates(["away_name", "home_name"])
        df["away_name"] = self.normalize_team_name(df["away_name"])
        df["home_name"] = self.normalize_team_name(df["home_name"])
        return df

    def pull_current_odds(self, start_date, end_date=None):
        return self.pull_current_odds_espn(start_date, end_date)

    def _fetch_html(self, url):
        req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read().decode("utf-8", errors="replace")

    def pull_bcftoys_stats(self, year=None):
        if year is None:
            year = _cfb_season_for_date(pd.Timestamp.today())
        with ThreadPoolExecutor(max_workers=2) as pool:
            fplus_html = pool.submit(self._fetch_html, f"https://www.bcftoys.com/{year}-fplus").result()
            fei_html = pool.submit(self._fetch_html, f"https://www.bcftoys.com/{year}-fei").result()

        fp_rows = [
            (tds[1], tds[4], tds[5], tds[7], tds[9])
            for tr in BeautifulSoup(fplus_html, "lxml").find_all("tr")
            for tds in [[td.get_text(strip=True) for td in tr.find_all("td")]]
            if len(tds) >= 10 and tds[0].isdigit()
        ]
        fei_rows = [
            (tds[1], tds[4])
            for tr in BeautifulSoup(fei_html, "lxml").find_all("tr")
            for tds in [[td.get_text(strip=True) for td in tr.find_all("td")]]
            if len(tds) >= 5 and tds[0].isdigit()
        ]
        df = pd.DataFrame(fp_rows, columns=["name", "f_plus", "of_plus", "df_plus", "sf_plus"]).merge(
            pd.DataFrame(fei_rows, columns=["name", "fei"]), on="name", how="left"
        )
        df["name"] = self.normalize_team_name(df["name"])
        num_cols = ["f_plus", "of_plus", "df_plus", "sf_plus", "fei"]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
        df.dropna(subset=num_cols, inplace=True)
        df[num_cols] = (df[num_cols] - df[num_cols].mean()) / df[num_cols].std(ddof=0)
        return df.reset_index(drop=True)

    pull_football_outsiders_stats = pull_bcftoys_stats

    def _fetch_team_rankings_table(self, path, col_name, year=None, is_pct=False, strip_record=False):
        url = f"https://www.teamrankings.com/college-football/{path}"
        if year is not None:
            url = f"{url}?date={year + 1}-02-01"
        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        table = BeautifulSoup(resp.text, "lxml").find("table", attrs={"class": "tr-table datatable scrollable"})
        rows = [
            (tds[1], tds[2])
            for tr in table.find_all("tr")
            for tds in [[td.get_text(strip=True) for td in tr.find_all("td") if td.get_text(strip=True)]]
            if len(tds) >= 3 and tds[2] != "--"
        ]
        df = pd.DataFrame(rows, columns=["name", col_name])
        if strip_record:
            df["name"] = df["name"].str.replace(r"\s*\(\d+-\d+\)$", "", regex=True).str.strip()
        vals = df[col_name].str.rstrip("%").astype(float)
        df[col_name] = vals / 100.0 if is_pct else vals
        return df

    def pull_team_rankings_data(self, year=None):
        specs = [
            ("ranking/predictive-by-other", "team_rankings_rating", False, True),
            ("ranking/schedule-strength-by-other", "strength_of_schedule", False, True),
            ("stat/points-per-play", "points_per_play", False, False),
            ("stat/opponent-points-per-play", "opponent_points_per_play", False, False),
            ("stat/yards-per-play", "yards_per_play", False, False),
            ("stat/opponent-yards-per-play", "opponent_yards_per_play", False, False),
            ("stat/yards-per-rush-attempt", "yards_per_rush", False, False),
            ("stat/opponent-yards-per-rush-attempt", "opponent_yards_per_rush", False, False),
            ("stat/yards-per-pass-attempt", "yards_per_pass", False, False),
            ("stat/opponent-yards-per-pass-attempt", "opponent_yards_per_pass", False, False),
            ("stat/qb-sacked-pct", "qb_sacked_percentage", True, False),
            ("stat/sack-pct", "sack_percentage", True, False),
            ("stat/giveaways-per-game", "giveaways_per_game", False, False),
            ("stat/takeaways-per-game", "takeaways_per_game", False, False),
        ]
        results = {}
        with ThreadPoolExecutor(max_workers=len(specs)) as pool:
            fmap = {pool.submit(self._fetch_team_rankings_table, p, c, year, pct, sr): c for p, c, pct, sr in specs}
            for f in as_completed(fmap):
                results[fmap[f]] = f.result()
        ordered = [s[1] for s in specs]
        df = results[ordered[0]]
        for col in ordered[1:]:
            df = df.merge(results[col], on="name", how="left")
        df["name"] = self.normalize_team_name(df["name"])
        df[ordered] = df[ordered].apply(pd.to_numeric, errors="coerce")
        df.dropna(subset=ordered, inplace=True)
        df[ordered] = (df[ordered] - df[ordered].mean()) / df[ordered].std(ddof=0)
        return df.reset_index(drop=True)

    def create_opponent_adjusted_stats(self, data, index_start):
        data = data.copy()
        data.iloc[:, index_start:] = data.iloc[:, index_start:].apply(pd.to_numeric)
        u = data.iloc[:, :index_start].copy()
        u["f_plus"] = data["home_f_plus"] - data["away_f_plus"]
        u["fei"] = data["home_fei"] - data["away_fei"]
        u["sf_plus"] = data["home_sf_plus"] - data["away_sf_plus"]
        u["team_rankings_rating"] = data["home_team_rankings_rating"] - data["away_team_rankings_rating"]
        u["strength_of_schedule"] = data["home_strength_of_schedule"] - data["away_strength_of_schedule"]
        u["home_of_vs_away_df"] = data["home_of_plus"] - data["away_df_plus"]
        u["away_of_vs_home_df"] = -(data["away_of_plus"] - data["home_df_plus"])
        u["away_points_per_play"] = -data["away_points_per_play"] - data["home_opponent_points_per_play"]
        u["away_yards_per_play"] = -data["away_yards_per_play"] - data["home_opponent_yards_per_play"]
        u["away_yards_per_rush"] = -data["away_yards_per_rush"] - data["home_opponent_yards_per_rush"]
        u["away_yards_per_pass"] = -data["away_yards_per_pass"] - data["home_opponent_yards_per_pass"]
        u["home_points_per_play"] = data["home_points_per_play"] + data["away_opponent_points_per_play"]
        u["home_yards_per_play"] = data["home_yards_per_play"] + data["away_opponent_yards_per_play"]
        u["home_yards_per_rush"] = data["home_yards_per_rush"] + data["away_opponent_yards_per_rush"]
        u["home_yards_per_pass"] = data["home_yards_per_pass"] + data["away_opponent_yards_per_pass"]
        u["away_sacked_percentage"] = data["away_qb_sacked_percentage"] + data["home_sack_percentage"]
        u["home_sacked_percentage"] = -data["home_qb_sacked_percentage"] - data["away_sack_percentage"]
        u["away_turnovers"] = data["away_giveaways_per_game"] + data["home_takeaways_per_game"]
        u["home_turnovers"] = -data["home_giveaways_per_game"] - data["away_takeaways_per_game"]
        return u

    def _merge_team_features(self, games_df, year=None):
        bcf = self.pull_bcftoys_stats(year)
        tr = self.pull_team_rankings_data(year)
        return (
            games_df.merge(bcf.add_prefix("away_"), on="away_name", how="left")
            .merge(bcf.add_prefix("home_"), on="home_name", how="left")
            .merge(tr.add_prefix("away_"), on="away_name", how="left")
            .merge(tr.add_prefix("home_"), on="home_name", how="left")
        )

    def create_dataframe(self, start_day, end_day):
        data = self.pull_schedule(start_day, end_day)
        if data.empty:
            return data
        data["neutral"] = np.where(data["neutral"] == 1, 1, 0)
        data = self._merge_team_features(data, year=None)
        data.dropna(inplace=True)
        data = self.create_opponent_adjusted_stats(data, 10)
        odds = self.pull_current_odds(start_day, end_day)
        if not odds.empty:
            rev = pd.DataFrame({"home_name": odds["away_name"], "away_name": odds["home_name"], "spread": -odds["spread"]})
            odds = pd.concat([odds, rev], ignore_index=True).drop_duplicates(["away_name", "home_name"])
            data = data.merge(odds, on=["away_name", "home_name"], how="left")
        else:
            data["spread"] = np.nan
        data.drop(columns=["away_abbr", "home_abbr"], inplace=True)
        return data

    def create_historical_data(self, year):
        df = self.pull_historical_schedule(year)
        df["neutral"] = np.where(df["neutral"] == 1, 1, 0)
        df = self._merge_team_features(df, year=year)
        df.dropna(inplace=True)
        df = self.create_opponent_adjusted_stats(df, 13)
        df["result"] = df["home_score"] - df["away_score"]
        df.dropna(subset=["result"], inplace=True)
        return df

    def create_historical_dataframe(self, first_year=2014, last_year=2025):
        full_data = pd.concat([self.create_historical_data(y) for y in range(first_year, last_year + 1)], ignore_index=True)
        full_data.to_csv("historical_cfb_data_full.csv", index=False)
        model_df = full_data.drop(columns=["away_abbr", "home_abbr", "away_name", "home_name", "home_score", "away_score", "date"])
        model_df.to_csv("historical_cfb_data.csv", index=False)
        return model_df
