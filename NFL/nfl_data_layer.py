import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

ESPN_NFL_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
NFLVERSE_GAMES_URL = (
    "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
)
NFLVERSE_TEAM_WEEK_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/stats_team/"
    "stats_team_week_{year}.csv"
)


def _nfl_season_for_date(dt):
    dt = pd.to_datetime(dt)
    return dt.year - 1 if dt.month <= 2 else dt.year


def _format_short_date(dt):
    dt = pd.to_datetime(dt)
    return f"{dt.strftime('%b')} {dt.day}"


# Canonical NFL team names (matching TeamRankings / historical conventions)
ABBR_TO_CANONICAL = {
    "ARI": "Arizona",
    "ATL": "Atlanta",
    "BAL": "Baltimore",
    "BUF": "Buffalo",
    "CAR": "Carolina",
    "CHI": "Chicago",
    "CIN": "Cincinnati",
    "CLE": "Cleveland",
    "DAL": "Dallas",
    "DEN": "Denver",
    "DET": "Detroit",
    "GB": "Green Bay",
    "HOU": "Houston",
    "IND": "Indianapolis",
    "JAX": "Jacksonville",
    "JAC": "Jacksonville",
    "KC": "Kansas City",
    "LA": "LA Rams",
    "LAR": "LA Rams",
    "STL": "LA Rams",
    "LAC": "LA Chargers",
    "SD": "LA Chargers",
    "LV": "Las Vegas",
    "OAK": "Las Vegas",
    "MIA": "Miami",
    "MIN": "Minnesota",
    "NE": "New England",
    "NO": "New Orleans",
    "NYG": "NY Giants",
    "NYJ": "NY Jets",
    "PHI": "Philadelphia",
    "PIT": "Pittsburgh",
    "SEA": "Seattle",
    "SF": "San Francisco",
    "TB": "Tampa Bay",
    "TEN": "Tennessee",
    "WAS": "Washington",
    "WSH": "Washington",
}


class NflDataLayer:
    """
    Data layer for NFL modeling.

    Replaces sportsipy, defunct Football Outsiders DVOA, and Sportsbook Review with:
      - Schedules, neutral/international flags, exact rest days (<6 short, >10 bye),
        and historical closing spreads: nflverse games.csv (1999-present, zero API key)
      - Opponent-adjusted efficiency (Net EPA/play, Off/Def Pass & Rush EPA/play):
          nflverse stats_team_week_{year}.csv (replaces paywalled DVOA, zero API key)
      - Live upcoming games & DraftKings spreads: ESPN NFL Public JSON API (zero API key)
      - TeamRankings NFL (2 /ranking/ pages): concurrent ThreadPoolExecutor fetching
    """

    def __init__(self, max_workers=10):
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self._games_cache = None

        self.team_replace = {
            "Los Angeles Rams": "LA Rams",
            "Los Angeles Chargers": "LA Chargers",
            "L.A. Rams": "LA Rams",
            "L.A. Chargers": "LA Chargers",
            "St. Louis": "LA Rams",
            "San Diego": "LA Chargers",
            "Oakland": "Las Vegas",
            "New York Giants": "NY Giants",
            "New York Jets": "NY Jets",
            "N.Y. Giants": "NY Giants",
            "N.Y. Jets": "NY Jets",
            "LA Rams": "LA Rams",
            "LA Chargers": "LA Chargers",
            "NY Giants": "NY Giants",
            "NY Jets": "NY Jets",
            "Washington Football Team": "Washington",
            "Washington Commanders": "Washington",
            "Washington Redskins": "Washington",
        }

    def normalize_team_name(self, series):
        s = series.astype(str).str.strip()
        s = s.replace(ABBR_TO_CANONICAL)
        return s.replace(self.team_replace)

    def _load_nflverse_games(self):
        if self._games_cache is None:
            self._games_cache = pd.read_csv(NFLVERSE_GAMES_URL)
        return self._games_cache.copy()

    def _fetch_espn_scoreboard_day(self, day_dt):
        date_str = pd.to_datetime(day_dt).strftime("%Y%m%d")
        resp = self.session.get(
            f"{ESPN_NFL_BASE_URL}/scoreboard",
            params={"dates": date_str},
            timeout=15,
        )
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
                    # ESPN spread is negative when home is favored; negate so + = home favored
                    spread = -float(odds_list[0]["spread"])
                except (TypeError, ValueError):
                    spread = np.nan

            games.append(
                {
                    "game_id": str(ev["id"]),
                    "datetime": game_dt.tz_localize(None),
                    "date": _format_short_date(game_dt),
                    "completed": completed,
                    "neutral": int(bool(comp.get("neutralSite", False))),
                    "away_abbr": away["team"].get("abbreviation", "").upper(),
                    "away_name": away["team"].get("abbreviation", "").upper(),
                    "home_abbr": home["team"].get("abbreviation", "").upper(),
                    "home_name": home["team"].get("abbreviation", "").upper(),
                    "espn_spread": spread,
                }
            )
        return games

    def pull_schedule(self, start_day, end_day, include_completed=False):
        """Pull upcoming NFL games for [start_day, end_day] with rest days & live spreads."""
        start_dt = pd.to_datetime(start_day).normalize()
        end_dt = pd.to_datetime(end_day).normalize()
        days = pd.date_range(start_dt, end_dt, freq="D")

        all_games = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            for f in as_completed([pool.submit(self._fetch_espn_scoreboard_day, d) for d in days]):
                all_games.extend(f.result())

        if not all_games:
            return pd.DataFrame(
                columns=[
                    "away_name",
                    "home_name",
                    "date",
                    "neutral",
                    "away_short_rest",
                    "away_long_rest",
                    "home_short_rest",
                    "home_long_rest",
                    "spread",
                ]
            )

        df = pd.DataFrame(all_games).drop_duplicates("game_id")
        if not include_completed:
            df = df[~df["completed"]].copy()
        if df.empty:
            return df

        df["away_name"] = self.normalize_team_name(df["away_name"])
        df["home_name"] = self.normalize_team_name(df["home_name"])

        season = _nfl_season_for_date(start_dt)
        nv = self._load_nflverse_games()
        nv = nv[nv["season"] == season].copy()
        nv["away_name"] = self.normalize_team_name(nv["away_team"])
        nv["home_name"] = self.normalize_team_name(nv["home_team"])
        nv["date"] = pd.to_datetime(nv["gameday"]).apply(_format_short_date)

        df = df.merge(
            nv[["date", "away_name", "home_name", "away_rest", "home_rest", "spread_line"]].drop_duplicates(
                ["date", "away_name", "home_name"]
            ),
            on=["date", "away_name", "home_name"],
            how="left",
        )

        df["away_short_rest"] = np.where(df["away_rest"] < 6, 1, 0)
        df["away_long_rest"] = np.where(df["away_rest"] > 10, 1, 0)
        df["home_short_rest"] = np.where(df["home_rest"] < 6, 1, 0)
        df["home_long_rest"] = np.where(df["home_rest"] > 10, 1, 0)
        df["spread"] = df["espn_spread"].combine_first(df["spread_line"])

        return df[
            [
                "away_name",
                "home_name",
                "date",
                "neutral",
                "away_short_rest",
                "away_long_rest",
                "home_short_rest",
                "home_long_rest",
                "spread",
            ]
        ].reset_index(drop=True)

    def pull_historical_schedule(self, year):
        """Pull all completed games, rest days, neutral flags, and Vegas closing spreads for a season."""
        nv = self._load_nflverse_games()
        df = nv[(nv["season"] == year) & nv["home_score"].notna() & nv["away_score"].notna()].copy()

        df["away_name"] = self.normalize_team_name(df["away_team"])
        df["home_name"] = self.normalize_team_name(df["home_team"])
        df["date"] = pd.to_datetime(df["gameday"]).apply(_format_short_date)
        df["year"] = year
        df["neutral"] = np.where(df["location"] == "Neutral", 1, 0)
        df["away_short_rest"] = np.where(df["away_rest"] < 6, 1, 0)
        df["away_long_rest"] = np.where(df["away_rest"] > 10, 1, 0)
        df["home_short_rest"] = np.where(df["home_rest"] < 6, 1, 0)
        df["home_long_rest"] = np.where(df["home_rest"] > 10, 1, 0)
        df["close"] = pd.to_numeric(df["spread_line"], errors="coerce")

        return df[
            [
                "away_name",
                "away_score",
                "home_name",
                "home_score",
                "date",
                "year",
                "neutral",
                "away_short_rest",
                "away_long_rest",
                "home_short_rest",
                "home_long_rest",
                "close",
            ]
        ].reset_index(drop=True)

    def pull_nflverse_epa_stats(self, year=None):
        """
        Compute season-level EPA/play (net, off/def, pass/rush) AND per-play
        efficiency (points/play, yards/play, yards/rush, net yards/pass + opponent
        counterparts) directly from nflverse CSVs (free, zero API key, zero WAF).
        """
        if year is None:
            year = _nfl_season_for_date(pd.Timestamp.today())

        url = NFLVERSE_TEAM_WEEK_URL.format(year=year)
        raw_wk = pd.read_csv(url)

        pass_plays = raw_wk["attempts"].fillna(0) + raw_wk["sacks_suffered"].fillna(0)
        rush_plays = raw_wk["carries"].fillna(0)
        net_pass_yds = raw_wk["passing_yards"].fillna(0) - raw_wk["sack_yards_lost"].fillna(0).abs()
        rush_yds = raw_wk["rushing_yards"].fillna(0)

        wk = pd.DataFrame(
            {
                "team": raw_wk["team"],
                "opponent_team": raw_wk["opponent_team"],
                "passing_epa": raw_wk["passing_epa"].fillna(0),
                "rushing_epa": raw_wk["rushing_epa"].fillna(0),
                "total_epa": raw_wk["passing_epa"].fillna(0) + raw_wk["rushing_epa"].fillna(0),
                "pass_plays": pass_plays,
                "rush_plays": rush_plays,
                "total_plays": pass_plays + rush_plays,
                "net_pass_yds": net_pass_yds,
                "rush_yds": rush_yds,
                "total_yds": net_pass_yds + rush_yds,
            }
        )

        nv = self._load_nflverse_games()
        nv_yr = nv[(nv["season"] == year) & nv["home_score"].notna() & nv["away_score"].notna()]
        pts_home = nv_yr[["home_team", "home_score", "away_score"]].rename(
            columns={"home_team": "team", "home_score": "pts_for", "away_score": "pts_against"}
        )
        pts_away = nv_yr[["away_team", "away_score", "home_score"]].rename(
            columns={"away_team": "team", "away_score": "pts_for", "home_score": "pts_against"}
        )
        pts = pd.concat([pts_home, pts_away], ignore_index=True).groupby("team", as_index=False).sum()

        agg_cols = [
            "passing_epa", "rushing_epa", "total_epa",
            "pass_plays", "rush_plays", "total_plays",
            "net_pass_yds", "rush_yds", "total_yds",
        ]
        off = wk.groupby("team", as_index=False)[agg_cols].sum().merge(pts, on="team", how="left")
        off["off_epa_play"] = off["total_epa"] / off["total_plays"].clip(lower=1)
        off["off_pass_epa"] = off["passing_epa"] / off["pass_plays"].clip(lower=1)
        off["off_rush_epa"] = off["rushing_epa"] / off["rush_plays"].clip(lower=1)
        off["points_per_play"] = off["pts_for"].fillna(0) / off["total_plays"].clip(lower=1)
        off["yards_per_play"] = off["total_yds"] / off["total_plays"].clip(lower=1)
        off["yards_per_rush"] = off["rush_yds"] / off["rush_plays"].clip(lower=1)
        off["yards_per_pass"] = off["net_pass_yds"] / off["pass_plays"].clip(lower=1)

        deff = (
            wk.groupby("opponent_team", as_index=False)[agg_cols]
            .sum()
            .rename(columns={"opponent_team": "team"})
            .merge(pts[["team", "pts_against"]], on="team", how="left")
        )
        deff["def_epa_play"] = deff["total_epa"] / deff["total_plays"].clip(lower=1)
        deff["def_pass_epa"] = deff["passing_epa"] / deff["pass_plays"].clip(lower=1)
        deff["def_rush_epa"] = deff["rushing_epa"] / deff["rush_plays"].clip(lower=1)
        deff["opponent_points_per_play"] = deff["pts_against"].fillna(0) / deff["total_plays"].clip(lower=1)
        deff["opponent_yards_per_play"] = deff["total_yds"] / deff["total_plays"].clip(lower=1)
        deff["opponent_yards_per_rush"] = deff["rush_yds"] / deff["rush_plays"].clip(lower=1)
        deff["opponent_yards_per_pass"] = deff["net_pass_yds"] / deff["pass_plays"].clip(lower=1)

        df = off[
            [
                "team", "off_epa_play", "off_pass_epa", "off_rush_epa",
                "points_per_play", "yards_per_play", "yards_per_rush", "yards_per_pass",
            ]
        ].merge(
            deff[
                [
                    "team", "def_epa_play", "def_pass_epa", "def_rush_epa",
                    "opponent_points_per_play", "opponent_yards_per_play",
                    "opponent_yards_per_rush", "opponent_yards_per_pass",
                ]
            ],
            on="team",
            how="inner",
        )
        df["net_epa_play"] = df["off_epa_play"] - df["def_epa_play"]
        df.rename(columns={"team": "name"}, inplace=True)
        df["name"] = self.normalize_team_name(df["name"])

        num_cols = [c for c in df.columns if c != "name"]
        df[num_cols] = (df[num_cols] - df[num_cols].mean()) / df[num_cols].std(ddof=0)
        return df[["name"] + num_cols].reset_index(drop=True)

    def _fetch_team_rankings_table(self, path, col_name, year=None, strip_record=False):
        url = f"https://www.teamrankings.com/nfl/{path}"
        if year is not None:
            url = f"{url}?date={year + 1}-02-01"
        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        table = BeautifulSoup(resp.text, "lxml").find(
            "table", attrs={"class": "tr-table datatable scrollable"}
        )
        if table is None:
            raise RuntimeError(f"Could not find TeamRankings NFL table at {url}")
        rows = [
            (tds[1], tds[2])
            for tr in table.find_all("tr")
            for tds in [[td.get_text(strip=True) for td in tr.find_all("td") if td.get_text(strip=True)]]
            if len(tds) >= 3 and tds[2] != "--"
        ]
        df = pd.DataFrame(rows, columns=["name", col_name])
        if strip_record:
            df["name"] = df["name"].str.replace(r"\s*\(\d+-\d+(?:-\d+)?\)$", "", regex=True).str.strip()
        df[col_name] = df[col_name].str.rstrip("%").astype(float)
        return df

    def pull_team_rankings_data(self, year=None):
        specs = [
            ("ranking/predictive-by-other", "team_rankings_rating", True),
            ("ranking/schedule-strength-by-other", "strength_of_schedule", True),
        ]
        results = {}
        with ThreadPoolExecutor(max_workers=len(specs)) as pool:
            fmap = {
                pool.submit(self._fetch_team_rankings_table, p, c, year, sr): c
                for p, c, sr in specs
            }
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

    def create_opponent_adjusted_stats(self, data, meta_cols):
        u = data[meta_cols].copy()

        u["net_epa"] = data["home_net_epa_play"] - data["away_net_epa_play"]
        u["home_off_epa_matchup"] = data["home_off_epa_play"] + data["away_def_epa_play"]
        u["away_off_epa_matchup"] = -data["away_off_epa_play"] - data["home_def_epa_play"]
        u["home_pass_epa_matchup"] = data["home_off_pass_epa"] + data["away_def_pass_epa"]
        u["away_pass_epa_matchup"] = -data["away_off_pass_epa"] - data["home_def_pass_epa"]
        u["home_rush_epa_matchup"] = data["home_off_rush_epa"] + data["away_def_rush_epa"]
        u["away_rush_epa_matchup"] = -data["away_off_rush_epa"] - data["home_def_rush_epa"]

        u["team_rankings_rating"] = (
            data["home_team_rankings_rating"] - data["away_team_rankings_rating"]
        )
        u["strength_of_schedule"] = (
            data["home_strength_of_schedule"] - data["away_strength_of_schedule"]
        )
        u["away_points_per_play"] = (
            -data["away_points_per_play"] - data["home_opponent_points_per_play"]
        )
        u["away_yards_per_play"] = (
            -data["away_yards_per_play"] - data["home_opponent_yards_per_play"]
        )
        u["away_yards_per_rush"] = (
            -data["away_yards_per_rush"] - data["home_opponent_yards_per_rush"]
        )
        u["away_yards_per_pass"] = (
            -data["away_yards_per_pass"] - data["home_opponent_yards_per_pass"]
        )
        u["home_points_per_play"] = (
            data["home_points_per_play"] + data["away_opponent_points_per_play"]
        )
        u["home_yards_per_play"] = (
            data["home_yards_per_play"] + data["away_opponent_yards_per_play"]
        )
        u["home_yards_per_rush"] = (
            data["home_yards_per_rush"] + data["away_opponent_yards_per_rush"]
        )
        u["home_yards_per_pass"] = (
            data["home_yards_per_pass"] + data["away_opponent_yards_per_pass"]
        )
        return u

    def _merge_team_features(self, games_df, year=None):
        epa = self.pull_nflverse_epa_stats(year)
        tr = self.pull_team_rankings_data(year)
        return (
            games_df.merge(epa.add_prefix("away_"), on="away_name", how="left")
            .merge(epa.add_prefix("home_"), on="home_name", how="left")
            .merge(tr.add_prefix("away_"), on="away_name", how="left")
            .merge(tr.add_prefix("home_"), on="home_name", how="left")
        )

    def create_dataframe(self, start_day, end_day):
        data = self.pull_schedule(start_day, end_day)
        if data.empty:
            return data

        meta_cols = [
            "away_name",
            "home_name",
            "date",
            "neutral",
            "away_short_rest",
            "away_long_rest",
            "home_short_rest",
            "home_long_rest",
            "spread",
        ]
        merged = self._merge_team_features(data, year=None)
        feature_cols = [c for c in merged.columns if c not in meta_cols]
        merged.dropna(subset=feature_cols, inplace=True)
        return self.create_opponent_adjusted_stats(merged, meta_cols).reset_index(drop=True)

    def create_historical_data(self, year):
        df = self.pull_historical_schedule(year)
        meta_cols = [
            "away_name",
            "away_score",
            "home_name",
            "home_score",
            "date",
            "year",
            "neutral",
            "away_short_rest",
            "away_long_rest",
            "home_short_rest",
            "home_long_rest",
            "close",
        ]
        merged = self._merge_team_features(df, year=year)
        feature_cols = [c for c in merged.columns if c not in meta_cols]
        merged.dropna(subset=feature_cols, inplace=True)
        out = self.create_opponent_adjusted_stats(merged, meta_cols)
        out["result"] = out["home_score"] - out["away_score"]
        return out.dropna(subset=["result"]).reset_index(drop=True)

    def create_historical_dataframe(self, first_year=2010, last_year=2025):
        full_data = pd.concat(
            [self.create_historical_data(y) for y in range(first_year, last_year + 1)],
            ignore_index=True,
        )
        full_data.to_csv("historical_nfl_data_full.csv", index=False)
        model_df = full_data.drop(
            columns=["away_name", "home_name", "home_score", "away_score", "date"]
        )
        model_df.to_csv("historical_nfl_data.csv", index=False)
        return model_df
