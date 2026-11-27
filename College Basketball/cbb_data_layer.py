import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from sklearn.preprocessing import StandardScaler


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

ESPN_BASE_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball"
)
CBBD_BASE_URL = "https://api.collegebasketballdata.com"
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"


def _cbb_season_for_date(dt):
    """Return the NCAA season year (ending year) for a given date.
    e.g., Nov 2026 -> 2027 season; Feb 2026 -> 2026 season.
    """
    dt = pd.to_datetime(dt)
    return dt.year + 1 if dt.month >= 7 else dt.year


def _format_short_date(dt):
    """Cross-platform 'Nov 4' style date string (avoids Windows-only '%#d')."""
    dt = pd.to_datetime(dt)
    return f"{dt.strftime('%b')} {dt.day}"


class CbbDataLayer:
    """
    Data layer for College Basketball modeling.

    Replaces the deprecated/throttled sportsipy (Sports Reference) and Selenium
    scrapers with direct JSON APIs and concurrent HTTP fetching:
      - Schedules, teams, neutral-site flags, and days of rest:
          ESPN Public JSON API (no API key required) or CollegeBasketballData (CBBD) API
      - Current betting spreads:
          The Odds API (optional key), CBBD /lines (optional free key),
          or ESPN Scoreboard API (no API key required)
      - Historical betting spreads:
          CBBD /lines (free API key) or The Odds API historical endpoint (paid tier)
      - KenPom efficiency & tempo:
          Direct HTTP session (no Selenium) or KenPom Official API (subscriber key)
      - TeamRankings stats:
          Concurrent ThreadPoolExecutor fetching via a shared requests.Session
    """

    def __init__(
        self,
        cbbd_api_key=None,
        odds_api_key=None,
        kenpom_api_key=None,
        max_workers=10,
    ):
        self.cbbd_api_key = cbbd_api_key or os.getenv("CBBD_API_KEY")
        self.odds_api_key = odds_api_key or os.getenv("THE_ODDS_API_KEY")
        self.kenpom_api_key = kenpom_api_key or os.getenv("KENPOM_API_KEY")
        self.max_workers = max_workers

        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

        self.team_replace = {
            "Missouri KC": "UMKC",
            "CS Fullerton": "Cal State Fullerton",
            "Gardner Webb": "Gardner-Webb",
            "SC Upstate": "USC Upstate",
            "TX Southern": "Texas Southern",
            "Loyola MD": "Loyola (MD)",
            "Loyola Maryland": "Loyola (MD)",
            "SIUE": "SIU-Edwardsville",
            "F Dickinson": "Fairleigh Dickinson",
            "Cent Arkansas": "Central Arkansas",
            "American Univ": "American",
            "Ark Pine Bluff": "Arkansas-Pine Bluff",
            "Bowling Green": "Bowling Green State",
            "UC Irvine": "UC-Irvine",
            "UC Riverside": "UC-Riverside",
            "UC San Diego": "UC-San Diego",
            "Central Conn": "Central Connecticut",
            "Charleston So": "Charleston Southern",
            "E Kentucky": "Eastern Kentucky",
            "FL Atlantic": "Florida Atlantic",
            "Ga Southern": "Georgia Southern",
            "North Carolina": "UNC",
            "Miami FL": "Miami (FL)",
            "Miami": "Miami (FL)",
            "Monmouth NJ": "Monmouth",
            "S Dakota State": "South Dakota State",
            "SE Louisiana": "Southeastern Louisiana",
            "Connecticut": "UConn",
            "N Kentucky": "Northern Kentucky",
            "Florida Intl": "Florida International",
            "NC A&T": "North Carolina A&T",
            "Abilene Chr": "Abilene Christian",
            "G Washington": "George Washington",
            "SUNY Albany": "Albany (NY)",
            "UAlbany": "Albany (NY)",
            "CS Northridge": "Cal State Northridge",
            "Mississippi": "Ole Miss",
            "Massachusetts": "UMass",
            "SE Missouri State": "Southeast Missouri State",
            "WKU": "Western Kentucky",
            "St John's": "St. John's (NY)",
            "St Bonaventure": "St. Bonaventure",
            "UTRGV": "Texas-Rio Grande Valley",
            "IL Chicago": "UIC",
            "Boston Univ": "Boston University",
            "TAM C. Christi": "Texas A&M-Corpus Christi",
            "Coastal Car": "Coastal Carolina",
            "St Louis": "Saint Louis",
            "Col Charleston": "College of Charleston",
            "C Michigan": "Central Michigan",
            "E Michigan": "Eastern Michigan",
            "MTSU": "Middle Tennessee",
            "MD E Shore": "Maryland-Eastern Shore",
            "E Washington": "Eastern Washington",
            "CS Bakersfield": "Cal State Bakersfield",
            "WI Milwaukee": "Milwaukee",
            "Loyola-Chicago": "Loyola (IL)",
            "St Mary's CA": "Saint Mary's",
            "Saint Mary's (CA)": "Saint Mary's",
            "FL Gulf Coast": "Florida Gulf Coast",
            "UC Santa Barbara": "UCSB",
            "Ark Little Rock": "Little Rock",
            "W Illinois": "Western Illinois",
            "W Michigan": "Western Michigan",
            "Kent": "Kent State",
            "PFW": "Purdue-Fort Wayne",
            "UT San Antonio": "UTSA",
            "Northwestern LA": "Northwestern State",
            "S Dakota St": "South Dakota State",
            "MA Lowell": "UMass-Lowell",
            "Pittsburgh": "Pitt",
            "Houston Bap": "Houston Baptist",
            "N Illinois": "Northern Illinois",
            "CS Sacramento": "Sacramento State",
            "Miami OH": "Miami (OH)",
            "Loy Marymount": "Loyola Marymount",
            "W Carolina": "Western Carolina",
            "LIU Brooklyn": "LIU",
            "Long Island University": "LIU",
            "TN Martin": "UT-Martin",
            "UT Martin": "UT-Martin",
            "SF Austin": "Stephen F. Austin",
            "Kennesaw": "Kennesaw State",
            "ULM": "Louisiana-Monroe",
            "WI Green Bay": "Green Bay",
            "NC Central": "North Carolina Central",
            "St Francis NY": "St. Francis (NY)",
            "St Francis PA": "Saint Francis (PA)",
            "St Peter's": "St. Peter's",
            "S Carolina State": "South Carolina State",
            "N Colorado": "Northern Colorado",
            "Mt State Mary's": "Mount St. Mary's",
            "N Dakota State": "North Dakota State",
            "Southern Univ": "Southern",
            "St Joseph's PA": "St. Joseph's",
            "NE Omaha": "Omaha",
            "MS Valley State": "Mississippi Valley State",
            "UC Davis": "UC-Davis",
            "S Illinois": "Southern Illinois",
            "E Illinois": "Eastern Illinois",
            "FIU": "Florida International",
            "Albany": "Albany (NY)",
            "Bethune Cookman": "Bethune-Cookman",
            "St. John's": "St. John's (NY)",
            "UT Rio Grande Valley": "Texas-Rio Grande Valley",
            "Illinois Chicago": "UIC",
            "Citadel": "The Citadel",
            "Texas A&M Corpus Chris": "Texas A&M-Corpus Christi",
            "Texas A&M-Corpus Christi": "Texas A&M-Corpus Christi",
            "Maryland Eastern Shore": "Maryland-Eastern Shore",
            "N.C. State": "NC State",
            "Loyola Chicago": "Loyola (IL)",
            "Fort Wayne": "Purdue-Fort Wayne",
            "UMass Lowell": "UMass-Lowell",
            "Arkansas Pine Bluff": "Arkansas-Pine Bluff",
            "Louisiana Lafayette": "Louisiana",
            "Tennessee Martin": "UT-Martin",
            "SIU Edwardsville": "SIU-Edwardsville",
            "Louisiana Monroe": "Louisiana-Monroe",
            "Mount State Mary's": "Mount St. Mary's",
            "Saint Joseph's": "St. Joseph's",
            "St. Francis PA": "Saint Francis (PA)",
            "St. Francis NY": "St. Francis (NY)",
            "Saint Peter's": "St. Peter's",
            "East Tennessee State": "ETSU",
            "Prairie View A&M": "Prairie View",
            "Nebraska Omaha": "Omaha",
            "Grambling State": "Grambling",
            "N Carolina": "UNC",
            "Abl Christian": "Abilene Christian",
            "W Virginia": "West Virginia",
            "Miss State": "Mississippi State",
            "James Mad": "James Madison",
            "VA Tech": "Virginia Tech",
            "N Mex State": "New Mexico State",
            "Grd Canyon": "Grand Canyon",
            "S Florida": "South Florida",
            "Sacred Hrt": "Sacred Heart",
            "Geo Wshgtn": "George Washington",
            "Beth-Cook": "Bethune-Cookman",
            "Geo Mason": "George Mason",
            "Cal State Nrdge": "Cal State Northridge",
            "San Fransco": "San Francisco",
            "TX El Paso": "UTEP",
            "TX Christian": "TCU",
            "Boston Col": "Boston College",
            "S Methodist": "SMU",
            "U Mass": "UMass",
            "Central FL": "UCF",
            "TN Tech": "Tennessee Tech",
            "SE Missouri": "Southeast Missouri State",
            "W Kentucky": "Western Kentucky",
            "St Johns": "St. John's (NY)",
            "St Bonavent": "St. Bonaventure",
            "TX-Pan Am": "Texas-Rio Grande Valley",
            "IL-Chicago": "UIC",
            "Boston U": "Boston University",
            "TX A&M-CC": "Texas A&M-Corpus Christi",
            "Col Charlestn": "College of Charleston",
            "Central Mich": "Central Michigan",
            "S Mississippi": "Southern Miss",
            "Incar Word": "Incarnate Word",
            "Jksnville State": "Jacksonville State",
            "LA Tech": "Louisiana Tech",
            "Middle Tenn": "Middle Tennessee",
            "Maryland ES": "Maryland-Eastern Shore",
            "E Washingtn": "Eastern Washington",
            "N Hampshire": "New Hampshire",
            "CS Bakersfld": "Cal State Bakersfield",
            "WI-Milwkee": "Milwaukee",
            "Loyola-Chi": "Loyola (IL)",
            "St Marys": "Saint Mary's",
            "Fla Gulf Cst": "Florida Gulf Coast",
            "NC-Wilmgton": "UNC Wilmington",
            "AR Lit Rock": "Little Rock",
            "Sam Hous State": "Sam Houston State",
            "Sam Houston": "Sam Houston State",
            "App State": "Appalachian State",
            "E Carolina": "East Carolina",
            "IPFW": "Purdue-Fort Wayne",
            "TX-San Ant": "UTSA",
            "NW State": "Northwestern State",
            "Mass Lowell": "UMass-Lowell",
            "Wash State": "Washington State",
            "Ark Pine Bl": "Arkansas-Pine Bluff",
            "Central Ark": "Central Arkansas",
            "Northeastrn": "Northeastern",
            "N Iowa": "Northern Iowa",
            "GA Southern": "Georgia Southern",
            "N Arizona": "Northern Arizona",
            "U Penn": "Penn",
            "Pennsylvania": "Penn",
            "NC-Asheville": "UNC Asheville",
            "S Alabama": "South Alabama",
            "TN State": "Tennessee State",
            "Bowling Grn": "Bowling Green State",
            "S Carolina": "South Carolina",
            "Youngs State": "Youngstown State",
            "LA Lafayette": "Louisiana",
            "Sac State": "Sacramento State",
            "Lg Beach State": "Long Beach State",
            "Loyola Mymt": "Loyola Marymount",
            "Charl South": "Charleston Southern",
            "Wm & Mary": "William & Mary",
            "LIU-Brooklyn": "LIU",
            "NC-Grnsboro": "UNC Greensboro",
            "Ste F Austin": "Stephen F. Austin",
            "SIU Edward": "SIU-Edwardsville",
            "Maryland BC": "UMBC",
            "Fla Atlantic": "Florida Atlantic",
            "S Utah": "Southern Utah",
            "LA Monroe": "Louisiana-Monroe",
            "WI-Grn Bay": "Green Bay",
            "St Fran (NY)": "St. Francis (NY)",
            "St Fran (PA)": "Saint Francis (PA)",
            "Utah Val State": "Utah Valley",
            "Gard-Webb": "Gardner-Webb",
            "N Florida": "North Florida",
            "Loyola-MD": "Loyola (MD)",
            "VA Military": "VMI",
            "Mt State Marys": "Mount St. Mary's",
            "GA Tech": "Georgia Tech",
            "E Tenn State": "ETSU",
            "Rob Morris": "Robert Morris",
            "Neb Omaha": "Omaha",
            "St Josephs": "St. Joseph's",
            "St Peters": "St. Peter's",
            "S Car State": "South Carolina State",
            "Alab A&M": "Alabama A&M",
            "Miss Val State": "Mississippi Valley State",
            "CSFullerton": "Cal State Fullerton",
            "Miami Florida": "Miami (FL)",
            "USCUpstate": "USC Upstate",
            "Tennessee-Martin": "UT-Martin",
            "Central Connecticut State": "Central Connecticut",
            "Illinois-Chicago": "UIC",
            "Louisiana-Lafayette": "Louisiana",
            "Texas A&M-CC": "Texas A&M-Corpus Christi",
            "North Carolina State": "NC State",
            "Arkansas-Little Rock": "Little Rock",
            "Detroit Mercy": "Detroit",
            "Saint Joseph's (PA)": "St. Joseph's",
            "Purdue Fort Wayne": "Purdue-Fort Wayne",
            "Charleston": "College of Charleston",
            "UMKC": "Kansas City",
            "Nebraska-Omaha": "Omaha",
            "Texas Rio Grande Valley": "Texas-Rio Grande Valley",
            "Bryant University": "Bryant",
            "Charleston (WV)": "College of Charleston",
            "Virginia Military": "VMI",
            "American University": "American",
            "Virginia Commonwealth": "VCU",
            "Southern California": "USC",
            "Louisiana State": "LSU",
            "Queens": "Queens (NC)",
            "Queens University": "Queens (NC)",
            "TX-Arlington": "UT Arlington",
            "Texas A&M Commerce": "Texas A&M-Commerce",
            "TX A&M-Com": "Texas A&M-Commerce",
            "East Texas A&M": "Texas A&M-Commerce",
            "Hsn Christian": "Houston Christian",
            "Cal Baptist": "California Baptist",
            "St. Thomas (MN)": "St. Thomas",
            "St. Thomas-Minnesota": "St. Thomas",
            "Hawai'i": "Hawaii",
            "San José State": "San Jose State",
            "McNeese": "McNeese State",
            "Nicholls": "Nicholls State",
            "SE Missouri St": "Southeast Missouri State",
            "Alcorn": "Alcorn State",
            "Grambling": "Grambling",
            "Seattle U": "Seattle",
            "IU Indy": "IUPUI",
            "IU Indianapolis": "IUPUI",
            "South Carolina Upstate": "USC Upstate",
            "UL Monroe": "Louisiana-Monroe",
            "CSUN": "Cal State Northridge",
            "Cal St. Bakersfield": "Cal State Bakersfield",
            "Cal St. Fullerton": "Cal State Fullerton",
            "Southeast Missouri": "Southeast Missouri State",
            "AR-Pine Bluff": "Arkansas-Pine Bluff",
            "Bethune": "Bethune-Cookman",
            "C Arkansas": "Central Arkansas",
            "C Connecticut": "Central Connecticut",
            "E Tennessee State": "ETSU",
            "E Texas A&M": "Texas A&M-Commerce",
            "FGCU": "Florida Gulf Coast",
            "Georgia So": "Georgia Southern",
            "Hou Christian": "Houston Christian",
            "J Madison": "James Madison",
            "Loyola Chi": "Loyola (IL)",
            "Miss Valley State": "Mississippi Valley State",
            "Mt St Mary's": "Mount St. Mary's",
            "N Alabama": "North Alabama",
            "N Texas": "North Texas",
            "NC Asheville": "UNC Asheville",
            "NC Greensboro": "UNC Greensboro",
            "NC Wilmington": "UNC Wilmington",
            "Purdue FW": "Purdue-Fort Wayne",
            "S Indiana": "Southern Indiana",
            "St Thomas": "St. Thomas",
            "Tenn Tech": "Tennessee Tech",
            "Texas So": "Texas Southern",
            "UCSD": "UC-San Diego",
            "UT Rio Grande": "Texas-Rio Grande Valley",
            "W Florida": "West Florida",
            "W Georgia": "West Georgia",
        }

    def normalize_team_name(self, series):
        s = series.astype(str).str.strip()
        s = s.str.replace(r"\bSt\.?$", "State", regex=True)
        return s.replace(self.team_replace)

    def pull_teams_information(self):
        resp = self.session.get(
            f"{ESPN_BASE_URL}/teams", params={"limit": 500}, timeout=15
        )
        resp.raise_for_status()
        raw_teams = resp.json()["sports"][0]["leagues"][0]["teams"]

        rows = []
        for entry in raw_teams:
            t = entry["team"]
            rows.append(
                {
                    "team_id": str(t["id"]),
                    "name": t.get("location") or t.get("displayName"),
                    "display_name": t.get("displayName"),
                    "abbreviation": t.get("abbreviation", "").lower(),
                }
            )

        self.all_team_data = pd.DataFrame(rows)
        self.all_team_data["name"] = self.normalize_team_name(
            self.all_team_data["name"]
        )
        self.team_list = list(self.all_team_data["name"].unique())
        self.team_abbreviations = list(self.all_team_data["abbreviation"].unique())
        return self.all_team_data

    def _fetch_espn_scoreboard_day(self, day_dt):
        date_str = pd.to_datetime(day_dt).strftime("%Y%m%d")
        resp = self.session.get(
            f"{ESPN_BASE_URL}/scoreboard",
            params={"dates": date_str, "groups": "50", "limit": 500},
            timeout=15,
        )
        resp.raise_for_status()
        events = resp.json().get("events", [])

        games = []
        for ev in events:
            comp = ev["competitions"][0]
            competitors = {c["homeAway"]: c for c in comp.get("competitors", [])}
            if "home" not in competitors or "away" not in competitors:
                continue

            home = competitors["home"]
            away = competitors["away"]
            game_dt = pd.to_datetime(ev["date"], utc=True).tz_convert("US/Eastern")
            completed = comp.get("status", {}).get("type", {}).get("completed", False)

            spread = np.nan
            odds_list = comp.get("odds") or []
            if odds_list and odds_list[0].get("spread") is not None:
                try:
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
                    "away_id": str(away["team"]["id"]),
                    "away_abbr": away["team"].get("abbreviation", "").lower(),
                    "away_name": away["team"].get("location")
                    or away["team"].get("displayName"),
                    "away_score": pd.to_numeric(away.get("score"), errors="coerce")
                    if completed
                    else np.nan,
                    "home_id": str(home["team"]["id"]),
                    "home_abbr": home["team"].get("abbreviation", "").lower(),
                    "home_name": home["team"].get("location")
                    or home["team"].get("displayName"),
                    "home_score": pd.to_numeric(home.get("score"), errors="coerce")
                    if completed
                    else np.nan,
                    "espn_spread": spread,
                }
            )
        return games

    def _fetch_espn_team_schedule_dates(self, team_id, season):
        resp = self.session.get(
            f"{ESPN_BASE_URL}/teams/{team_id}/schedule",
            params={"season": season},
            timeout=15,
        )
        resp.raise_for_status()
        events = resp.json().get("events", [])
        rows = []
        for ev in events:
            game_dt = pd.to_datetime(ev["date"], utc=True).tz_convert("US/Eastern")
            rows.append(
                {
                    "team_id": str(team_id),
                    "game_id": str(ev["id"]),
                    "game_day": game_dt.tz_localize(None).normalize(),
                }
            )
        if not rows:
            return pd.DataFrame(columns=["team_id", "game_id", "days_between_games"])
        df = pd.DataFrame(rows).sort_values("game_day").drop_duplicates("game_id")
        df["days_between_games"] = df["game_day"].diff().dt.days
        return df[["team_id", "game_id", "days_between_games"]]

    @staticmethod
    def _compute_rest_from_season_games(games_df):
        df = games_df.copy()
        df["game_day"] = pd.to_datetime(df["datetime"]).dt.normalize()

        away_appearances = df[["game_id", "game_day", "away_name"]].rename(
            columns={"away_name": "team"}
        )
        home_appearances = df[["game_id", "game_day", "home_name"]].rename(
            columns={"home_name": "team"}
        )
        appearances = (
            pd.concat([away_appearances, home_appearances], ignore_index=True)
            .sort_values(["team", "game_day", "game_id"])
            .drop_duplicates(["team", "game_id"])
        )
        appearances["off_days"] = appearances.groupby("team")["game_day"].diff().dt.days

        df = df.merge(
            appearances[["game_id", "team", "off_days"]].rename(
                columns={"team": "away_name", "off_days": "away_off_days"}
            ),
            on=["game_id", "away_name"],
            how="left",
        )
        df = df.merge(
            appearances[["game_id", "team", "off_days"]].rename(
                columns={"team": "home_name", "off_days": "home_off_days"}
            ),
            on=["game_id", "home_name"],
            how="left",
        )

        df["away_short_rest"] = np.where(df["away_off_days"] == 1, 1, 0)
        df["away_long_rest"] = np.where(df["away_off_days"] > 6, 1, 0)
        df["home_short_rest"] = np.where(df["home_off_days"] == 1, 1, 0)
        df["home_long_rest"] = np.where(df["home_off_days"] > 6, 1, 0)
        return df

    def pull_schedule(self, start_date, end_date, include_completed=False):
        start_dt = pd.to_datetime(start_date).normalize()
        end_dt = pd.to_datetime(end_date).normalize()
        days = pd.date_range(start_dt, end_dt, freq="D")

        all_games = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = [pool.submit(self._fetch_espn_scoreboard_day, d) for d in days]
            for f in as_completed(futures):
                all_games.extend(f.result())

        if not all_games:
            return pd.DataFrame(
                columns=[
                    "away_abbr",
                    "away_name",
                    "home_abbr",
                    "home_name",
                    "date",
                    "neutral",
                    "away_short_rest",
                    "away_long_rest",
                    "home_short_rest",
                    "home_long_rest",
                ]
            )

        full_schedule = pd.DataFrame(all_games).drop_duplicates("game_id")
        full_schedule = full_schedule[
            full_schedule["away_id"] != full_schedule["home_id"]
        ]
        if not include_completed:
            full_schedule = full_schedule[~full_schedule["completed"]]

        if full_schedule.empty:
            return full_schedule

        season = _cbb_season_for_date(start_dt)
        team_ids = list(
            set(full_schedule["away_id"]).union(set(full_schedule["home_id"]))
        )
        rest_frames = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self._fetch_espn_team_schedule_dates, tid, season): tid
                for tid in team_ids
            }
            for f in as_completed(futures):
                try:
                    rest_frames.append(f.result())
                except Exception:
                    continue

        if rest_frames:
            rest_df = pd.concat(rest_frames, ignore_index=True)
            full_schedule = full_schedule.merge(
                rest_df.rename(
                    columns={
                        "team_id": "away_id",
                        "days_between_games": "away_off_days",
                    }
                ),
                on=["away_id", "game_id"],
                how="left",
            )
            full_schedule = full_schedule.merge(
                rest_df.rename(
                    columns={
                        "team_id": "home_id",
                        "days_between_games": "home_off_days",
                    }
                ),
                on=["home_id", "game_id"],
                how="left",
            )
        else:
            full_schedule["away_off_days"] = np.nan
            full_schedule["home_off_days"] = np.nan

        full_schedule["away_short_rest"] = np.where(
            full_schedule["away_off_days"] == 1, 1, 0
        )
        full_schedule["away_long_rest"] = np.where(
            full_schedule["away_off_days"] > 6, 1, 0
        )
        full_schedule["home_short_rest"] = np.where(
            full_schedule["home_off_days"] == 1, 1, 0
        )
        full_schedule["home_long_rest"] = np.where(
            full_schedule["home_off_days"] > 6, 1, 0
        )

        full_schedule["away_name"] = self.normalize_team_name(
            full_schedule["away_name"]
        )
        full_schedule["home_name"] = self.normalize_team_name(
            full_schedule["home_name"]
        )

        return full_schedule[
            [
                "away_abbr",
                "away_name",
                "home_abbr",
                "home_name",
                "date",
                "neutral",
                "away_short_rest",
                "away_long_rest",
                "home_short_rest",
                "home_long_rest",
            ]
        ].reset_index(drop=True)

    def pull_historical_schedule_cbbd(self, year):
        if not self.cbbd_api_key:
            raise ValueError(
                "CBBD_API_KEY is required for pull_historical_schedule_cbbd()."
            )

        resp = self.session.get(
            f"{CBBD_BASE_URL}/games",
            headers={"Authorization": f"Bearer {self.cbbd_api_key}"},
            params={"season": year},
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()

        rows = []
        for g in raw:
            if g.get("homePoints") is None or g.get("awayPoints") is None:
                continue
            dt = pd.to_datetime(g["startDate"], utc=True).tz_convert("US/Eastern")
            rows.append(
                {
                    "game_id": str(g["id"]),
                    "datetime": dt.tz_localize(None),
                    "date": _format_short_date(dt),
                    "year": year,
                    "neutral": int(bool(g.get("neutralSite", False))),
                    "away_abbr": (g.get("awayTeam") or "").lower().replace(" ", "-"),
                    "away_name": g.get("awayTeam"),
                    "away_score": g.get("awayPoints"),
                    "home_abbr": (g.get("homeTeam") or "").lower().replace(" ", "-"),
                    "home_name": g.get("homeTeam"),
                    "home_score": g.get("homePoints"),
                }
            )

        df = pd.DataFrame(rows).drop_duplicates("game_id")
        df["away_name"] = self.normalize_team_name(df["away_name"])
        df["home_name"] = self.normalize_team_name(df["home_name"])
        df = self._compute_rest_from_season_games(df)

        return df[
            [
                "away_abbr",
                "away_name",
                "away_score",
                "home_abbr",
                "home_name",
                "home_score",
                "date",
                "year",
                "neutral",
                "away_short_rest",
                "away_long_rest",
                "home_short_rest",
                "home_long_rest",
            ]
        ].reset_index(drop=True)

    def pull_historical_schedule(self, year):
        if self.cbbd_api_key:
            return self.pull_historical_schedule_cbbd(year)

        days = pd.date_range(datetime(year - 1, 11, 1), datetime(year, 4, 15), freq="D")
        all_games = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = [pool.submit(self._fetch_espn_scoreboard_day, d) for d in days]
            for f in as_completed(futures):
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

        return df[
            [
                "away_abbr",
                "away_name",
                "away_score",
                "home_abbr",
                "home_name",
                "home_score",
                "date",
                "year",
                "neutral",
                "away_short_rest",
                "away_long_rest",
                "home_short_rest",
                "home_long_rest",
            ]
        ].reset_index(drop=True)

    def pull_current_odds_the_odds_api(self, bookmakers=None, api_key=None):
        key = api_key or self.odds_api_key
        if not key:
            raise ValueError("THE_ODDS_API_KEY is required for The Odds API.")

        params = {
            "apiKey": key,
            "regions": "us",
            "markets": "spreads",
            "oddsFormat": "american",
        }
        if bookmakers:
            params["bookmakers"] = ",".join(bookmakers)

        resp = self.session.get(
            f"{ODDS_API_BASE_URL}/sports/basketball_ncaab/odds",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return self._parse_the_odds_api_response(resp.json())

    def pull_historical_odds_the_odds_api(self, date_iso, api_key=None):
        key = api_key or self.odds_api_key
        if not key:
            raise ValueError("THE_ODDS_API_KEY is required for The Odds API.")

        resp = self.session.get(
            f"{ODDS_API_BASE_URL}/historical/sports/basketball_ncaab/odds",
            params={
                "apiKey": key,
                "regions": "us",
                "markets": "spreads",
                "oddsFormat": "american",
                "date": date_iso,
            },
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        return self._parse_the_odds_api_response(payload.get("data", []))

    def _parse_the_odds_api_response(self, events):
        rows = []
        for ev in events:
            home_raw = ev["home_team"]
            away_raw = ev["away_team"]
            home_spreads = []
            for bm in ev.get("bookmakers", []):
                for mkt in bm.get("markets", []):
                    if mkt.get("key") != "spreads":
                        continue
                    for outcome in mkt.get("outcomes", []):
                        if outcome.get("name") == home_raw and outcome.get("point") is not None:
                            home_spreads.append(float(outcome["point"]))

            if home_spreads:
                consensus_home_spread = float(np.median(home_spreads))
                rows.append(
                    {
                        "away_name": away_raw,
                        "home_name": home_raw,
                        "spread": -consensus_home_spread,
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["away_name", "home_name", "spread"])

        df = pd.DataFrame(rows).drop_duplicates(["away_name", "home_name"])
        df["away_name"] = self.normalize_team_name(df["away_name"])
        df["home_name"] = self.normalize_team_name(df["home_name"])
        return df

    def pull_current_odds_espn(self, start_date, end_date=None):
        end_date = end_date or start_date
        days = pd.date_range(
            pd.to_datetime(start_date).normalize(),
            pd.to_datetime(end_date).normalize(),
            freq="D",
        )
        rows = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = [pool.submit(self._fetch_espn_scoreboard_day, d) for d in days]
            for f in as_completed(futures):
                for g in f.result():
                    if pd.notna(g["espn_spread"]):
                        rows.append(
                            {
                                "away_name": g["away_name"],
                                "home_name": g["home_name"],
                                "spread": g["espn_spread"],
                            }
                        )

        if not rows:
            return pd.DataFrame(columns=["away_name", "home_name", "spread"])

        df = pd.DataFrame(rows).drop_duplicates(["away_name", "home_name"])
        df["away_name"] = self.normalize_team_name(df["away_name"])
        df["home_name"] = self.normalize_team_name(df["home_name"])
        return df

    def pull_odds_cbbd(self, season, start_date=None, end_date=None, api_key=None):
        key = api_key or self.cbbd_api_key
        if not key:
            raise ValueError("CBBD_API_KEY is required for pull_odds_cbbd().")

        params = {"season": season}
        if start_date is not None:
            params["startDateRange"] = pd.to_datetime(start_date).strftime("%Y-%m-%dT00:00:00Z")
        if end_date is not None:
            params["endDateRange"] = pd.to_datetime(end_date).strftime("%Y-%m-%dT23:59:59Z")

        resp = self.session.get(
            f"{CBBD_BASE_URL}/lines",
            headers={"Authorization": f"Bearer {key}"},
            params=params,
            timeout=30,
        )
        resp.raise_for_status()

        rows = []
        for g in resp.json():
            lines = g.get("lines") or []
            spreads = [
                float(l["spread"])
                for l in lines
                if l.get("spread") is not None
            ]
            if not spreads:
                continue
            dt = pd.to_datetime(g["startDate"], utc=True).tz_convert("US/Eastern")
            rows.append(
                {
                    "away_name": g["awayTeam"],
                    "home_name": g["homeTeam"],
                    "close": -float(np.median(spreads)),
                    "spread": -float(np.median(spreads)),
                    "date": _format_short_date(dt),
                    "year": season,
                }
            )

        if not rows:
            return pd.DataFrame(
                columns=["away_name", "home_name", "close", "spread", "date", "year"]
            )

        df = pd.DataFrame(rows).drop_duplicates(["away_name", "home_name", "date", "year"])
        df["away_name"] = self.normalize_team_name(df["away_name"])
        df["home_name"] = self.normalize_team_name(df["home_name"])
        return df

    def pull_current_odds(self, start_date, end_date=None):
        end_date = end_date or start_date
        if self.cbbd_api_key:
            season = _cbb_season_for_date(start_date)
            odds = self.pull_odds_cbbd(season, start_date=start_date, end_date=end_date)
            if not odds.empty:
                return odds[["away_name", "home_name", "spread"]]

        odds = self.pull_current_odds_espn(start_date, end_date)
        if not odds.empty:
            return odds

        if self.odds_api_key:
            return self.pull_current_odds_the_odds_api()

        return pd.DataFrame(columns=["away_name", "home_name", "spread"])

    def pull_historical_odds(self, first_year=2015, last_year=2025):
        frames = [
            self.pull_odds_cbbd(year) for year in range(first_year, last_year + 1)
        ]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame(columns=["away_name", "home_name", "close", "date", "year"])
        return pd.concat(frames, ignore_index=True)[
            ["away_name", "home_name", "close", "date", "year"]
        ]

    def pull_ken_pom_data(self, year=None):
        if self.kenpom_api_key:
            return self.pull_ken_pom_official_api(year=year)

        url = f"https://kenpom.com/index.php?y={year}" if year else "https://kenpom.com/"
        import urllib.request

        req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "lxml")

        table = soup.find("table", id="ratings-table")
        if table is None:
            raise RuntimeError(
                "Could not find KenPom ratings-table. If KenPom blocked the request, "
                "set KENPOM_API_KEY for the official API or use kenpompy."
            )

        rows = []
        for tr in table.find_all("tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td") if td.get_text(strip=True)]
            if len(tds) >= 10:
                rows.append([tds[1], tds[5], tds[7], tds[9]])

        ken_pom = pd.DataFrame(
            rows, columns=["name", "ken_pom_offense", "ken_pom_defense", "tempo"]
        )
        ken_pom["name"] = ken_pom["name"].str.rstrip("0123456789 ")
        ken_pom["name"] = self.normalize_team_name(ken_pom["name"])

        numeric_cols = ["ken_pom_offense", "ken_pom_defense", "tempo"]
        ken_pom[numeric_cols] = ken_pom[numeric_cols].apply(pd.to_numeric, errors="coerce")
        ken_pom.dropna(subset=numeric_cols, inplace=True)
        ken_pom[numeric_cols] = (
            ken_pom[numeric_cols] - ken_pom[numeric_cols].mean()
        ) / ken_pom[numeric_cols].std(ddof=0)

        return ken_pom.reset_index(drop=True)

    def pull_ken_pom_official_api(self, year=None, api_key=None):
        key = api_key or self.kenpom_api_key
        if not key:
            raise ValueError("KENPOM_API_KEY is required for the official KenPom API.")

        season = year or _cbb_season_for_date(pd.Timestamp.today())
        resp = self.session.get(
            "https://kenpom.com/api.php",
            headers={"Authorization": f"Bearer {key}"},
            params={"endpoint": "ratings", "y": season},
            timeout=15,
        )
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())

        ken_pom = df[["TeamName", "AdjOE", "AdjDE", "AdjTempo"]].rename(
            columns={
                "TeamName": "name",
                "AdjOE": "ken_pom_offense",
                "AdjDE": "ken_pom_defense",
                "AdjTempo": "tempo",
            }
        )
        ken_pom["name"] = self.normalize_team_name(ken_pom["name"])
        numeric_cols = ["ken_pom_offense", "ken_pom_defense", "tempo"]
        ken_pom[numeric_cols] = ken_pom[numeric_cols].apply(pd.to_numeric, errors="coerce")
        ken_pom[numeric_cols] = (
            ken_pom[numeric_cols] - ken_pom[numeric_cols].mean()
        ) / ken_pom[numeric_cols].std(ddof=0)
        return ken_pom.reset_index(drop=True)

    pull_ken_pom_data_selenium = pull_ken_pom_data

    def _fetch_team_rankings_table(self, path, col_name, year=None, is_pct=False, strip_record=False):
        url = f"https://www.teamrankings.com/ncaa-basketball/{path}"
        if year is not None:
            url = f"{url}?date={year}-05-01"

        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        table = soup.find("table", attrs={"class": "tr-table datatable scrollable"})
        if table is None:
            raise RuntimeError(f"Could not find TeamRankings table at {url}")

        rows = []
        for tr in table.find_all("tr"):
            tds = [td.get_text(strip=True) for td in tr.find_all("td") if td.get_text(strip=True)]
            if len(tds) >= 3 and tds[2] != "--":
                rows.append((tds[1], tds[2]))

        df = pd.DataFrame(rows, columns=["name", col_name])
        if strip_record:
            df["name"] = df["name"].str.replace(r"\s*\(\d+-\d+\)$", "", regex=True).str.strip()

        vals = df[col_name].str.rstrip("%").astype(float)
        df[col_name] = vals / 100.0 if is_pct else vals
        return df

    def pull_team_rankings_data(self, year=None, early_season=False):
        specs = [
            ("ranking/predictive-by-other", "team_rankings_rating", False, True),
            ("ranking/schedule-strength-by-other", "strength_of_schedule", False, True),
            ("stat/offensive-efficiency", "offensive_efficiency", False, False),
            ("stat/defensive-efficiency", "defensive_efficiency", False, False),
            ("stat/turnovers-per-possession", "to_percentage", True, False),
            ("stat/opponent-turnovers-per-possession", "opp_to_percentage", True, False),
            ("stat/defensive-rebounding-pct", "def_rebounding_percentage", True, False),
            ("stat/offensive-rebounding-pct", "off_rebounding_percentage", True, False),
            ("stat/free-throw-rate", "ft_rate", True, False),
            ("stat/opponent-free-throw-rate", "opp_ft_rate", True, False),
        ]
        if early_season:
            specs = specs[:2]

        results = {}
        with ThreadPoolExecutor(max_workers=len(specs)) as pool:
            future_map = {
                pool.submit(
                    self._fetch_team_rankings_table,
                    path,
                    col,
                    year,
                    is_pct,
                    strip_rec,
                ): col
                for path, col, is_pct, strip_rec in specs
            }
            for f in as_completed(future_map):
                col = future_map[f]
                results[col] = f.result()

        ordered_cols = [s[1] for s in specs]
        df = results[ordered_cols[0]]
        for col in ordered_cols[1:]:
            df = df.merge(results[col], on="name", how="left")

        df["name"] = self.normalize_team_name(df["name"])
        df.dropna(subset=[ordered_cols[-1]], inplace=True)

        num_cols = ordered_cols
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
        df[num_cols] = (df[num_cols] - df[num_cols].mean()) / df[num_cols].std(ddof=0)
        return df.reset_index(drop=True)

    def create_opponent_adjusted_stats(self, data, index_start):
        data = data.copy()
        data.iloc[:, index_start:] = data.iloc[:, index_start:].apply(pd.to_numeric)
        updated = data.iloc[:, :index_start].copy()

        updated["team_rankings_rating"] = (
            data["home_team_rankings_rating"] - data["away_team_rankings_rating"]
        )
        updated["strength_of_schedule"] = (
            data["home_strength_of_schedule"] - data["away_strength_of_schedule"]
        )
        updated["home_ken_pom"] = (
            data["home_ken_pom_offense"] + data["away_ken_pom_defense"]
        )
        updated["away_ken_pom"] = (
            -data["away_ken_pom_offense"] - data["home_ken_pom_defense"]
        )
        if "home_offensive_efficiency" in data.columns:
            updated["home_efficiency"] = (
                data["home_offensive_efficiency"] + data["away_defensive_efficiency"]
            )
            updated["away_efficiency"] = (
                -data["away_offensive_efficiency"] - data["home_defensive_efficiency"]
            )
            updated["home_rebounding"] = (
                data["home_def_rebounding_percentage"]
                - data["away_off_rebounding_percentage"]
            )
            updated["away_rebounding"] = (
                -data["away_def_rebounding_percentage"]
                + data["home_off_rebounding_percentage"]
            )
            updated["home_turnover_rate"] = (
                -data["home_to_percentage"] - data["away_opp_to_percentage"]
            )
            updated["away_turnover_rate"] = (
                data["away_to_percentage"] + data["home_opp_to_percentage"]
            )
            updated["home_ft_rate"] = data["home_ft_rate"] + data["away_opp_ft_rate"]
            updated["away_ft_rate"] = -data["away_ft_rate"] - data["home_opp_ft_rate"]

        return updated

    def _merge_team_features(self, games_df, year=None, early_season=False):
        ken_pom = self.pull_ken_pom_data(year)
        tr = self.pull_team_rankings_data(year, early_season=early_season)

        away_kp = ken_pom.add_prefix("away_")
        home_kp = ken_pom.add_prefix("home_")
        away_tr = tr.add_prefix("away_")
        home_tr = tr.add_prefix("home_")

        data = games_df.merge(away_kp, on="away_name", how="left")
        data = data.merge(home_kp, on="home_name", how="left")
        data = data.merge(away_tr, on="away_name", how="left")
        data = data.merge(home_tr, on="home_name", how="left")
        return data

    def create_single_game_df(self, away, home, neutral, home_rest=2, away_rest=2):
        data = pd.DataFrame(
            {
                "away_name": [away, home] if neutral else [away],
                "home_name": [home, away] if neutral else [home],
                "neutral": neutral,
                "away_short_rest": 1 if away_rest == 1 else 0,
                "away_long_rest": 1 if away_rest > 6 else 0,
                "home_short_rest": 1 if home_rest == 1 else 0,
                "home_long_rest": 1 if home_rest > 6 else 0,
            }
        )
        data["away_name"] = self.normalize_team_name(data["away_name"])
        data["home_name"] = self.normalize_team_name(data["home_name"])

        data = self._merge_team_features(data, year=None)
        missing = data.loc[data.isna().any(axis=1), ["home_name", "away_name"]]
        if not missing.empty:
            print("Dropped due to missing team stats:\n", missing)
        data.dropna(inplace=True)
        return self.create_opponent_adjusted_stats(data, 7)

    def create_dataframe(self, start_date, end_date, early_season=False):
        data = self.pull_schedule(start_date, end_date)
        if data.empty:
            return data

        data = self._merge_team_features(data, year=None, early_season=early_season)
        missing = data.loc[data.isna().any(axis=1), ["home_name", "away_name"]]
        if not missing.empty:
            print("Dropped due to missing team stats:\n", missing)
        data.dropna(inplace=True)

        data = self.create_opponent_adjusted_stats(data, 10)

        odds = self.pull_current_odds(start_date, end_date)
        if not odds.empty:
            reversed_odds = pd.DataFrame(
                {
                    "home_name": odds["away_name"],
                    "away_name": odds["home_name"],
                    "spread": -odds["spread"],
                }
            )
            odds = pd.concat([odds, reversed_odds], ignore_index=True).drop_duplicates(
                ["away_name", "home_name"]
            )
            data = data.merge(odds, on=["away_name", "home_name"], how="left")
        else:
            data["spread"] = np.nan

        data.drop(columns=["away_abbr", "home_abbr"], inplace=True)
        return data

    def create_historical_data(self, year):
        historical_data = self.pull_historical_schedule(year)
        historical_data["neutral"] = np.where(historical_data["neutral"] == 1, 1, 0)

        historical_data = self._merge_team_features(historical_data, year=year)
        missing = historical_data.loc[
            historical_data.isna().any(axis=1), ["home_name", "away_name"]
        ]
        if not missing.empty:
            print(f"[{year}] Dropped {len(missing)} games due to missing team stats")
        historical_data.dropna(inplace=True)

        historical_data = self.create_opponent_adjusted_stats(historical_data, 13)
        historical_data["result"] = (
            historical_data["home_score"] - historical_data["away_score"]
        )
        historical_data.dropna(subset=["result"], inplace=True)
        return historical_data

    def create_historical_dataframe(self, first_year=2015, last_year=2025):
        full_data = []
        for year in range(first_year, last_year + 1):
            subset_data = self.create_historical_data(year)
            full_data.append(subset_data)
            pd.concat(full_data, ignore_index=True).to_csv(
                "historical_cbb_data.csv", index=False
            )

        full_data = pd.concat(full_data, ignore_index=True)

        if self.cbbd_api_key:
            odds = self.pull_historical_odds(first_year, last_year)
            if not odds.empty:
                reversed_odds = pd.DataFrame(
                    {
                        "home_name": odds["away_name"],
                        "away_name": odds["home_name"],
                        "close": -odds["close"],
                        "year": odds["year"],
                        "date": odds["date"],
                    }
                )
                odds = pd.concat([odds, reversed_odds], ignore_index=True).drop_duplicates(
                    ["away_name", "home_name", "date", "year"]
                )
                full_data = full_data.merge(
                    odds, on=["away_name", "home_name", "date", "year"], how="left"
                )

        full_data.to_csv("historical_cbb_data_full.csv", index=False)

        model_df = full_data.drop(
            columns=[
                "away_abbr",
                "home_abbr",
                "away_name",
                "home_name",
                "home_score",
                "away_score",
                "date",
            ]
        )
        model_df.to_csv("historical_cbb_data.csv", index=False)
        return model_df

    def create_clustering_dataframe(self, year=None):
        specs = [
            ("stat/possessions-per-game", "possessions_per_game", False, False),
            ("stat/offensive-rebounding-pct", "off_rebounding_percentage", True, False),
            ("stat/defensive-rebounding-pct", "def_rebounding_percentage", True, False),
            ("stat/personal-fouls-per-possession", "foul_rate", True, False),
            ("stat/opponent-personal-fouls-per-possession", "opp_foul_rate", True, False),
            ("stat/three-point-rate", "three_point_rate", True, False),
            ("stat/opponent-three-point-rate", "opp_three_point_rate", True, False),
        ]

        results = {}
        with ThreadPoolExecutor(max_workers=len(specs)) as pool:
            future_map = {
                pool.submit(
                    self._fetch_team_rankings_table, path, col, year, is_pct, strip_rec
                ): col
                for path, col, is_pct, strip_rec in specs
            }
            for f in as_completed(future_map):
                results[future_map[f]] = f.result()

        ordered_cols = [s[1] for s in specs]
        df = results[ordered_cols[0]]
        for col in ordered_cols[1:]:
            df = df.merge(results[col], on="name", how="left")

        df["name"] = self.normalize_team_name(df["name"])
        df.dropna(inplace=True)
        df[ordered_cols] = StandardScaler().fit_transform(df[ordered_cols])
        df["year"] = year
        return df.reset_index(drop=True)

    def create_historical_clustering_dataframe(self, first_year=2015, last_year=2025):
        frames = [
            self.create_clustering_dataframe(year)
            for year in range(first_year, last_year + 1)
        ]
        return pd.concat(frames, ignore_index=True)
