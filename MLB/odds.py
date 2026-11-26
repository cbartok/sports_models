"""
MLB Odds & +EV / Kelly Calculator.
Default provider: ESPN Core API (DraftKings moneylines, free, no API key).
Optional provider: The Odds API (multi-book, requires ODDS_API_KEY).
"""

import os
import requests

ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"

TEAM_NAME_TO_ABBR = {
    "Arizona Diamondbacks": "AZ",
    "Atlanta Braves": "ATL",
    "Athletics": "ATH",
    "Oakland Athletics": "ATH",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

ESPN_ABBR_TO_MLB = {
    "CHW": "CWS",
    "ARI": "AZ",
    "WAS": "WSH",
    "OAK": "ATH",
}


def american_to_decimal(american: int | float) -> float:
    if american >= 100:
        return 1.0 + (american / 100.0)
    return 1.0 + (100.0 / abs(american))


def american_to_implied_prob(american: int | float) -> float:
    if american >= 100:
        return 100.0 / (american + 100.0)
    return abs(american) / (abs(american) + 100.0)


def strip_vig(away_american: float, home_american: float) -> tuple[float, float]:
    p_a = american_to_implied_prob(away_american)
    p_h = american_to_implied_prob(home_american)
    total = p_a + p_h
    if total <= 0:
        return 0.5, 0.5
    return p_a / total, p_h / total


def calc_ev(model_prob: float, book_american: float) -> float:
    dec = american_to_decimal(book_american)
    return (model_prob * dec) - 1.0


def calc_kelly_fraction(
    model_prob: float,
    book_american: float,
    kelly_mult: float = 0.25,
    max_stake: float = 0.03,
) -> float:
    dec = american_to_decimal(book_american)
    b = dec - 1.0
    if b <= 0:
        return 0.0
    full_kelly = (dec * model_prob - 1.0) / b
    if full_kelly <= 0:
        return 0.0
    return min(max_stake, kelly_mult * full_kelly)


def fetch_espn_mlb_odds(target_date: str) -> dict[tuple[str, str], dict]:
    date_compact = target_date.replace("-", "")
    sb_url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={date_compact}"
    sb = requests.get(sb_url, timeout=15).json()

    odds_map: dict[tuple[str, str], dict] = {}
    for ev in sb.get("events", []):
        eid = ev.get("id")
        comps = ev.get("competitions", [])
        if not eid or not comps:
            continue

        away_abbr, home_abbr = None, None
        for c in comps[0].get("competitors", []):
            raw_abbr = c.get("team", {}).get("abbreviation", "")
            abbr = ESPN_ABBR_TO_MLB.get(raw_abbr, raw_abbr)
            if c.get("homeAway") == "away":
                away_abbr = abbr
            elif c.get("homeAway") == "home":
                home_abbr = abbr

        if not away_abbr or not home_abbr:
            continue

        core_url = f"https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{eid}/competitions/{eid}/odds"
        try:
            core = requests.get(core_url, timeout=15).json()
        except Exception:
            continue

        items = core.get("items", [])
        if not items:
            continue

        item = items[0]
        away_ml = item.get("awayTeamOdds", {}).get("moneyLine")
        home_ml = item.get("homeTeamOdds", {}).get("moneyLine")
        if away_ml is None or home_ml is None:
            continue

        away_ml, home_ml = int(away_ml), int(home_ml)
        provider = item.get("provider", {}).get("name", "DraftKings")
        nv_a, nv_h = strip_vig(away_ml, home_ml)

        odds_map[(away_abbr, home_abbr)] = {
            "best_away": (away_ml, provider),
            "best_home": (home_ml, provider),
            "no_vig_away": nv_a,
            "no_vig_home": nv_h,
            "sharp_source": provider,
        }

    print(f"  ESPN Odds API: loaded {len(odds_map)} MLB game moneylines for {target_date}")
    return odds_map


def fetch_live_mlb_odds(api_key: str | None = None, regions: str = "us") -> dict[tuple[str, str], dict]:
    key = api_key or os.getenv("ODDS_API_KEY")
    if not key:
        raise ValueError(
            "Missing ODDS_API_KEY. Get a free key at https://the-odds-api.com or use --odds-source espn."
        )

    params = {
        "apiKey": key,
        "regions": regions,
        "markets": "h2h",
        "oddsFormat": "american",
    }
    resp = requests.get(ODDS_API_URL, params=params, timeout=20)
    resp.raise_for_status()
    events = resp.json()

    remaining = resp.headers.get("x-requests-remaining")
    if remaining is not None:
        print(f"  The Odds API: {len(events)} live MLB events loaded ({remaining} monthly requests remaining)")

    odds_map: dict[tuple[str, str], dict] = {}
    for ev in events:
        away_abbr = TEAM_NAME_TO_ABBR.get(ev.get("away_team", ""))
        home_abbr = TEAM_NAME_TO_ABBR.get(ev.get("home_team", ""))
        if not away_abbr or not home_abbr:
            continue

        best_away = None
        best_home = None
        pinnacle_pair = None
        all_pairs = []

        for bm in ev.get("bookmakers", []):
            book_key = bm.get("key", "")
            book_title = bm.get("title", book_key)
            for mkt in bm.get("markets", []):
                if mkt.get("key") != "h2h":
                    continue
                away_price, home_price = None, None
                for out in mkt.get("outcomes", []):
                    if out.get("name") == ev.get("away_team"):
                        away_price = int(out["price"])
                    elif out.get("name") == ev.get("home_team"):
                        home_price = int(out["price"])

                if away_price is not None and home_price is not None:
                    all_pairs.append((away_price, home_price))
                    if book_key == "pinnacle":
                        pinnacle_pair = (away_price, home_price)

                    dec_a = american_to_decimal(away_price)
                    dec_h = american_to_decimal(home_price)
                    if best_away is None or dec_a > best_away[0]:
                        best_away = (dec_a, away_price, book_title)
                    if best_home is None or dec_h > best_home[0]:
                        best_home = (dec_h, home_price, book_title)

        if not best_away or not best_home:
            continue

        if pinnacle_pair:
            nv_a, nv_h = strip_vig(pinnacle_pair[0], pinnacle_pair[1])
            sharp_source = "Pinnacle"
        else:
            nv_list = [strip_vig(a, h) for a, h in all_pairs]
            nv_a = sum(x[0] for x in nv_list) / len(nv_list)
            nv_h = sum(x[1] for x in nv_list) / len(nv_list)
            sharp_source = f"Consensus ({len(nv_list)} books)"

        odds_map[(away_abbr, home_abbr)] = {
            "best_away": (best_away[1], best_away[2]),
            "best_home": (best_home[1], best_home[2]),
            "no_vig_away": nv_a,
            "no_vig_home": nv_h,
            "sharp_source": sharp_source,
        }

    return odds_map
