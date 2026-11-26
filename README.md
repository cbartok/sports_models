# sports_models

Python predictive modeling pipelines for College Basketball, MLB, College Football, and the NFL.

**Live Daily CBB Predictions:** [cbartok.github.io/sports_models](https://cbartok.github.io/sports_models/)

---

## Status by Sport

| Sport | Status | Data Sources | Model |
| :--- | :--- | :--- | :--- |
| **College Basketball** | Active (updated 11/2026) | ESPN Public JSON API, KenPom (direct HTTP / official API), TeamRankings (concurrent), optional CBBD & The Odds API | 12-season (`2015–2026`, 62,629 games) EM-BMA Ensemble (**LASSO**, **Random Forest**, **XGBoost**, **SVR**) |
| **MLB** | Active | Official MLB Stats API, Baseball Savant (Statcast xwOBA, xERA, OAA, Framing, Baserunning), optional FanGraphs projections | Runs Above Average (RAA) + Pythagenpat Win Probability & Fair Odds (calibrated on 17,006 games) |
| **College Football** | Legacy (9/2021) | `sportsipy` *(deprecated due to Sports Reference rate limits — pending API migration)* | Ensemble regression |
| **NFL** | Legacy (9/2019) | `sportsipy` *(deprecated due to Sports Reference rate limits — pending API migration)* | Ensemble regression |

---

## College Basketball Pipeline

The College Basketball model was rebuilt to remove `sportsipy` and `selenium`/`ChromeDriver` in favor of direct JSON APIs and concurrent HTTP requests:

- **Data Layer (`College Basketball/cbb_data_layer.py`)**
  - **Schedules, Neutral-Site Flags & Days of Rest**: ESPN Public JSON API (no API key required) or CollegeBasketballData.com (`CBBD_API_KEY`).
  - **Betting Spreads**: Current spreads via ESPN Scoreboard API (free, no key), CBBD `/lines`, or The Odds API (`THE_ODDS_API_KEY`); historical closing spreads via CBBD `/lines` or The Odds API historical snapshots.
  - **Efficiency & Four Factors**: Direct HTTP KenPom fetching (or `KENPOM_API_KEY` for KenPom's official REST API) and concurrent `ThreadPoolExecutor` fetching from TeamRankings, with 365/365 Division I team-name alignment.
- **Model Builder (`College Basketball/cbb_model_builder.py`)**
  - Season-grouped `RandomizedSearchCV` (`GroupKFold` by year) to prevent end-of-season rating leakage.
  - **Truncated MAE (capped at 20 points)** on `|actual - predicted|` for hyperparameter selection so unexpected upsets or blowouts don't dominate model tuning, plus Pseudo-Huber loss (`reg:pseudohubererror`) for XGBoost.
  - **EM-based Bayesian Model Averaging (BMA)** fitted on a held-out validation season (`2025`) and evaluated out-of-sample on `2026` before refitting on all 12 seasons (`2015–2026`).
- **Daily Automation (`.github/workflows/daily_cbb_predictions.yml` & `College Basketball/cbb_model_predictor.py`)**
  - Runs automatically every morning at **7:00 AM Pacific** during college basketball season (`Nov–Apr`, DST-safe) via GitHub Actions.
  - Publishes interactive daily predictions to [cbartok.github.io/sports_models](https://cbartok.github.io/sports_models/) (`docs/data/latest.json` + daily archives) and optionally exports to Google Sheets via `GOOGLE_SERVICE_ACCOUNT_JSON`.

---

## MLB Pipeline

See [`MLB/README.md`](MLB/README.md) for full details.

- **Offense (`Off_RAA`)**: 9-man starting batting order from the MLB Stats API, evaluated with FanGraphs `wRC+` / `BsR` or Bayesian-regressed Statcast `xwOBA` + `wOBA` with L/R platoon splits.
- **Pitching & Bullpen (`Pitch_RAA_allowed`)**: Starter true-talent run rate (`SIERA` or `xERA`/`FIP` blend), dynamic expected IP, and 2-day bullpen pitch-count fatigue weighting.
- **Defense & Fair Odds (`Def_RAA`)**: Statcast OAA + catcher framing, 5-year ballpark factors, Pythagenpat win probability, and empirical Home-Field Advantage (`+0.1254` log-odds).

```bash
cd MLB
pip install -r requirements.txt
python3 model.py --date 2026-07-04
```

---

## Quick Start (College Basketball)

```bash
cd "College Basketball"
pip install -r requirements.txt

# Run today's predictions locally (no API keys required)
python cbb_model_predictor.py --no-sheets

# Retrain the 12-season EM-BMA ensemble
python cbb_model_builder.py
```

### Optional Environment Variables / GitHub Secrets
- `GOOGLE_SERVICE_ACCOUNT_JSON` — Service account JSON for exporting daily predictions to Google Sheets
- `GOOGLE_SHEET_ID` — Target Google Sheet ID
- `CBBD_API_KEY` — Free key from [CollegeBasketballData.com](https://collegebasketballdata.com/key) for 1-call seasonal schedules and historical spreads
- `THE_ODDS_API_KEY` — Key from [The Odds API](https://the-odds-api.com/) for multi-sportsbook spreads
- `KENPOM_API_KEY` — Subscriber key for KenPom's official REST API

---

## Disclaimer

For informational and entertainment purposes only. All predictions are automated statistical model outputs—not betting advice, financial recommendations, or guarantees of any outcome.
