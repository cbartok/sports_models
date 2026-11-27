# sports_models

Automated Python quantitative sports modeling pipelines for **College Basketball (CBB)**, **College Football (CFB)**, **NFL**, and **MLB**—requiring **zero paid API keys** and publishing directly to GitHub Pages.

**Live Predictions Hub:** [cbartok.github.io/models](https://cbartok.github.io/models/)
- [College Basketball (CBB)](https://cbartok.github.io/cbb/)
- [College Football (CFB)](https://cbartok.github.io/cfb/)
- [NFL](https://cbartok.github.io/nfl/)
- [MLB](https://cbartok.github.io/mlb/)

---

## Status & Automation Schedule

| Sport | Status | Data Sources (Zero API Key) | Model | Schedule (PT) |
| :--- | :--- | :--- | :--- | :--- |
| **College Basketball** | Active (11/2026) | ESPN Public JSON API, KenPom (direct HTTP / optional API), TeamRankings (concurrent) | 12-season (`2015–2026`, 62,629 games) EM-BMA Ensemble (**LASSO**, **RF**, **XGBoost**, **SVR**) — Early-Season (Nov 1–25) & Full-Season | Daily at **7:13 AM** (`Nov–Apr`) |
| **College Football** | Active (11/2026) | ESPN CFB API (`groups=80`), BCFToys (`F+`, `OF+`, `DF+`, `SF+`, `FEI`), TeamRankings (14 stats, concurrent) | 12-season (`2014–2025`, 9,022 FBS games) EM-BMA Ensemble — Early-Season (Aug–Sep) & Full-Season (Oct–Jan) | Daily at **7:17 AM** (`Aug–Jan`, upcoming 7-day slate) |
| **NFL** | Active (11/2026) | `nflverse` (`games.csv` + `stats_team_week_{year}.csv` EPA/play & efficiency), ESPN NFL API, TeamRankings (`/ranking/`) | 16-season (`2010–2025`, 4,363 games) EM-BMA Ensemble — Early-Season (Sep) & Full-Season (Oct–Feb) | Daily at **7:21 AM** (`Sep–Feb`, upcoming 7-day slate) |
| **MLB** | Active (11/2026) | Official MLB Stats API (lineups, gameLogs, bullpen pitch loads), Baseball Savant (Statcast `xwOBA`, `xERA`, `OAA`, Framing, Baserunning), ESPN DraftKings Moneylines | Matchup-level Runs Above Average (RAA) + Pythagenpat Win Probability & Fair Odds (HFA calibrated on 17,006 games) | Every **30 min**, **7:00 AM–7:30 PM** (`Mar–Oct`) |

All four workflows overwrite a single `latest.json` under `docs/data/` (no daily archive bloat) which is consumed directly by [cbartok.github.io](https://cbartok.github.io/models/).

---

## Spread Ensembles (CBB, CFB & NFL)

All three spread models share a unified modeling framework replacing deprecated `sportsipy`, `selenium`, dead Football Outsiders, and Sportsbook Review scrapers:

- **Spread Convention**: Positive spreads and predictions always favor the **home team** (matching `home_score - away_score`).
- **Training & Validation**:
  - Season-grouped `RandomizedSearchCV` (`GroupKFold` by year) to prevent end-of-season rating leakage.
  - **Truncated MAE** scoring (`20 pts` for CBB, `16 pts` for CFB & NFL) + Pseudo-Huber loss (`reg:pseudohubererror`) for XGBoost so blowouts don't dominate tuning.
  - **Expectation-Maximization Bayesian Model Averaging (EM-BMA)** weights fitted on a held-out validation season and evaluated out-of-sample on the most recent season before refitting on all seasons.
- **Automatic Early-Season / Full-Season Switching**:
  - **CBB**: Uses the 9-feature preseason-prior model (`cbb_early_model.pickle`: KenPom + TeamRankings predictive/SOS + rest) Nov 1–25, switching to the 17-feature full model (`cbb_model.pickle`) Nov 26–Apr.
  - **CFB**: Uses the 12-feature model (`cfb_model.pickle`: BCFToys F+/FEI + TeamRankings predictive/SOS + rest) Aug–Sep, switching to the 24-feature full model (`cfb_full_model.pickle`) Oct–Jan.
  - **NFL**: Uses the 10-feature model (`nfl_model.pickle`: `nflverse` EPA/play + TeamRankings predictive/SOS + short/bye rest) in Sep, switching to the 22-feature full model (`nfl_full_model.pickle`) Oct–Feb.

---

## MLB Pipeline

See [`MLB/README.md`](MLB/README.md) for full model details.

- **Offense (`Off_RAA`)**: 9-man starting batting order from the MLB Stats API evaluated with FanGraphs `wRC+`/`BsR` (if provided) or Bayesian-regressed Statcast `xwOBA` + `wOBA`, baserunning, and L/R platoon splits.
- **Pitching & Bullpen (`Pitch_RAA_allowed`)**: Starter true-talent run rate (`SIERA` or `xERA`/`FIP` blend), exact `gameLog`-verified starter IP for swingmen/converted relievers, opener/bulk detection, and 2-day bullpen pitch-count fatigue weighting.
- **Defense & Fair Odds (`Def_R
