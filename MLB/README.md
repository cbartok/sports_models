# MLB Win-Probability & Fair-Odds Model

A quantitative MLB matchup model built on **Runs Above Average (RAA)**, **Pythagenpat** win probability, and an empirically calibrated **Home-Field Advantage** constant.

## Architecture

1. **Offense (Off_RAA):**
   - Pulls each team's 9-man starting battingOrder from the official MLB Stats API.
   - Evaluates each batter using FanGraphs projected wRC+ & BsR (when CSVs are provided) or a Bayesian-regressed blend of Baseball Savant xwOBA + MLB wOBA and Savant Baserunning (runner_runs).
   - Applies L/R platoon split adjustments (shrunk toward league-average handedness priors) against the opposing starting pitcher.
   - Weights each batter by expected plate appearances per batting-order slot (4.65 for leadoff down to 3.85 for #9).
2. **Pitching & Bullpen Availability (Pitch_RAA_allowed):**
   - Estimates pitcher true-talent run rate via FanGraphs SIERA (if provided) or a 50/50 blend of Statcast xERA + regressed FIP.
   - Computes dynamic expected starter innings (IP / GS), adjusted for swingmen/relief appearances and regressed toward 5.3 IP.
   - Assigns the remaining 9.0 - SP_IP innings to the team's fatigue-adjusted bullpen, scanning the previous 2 days of boxscores to down-weight relievers with heavy recent pitch counts (15+ or 30+ pitches).
3. **Defense & Catcher Framing (Def_RAA):**
   - Sums the 9 starting fielders' Baseball Savant OAA (fielding_runs_prevented) and the starting catcher's Framing (rv_tot) scaled to per-game runs.
4. **Win Probability & Fair Odds:**
   - Applies 5-year ballpark factors to compute expected runs (RS_home, RS_away).
   - Converts expected runs to neutral win probability using Pythagenpat: gamma = (RS_home + RS_away) ** 0.287.
   - Adds empirical Home-Field Advantage (+0.1254 log-odds ~ 53.13% neutral home win%) fitted on 17,006 MLB games (2019, 2021-2026).

## Quickstart

    cd MLB
    pip install -r requirements.txt
    python3 model.py --date 2026-07-04

## Optional: FanGraphs Projections (Steamer / ATC / ZiPS)

Export batting and pitching projection CSVs from FanGraphs (including MLBAMID) and place them in:
- MLB/projections/fangraphs_bat.csv
- MLB/projections/fangraphs_pit.csv
