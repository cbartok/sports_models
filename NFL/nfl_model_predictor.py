import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import nfl_data_layer as ndl
import nfl_model_builder as nmb
from nfl_model_builder import EnsembleModel  # noqa: F401

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


def create_ensemble_fit_table(model_list, features):
    forecast_fit_table = pd.DataFrame()
    for model in model_list:
        forecast_fit_table[f"{model['name']}_FIT"] = model["model"].predict(features)
    return forecast_fit_table


def predict_week(start_date=None, end_date=None, output_csv="nfl_predictions.csv", force_full=None):
    now_pt = datetime.now(PACIFIC_TZ)
    ref_date = pd.Timestamp(now_pt.date()) if start_date is None else pd.to_datetime(start_date)

    week_start = ref_date if start_date is None else pd.to_datetime(start_date)
    week_end = pd.to_datetime(end_date) if end_date else week_start + pd.DateOffset(days=6)

    if force_full is not None:
        use_full = force_full
    else:
        use_full = (ref_date.month > 9) or (ref_date.month <= 2)

    model_file = "nfl_full_model.pickle" if use_full else "nfl_model.pickle"
    feature_cols = nmb.FULL_FEATURE_COLUMNS if use_full else nmb.EARLY_FEATURE_COLUMNS
    print(f"Using {'FULL' if use_full else 'EARLY-SEASON'} NFL model ({model_file}) for {week_start.date()} to {week_end.date()}")

    nfl_model = nmb.import_nfl_model(file_name=model_file)
    nfl_data = ndl.NflDataLayer()

    data = nfl_data.create_dataframe(week_start, week_end)
    if data.empty:
        print(f"No upcoming NFL games found for {week_start.date()} to {week_end.date()}.")
        empty_df = pd.DataFrame(
            columns=["date", "away_name", "home_name", "neutral", "spread", "prediction", "difference"]
        )
        empty_df.to_csv(output_csv, index=False)
        return empty_df

    model_data = data.copy()
    model_data["neutral"] = np.where(model_data["neutral"] == 1, 1, 0)
    game_features = model_data[feature_cols].to_numpy()

    ensemble_preds = create_ensemble_fit_table(nfl_model.model_list, game_features)
    predictions = np.dot(ensemble_preds, nfl_model.weights)

    results = data[["date", "away_name", "home_name", "neutral", "spread"]].copy()
    results["prediction"] = np.round(predictions, 2)
    results["difference"] = np.round(results["prediction"] - results["spread"], 2)
    results = results.drop_duplicates(subset=["away_name", "home_name"]).sort_values(
        ["date", "away_name"]
    )
    results.to_csv(output_csv, index=False)
    return results


def export_github_pages_json(results_df, target_date=None, data_dir="../docs/data/nfl"):
    if not data_dir:
        return None
    os.makedirs(data_dir, exist_ok=True)

    now_pt = datetime.now(PACIFIC_TZ)
    slate_date = (
        pd.to_datetime(target_date).strftime("%Y-%m-%d")
        if target_date
        else now_pt.strftime("%Y-%m-%d")
    )

    records = json.loads(results_df.copy().to_json(orient="records"))
    payload = {
        "slate_date": slate_date,
        "updated_at": now_pt.strftime("%Y-%m-%d %I:%M %p %Z"),
        "games": records,
    }

    latest_path = os.path.join(data_dir, "latest.json")
    dated_path = os.path.join(data_dir, f"{slate_date}.json")
    for path in (latest_path, dated_path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    dates = sorted(
        [
            fn[:-5]
            for fn in os.listdir(data_dir)
            if fn.endswith(".json") and fn not in ("latest.json", "dates.json")
        ],
        reverse=True,
    )
    with open(os.path.join(data_dir, "dates.json"), "w", encoding="utf-8") as f:
        json.dump(dates, f, indent=2)

    print(f"Exported {len(records)} NFL games to {latest_path} and {dated_path}.")
    return latest_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run daily NFL spread predictions (upcoming 7-day slate).")
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD). Defaults to today (PT).")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD). Defaults to start + 6 days.")
    parser.add_argument("--output", default="nfl_predictions.csv", help="Output CSV path.")
    parser.add_argument("--json-dir", default="../docs/data/nfl", help="Directory for GitHub Pages JSON output.")
    args = parser.parse_args()

    preds_df = predict_week(
        start_date=args.start_date, end_date=args.end_date, output_csv=args.output
    )
    print(f"\nNFL Predictions ({len(preds_df)} games):")
    if not preds_df.empty:
        print(preds_df.to_string(index=False))
    if args.json_dir:
        export_github_pages_json(preds_df, target_date=args.start_date, data_dir=args.json_dir)
