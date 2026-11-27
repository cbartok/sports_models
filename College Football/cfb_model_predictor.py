import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import cfb_data_layer as cdl
import cfb_model_builder as cmb
from cfb_model_builder import EnsembleModel  # noqa: F401

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


def create_ensemble_fit_table(model_list, features):
    forecast_fit_table = pd.DataFrame()
    for model in model_list:
        forecast_fit_table[f"{model['name']}_FIT"] = model["model"].predict(features)
    return forecast_fit_table


def predict_week(start_date=None, end_date=None, output_csv="cfb_predictions.csv", force_full=None):
    now_pt = datetime.now(PACIFIC_TZ)
    ref_date = pd.Timestamp(now_pt.date()) if start_date is None else pd.to_datetime(start_date)

    if start_date is None:
        week_start = ref_date
        week_end = ref_date + pd.DateOffset(days=6)
    else:
        week_start = pd.to_datetime(start_date)
        week_end = pd.to_datetime(end_date) if end_date else week_start + pd.DateOffset(days=6)

    if force_full is not None:
        use_full = force_full
    else:
        use_full = (ref_date.month > 9) or (ref_date.month == 9 and ref_date.day >= 20) or (ref_date.month <= 2)

    model_file = "cfb_full_model.pickle" if use_full else "cfb_model.pickle"
    feature_cols = cmb.FULL_FEATURE_COLUMNS if use_full else cmb.EARLY_FEATURE_COLUMNS
    print(f"Using {'FULL' if use_full else 'EARLY-SEASON'} model ({model_file}) for {week_start.date()} to {week_end.date()}")

    cfb_model = cmb.import_cfb_model(file_name=model_file)
    cfb_data = cdl.CfbDataLayer()

    data = cfb_data.create_dataframe(week_start, week_end)
    if data.empty:
        print(f"No upcoming FBS games found for {week_start.date()} to {week_end.date()}.")
        empty_df = pd.DataFrame(
            columns=["date", "away_name", "home_name", "neutral", "spread", "prediction", "difference"]
        )
        empty_df.to_csv(output_csv, index=False)
        return empty_df

    model_data = data.copy()
    model_data["neutral"] = np.where(model_data["neutral"] == 1, 1, 0)
    game_features = model_data[feature_cols].to_numpy()

    ensemble_preds = create_ensemble_fit_table(cfb_model.model_list, game_features)
    predictions = np.dot(ensemble_preds, cfb_model.weights)

    results = data[["date", "away_name", "home_name", "neutral", "spread"]].copy()
    results["prediction"] = np.round(predictions, 2)
    results["difference"] = np.round(results["prediction"] - results["spread"], 2)
    results = results.drop_duplicates(subset=["away_name", "home_name"]).sort_values(["date", "away_name"])
    results.to_csv(output_csv, index=False)
    return results


def export_github_pages_json(results_df, target_date=None, data_dir="../docs/data/cfb"):
    """Overwrite latest.json for the GitHub Pages dashboard (no daily archive bloat)."""
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
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Exported {len(records)} CFB games to {latest_path}.")
    return latest_path


def upload_to_google_sheets(results_df, sheet_id=None, sheet_name="CFB Model Predictions", worksheet_name="This Week's Predictions"):
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    key_path = os.path.join(os.path.dirname(os.getcwd()), "google.json")
    if not sa_json and not os.path.exists(key_path):
        print("Skipping Google Sheets upload (no service account credentials found).")
        return False

    import gspread

    client = gspread.service_account_from_dict(json.loads(sa_json)) if sa_json else gspread.service_account(filename=key_path)
    target_sheet_id = sheet_id or os.getenv("CFB_GOOGLE_SHEET_ID")
    spreadsheet = client.open_by_key(target_sheet_id) if target_sheet_id else client.open(sheet_name)

    try:
        ws = spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=worksheet_name, rows=150, cols=10)

    updated_ts = datetime.now(PACIFIC_TZ).strftime("%Y-%m-%d %I:%M %p %Z")
    headers = ["Date", "Away Team", "Home Team", "Neutral Site", "Vegas Spread (+ = Home Fav)", "Model Prediction (+ = Home Fav)", "Edge (Pred - Spread)"]
    clean_df = results_df.copy().fillna("")
    rows = [[f"Last Updated: {updated_ts}", "", "", "", "", "", ""], headers] + clean_df.values.tolist()
    ws.clear()
    ws.update(range_name="A1", values=rows)
    print(f"Uploaded {len(clean_df)} games to Google Sheet '{spreadsheet.title}'.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run daily CFB spread predictions (upcoming 7-day slate).")
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD). Defaults to today (PT).")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD). Defaults to start + 6 days.")
    parser.add_argument("--output", default="cfb_predictions.csv", help="Output CSV path.")
    parser.add_argument("--json-dir", default="../docs/data/cfb", help="Directory for GitHub Pages JSON output.")
    parser.add_argument("--no-sheets", action="store_true", help="Skip Google Sheets upload.")
    args = parser.parse_args()

    preds_df = predict_week(start_date=args.start_date, end_date=args.end_date, output_csv=args.output)
    print(f"\nCFB Predictions ({len(preds_df)} games):")
    if not preds_df.empty:
        print(preds_df.to_string(index=False))
    if args.json_dir:
        export_github_pages_json(preds_df, target_date=args.start_date, data_dir=args.json_dir)
    if not args.no_sheets:
        upload_to_google_sheets(preds_df)
