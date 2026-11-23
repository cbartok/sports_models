import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import cbb_data_layer as cdl
import cbb_model_builder as cmb
from cbb_model_builder import EnsembleModel  # noqa: F401 (kept for pickle compatibility)

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


def create_ensemble_fit_table(model_list, features):
    """Create a DataFrame of predictions for all models in the ensemble."""
    forecast_fit_table = pd.DataFrame()
    for model in model_list:
        forecast_fit_table[f"{model['name']}_FIT"] = model["model"].predict(features)
    return forecast_fit_table


def predict_games(target_date=None, model_path="cbb_model.pickle", output_csv="cbb_predictions.csv"):
    """Fetch games + features + current spreads for target_date (Pacific) and run the BMA ensemble."""
    if target_date is None:
        now_pt = datetime.now(PACIFIC_TZ)
        todays_date = pd.Timestamp(now_pt.date())
    else:
        todays_date = pd.to_datetime(target_date)

    date_label = f"{todays_date.strftime('%b')} {todays_date.day}"

    cbb_model = cmb.import_cbb_model(file_name=model_path)
    cbb_data = cdl.CbbDataLayer()

    data = cbb_data.create_dataframe(todays_date, todays_date)
    if data.empty:
        print(f"No upcoming D1 games found for {date_label}.")
        empty_df = pd.DataFrame(
            columns=["date", "away_name", "home_name", "neutral", "spread", "prediction", "difference"]
        )
        empty_df.to_csv(output_csv, index=False)
        return empty_df

    model_data = data.copy()
    model_data["neutral"] = np.where(model_data["neutral"] == 1, 1, 0)
    game_features = model_data[cmb.FEATURE_COLUMNS].to_numpy()

    ensemble_model_predictions = create_ensemble_fit_table(
        cbb_model.model_list, game_features
    )
    predictions = np.dot(ensemble_model_predictions, cbb_model.weights)

    results = data[["away_name", "home_name", "date", "neutral", "spread"]].copy()
    results["prediction"] = np.round(predictions, 2)
    results["difference"] = np.round(results["prediction"] - results["spread"], 2)
    results = results[results["date"] == date_label].drop_duplicates(
        subset=["away_name", "home_name"]
    )
    results = results[
        ["date", "away_name", "home_name", "neutral", "spread", "prediction", "difference"]
    ].sort_values("away_name")

    results.to_csv(output_csv, index=False)
    return results


def upload_to_google_sheets(
    results_df,
    sheet_id=None,
    sheet_name="CBB Model Predictions",
    worksheet_name="Today's Predictions",
):
    """
    Upload predictions DataFrame to a Google Sheet using either:
      - GOOGLE_SERVICE_ACCOUNT_JSON env var (for GitHub Actions), or
      - ../google.json service account file (for local runs).
    """
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    key_path = os.path.join(os.path.dirname(os.getcwd()), "google.json")

    if not sa_json and not os.path.exists(key_path):
        print(
            "Skipping Google Sheets upload (set GOOGLE_SERVICE_ACCOUNT_JSON or place google.json in repo root)."
        )
        return False

    import gspread

    if sa_json:
        client = gspread.service_account_from_dict(json.loads(sa_json))
    else:
        client = gspread.service_account(filename=key_path)

    target_sheet_id = sheet_id or os.getenv("GOOGLE_SHEET_ID")
    spreadsheet = (
        client.open_by_key(target_sheet_id)
        if target_sheet_id
        else client.open(sheet_name)
    )

    try:
        ws = spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=worksheet_name, rows=200, cols=10)

    updated_ts = datetime.now(PACIFIC_TZ).strftime("%Y-%m-%d %I:%M %p %Z")
    headers = [
        "Date",
        "Away Team",
        "Home Team",
        "Neutral Site",
        "Vegas Spread (+ = Home Fav)",
        "Model Prediction (+ = Home Fav)",
        "Edge (Pred - Spread)",
    ]

    clean_df = results_df.copy().fillna("")
    rows = [
        [f"Last Updated: {updated_ts}", "", "", "", "", "", ""],
        headers,
    ] + clean_df.values.tolist()

    ws.clear()
    ws.update(range_name="A1", values=rows)
    print(f"Uploaded {len(clean_df)} games to Google Sheet '{spreadsheet.title}' ({worksheet_name}).")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run daily CBB spread predictions.")
    parser.add_argument("--date", default=None, help="Target date (YYYY-MM-DD). Defaults to today (Pacific).")
    parser.add_argument("--model", default="cbb_model.pickle", help="Path to saved EnsembleModel pickle.")
    parser.add_argument("--output", default="cbb_predictions.csv", help="Output CSV path.")
    parser.add_argument("--sheet-id", default=None, help="Google Sheet ID (or set GOOGLE_SHEET_ID env var).")
    parser.add_argument("--sheet-name", default="CBB Model Predictions", help="Google Sheet title if no ID provided.")
    parser.add_argument("--no-sheets", action="store_true", help="Skip Google Sheets upload.")
    args = parser.parse_args()

    preds_df = predict_games(
        target_date=args.date, model_path=args.model, output_csv=args.output
    )
    print(f"\nPredictions for {args.date or 'today (PT)'} ({len(preds_df)} games):")
    if not preds_df.empty:
        print(preds_df.to_string(index=False))
    if not args.no_sheets:
        upload_to_google_sheets(
            preds_df, sheet_id=args.sheet_id, sheet_name=args.sheet_name
        )
