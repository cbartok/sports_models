import os
import pickle
from collections import namedtuple

import numpy as np
import pandas as pd
import xgboost as xgb
from scipy.special import logsumexp
from scipy.stats import loguniform, randint, uniform
from sklearn import ensemble, linear_model, svm
from sklearn.kernel_approximation import Nystroem
from sklearn.metrics import make_scorer, mean_absolute_error, root_mean_squared_error
from sklearn.model_selection import GroupKFold, RandomizedSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

import cbb_data_layer as cdl

# Must stay at module level so pickled models load cleanly in cbb_model_predictor.py / GUI
EnsembleModel = namedtuple("EnsembleModel", ["weights", "model_list"])

EARLY_FEATURE_COLUMNS = [
    "neutral",
    "away_short_rest",
    "away_long_rest",
    "home_short_rest",
    "home_long_rest",
    "team_rankings_rating",
    "strength_of_schedule",
    "home_ken_pom",
    "away_ken_pom",
]

FEATURE_COLUMNS = [
    "neutral",
    "away_short_rest",
    "away_long_rest",
    "home_short_rest",
    "home_long_rest",
    "team_rankings_rating",
    "strength_of_schedule",
    "home_ken_pom",
    "away_ken_pom",
    "home_efficiency",
    "away_efficiency",
    "home_rebounding",
    "away_rebounding",
    "home_turnover_rate",
    "away_turnover_rate",
    "home_ft_rate",
    "away_ft_rate",
]


def truncated_mae(y_true, y_pred, cap=20.0):
    """
    Truncated MAE loss: caps absolute errors above `cap` points (default 20)
    so garbage-time blowouts don't dominate model selection.
    """
    diff = np.minimum(np.abs(np.asarray(y_true) - np.asarray(y_pred)), cap)
    return float(np.mean(diff))


def fit_bma_weights(val_preds_matrix, y_val, cap=20.0, max_iter=500, tol=1e-7):
    """
    Estimate Bayesian Model Averaging (BMA) ensemble weights via EM
    (Raftery et al., 2005) on out-of-sample validation predictions.
    """
    F = np.asarray(val_preds_matrix, dtype=float)
    y = np.asarray(y_val, dtype=float).reshape(-1, 1)
    n_samples, n_models = F.shape

    residuals = y - F
    if cap is not None:
        residuals = np.clip(residuals, -cap, cap)
    sq_err = residuals ** 2

    weights = np.full(n_models, 1.0 / n_models)
    sigma2 = float(np.mean(sq_err))

    for _ in range(max_iter):
        log_w = np.log(np.maximum(weights, 1e-15))
        log_lik = -0.5 * (np.log(2.0 * np.pi * sigma2) + sq_err / sigma2)
        log_joint = log_lik + log_w
        log_norm = logsumexp(log_joint, axis=1, keepdims=True)
        z = np.exp(log_joint - log_norm)

        new_weights = np.mean(z, axis=0)
        new_weights /= np.sum(new_weights)
        new_sigma2 = float(np.sum(z * sq_err) / n_samples)

        if np.max(np.abs(new_weights - weights)) < tol and abs(new_sigma2 - sigma2) < tol:
            weights, sigma2 = new_weights, new_sigma2
            break
        weights, sigma2 = new_weights, new_sigma2

    return weights, float(np.sqrt(sigma2))


def evaluate_predictions(y_true, y_pred, spreads=None, label=""):
    """Compute Truncated MAE (cap=20), MAE, RMSE, and optional ATS accuracy."""
    t_mae = truncated_mae(y_true, y_pred, cap=20.0)
    mae = mean_absolute_error(y_true, y_pred)
    rmse = root_mean_squared_error(y_true, y_pred)
    metrics = {"model": label, "trunc_mae_20": t_mae, "mae": mae, "rmse": rmse}

    if spreads is not None:
        mask = pd.notna(spreads) & (y_true != spreads) & (y_pred != spreads)
        if np.any(mask):
            pick_home = y_pred[mask] > spreads[mask]
            home_covered = y_true[mask] > spreads[mask]
            metrics["ats_win_pct"] = float(np.mean(pick_home == home_covered) * 100.0)
            metrics["ats_games"] = int(np.sum(mask))
    return metrics


class _CbbUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if name == "EnsembleModel":
            return EnsembleModel
        return super().find_class(module, name)


def import_cbb_model(folder_path=None, file_name="cbb_model.pickle"):
    """Load a saved CBB EnsembleModel pickle from any caller script."""
    path = file_name if folder_path is None else os.path.join(folder_path, file_name)
    with open(path, "rb") as model_file:
        return _CbbUnpickler(model_file).load()


def save_cbb_model(model, folder_path=None, file_name="cbb_model.pickle"):
    """Save a CBB EnsembleModel to pickle."""
    path = file_name if folder_path is None else os.path.join(folder_path, file_name)
    with open(path, "wb") as model_file:
        pickle.dump(model, model_file)
    return path


class CbbModelBuilder:
    """
    Tunes and builds the LASSO + Random Forest + XGBoost + SVR ensemble using:
      1. Season-grouped RandomizedSearchCV (prevents end-of-season rating leakage)
      2. Truncated MAE (capped at 20 pts) as the CV scoring metric
      3. EM-based Bayesian Model Averaging (BMA) on a held-out validation season
    """

    def __init__(
        self,
        data_path="historical_cbb_data.csv",
        feature_columns=None,
        val_year=None,
        test_year=None,
        trunc_cap=20.0,
        random_state=42,
    ):
        self.data_path = data_path
        self.feature_columns = feature_columns or FEATURE_COLUMNS
        self.trunc_cap = trunc_cap
        self.random_state = random_state
        self.scorer = make_scorer(
            truncated_mae, greater_is_better=False, cap=self.trunc_cap
        )

        df = pd.read_csv(self.data_path)
        years = sorted(df["year"].unique())
        if len(years) < 3:
            raise ValueError("Need at least 3 seasons for train / validation / test split.")

        self.test_year = test_year if test_year is not None else years[-1]
        self.val_year = val_year if val_year is not None else years[-2]
        self.train_years = [y for y in years if y < self.val_year]

        self.df = df
        self.train_df = df[df["year"].isin(self.train_years)].reset_index(drop=True)
        self.val_df = df[df["year"] == self.val_year].reset_index(drop=True)
        self.test_df = df[df["year"] == self.test_year].reset_index(drop=True)

        self.X_train = self.train_df[self.feature_columns].to_numpy()
        self.y_train = self.train_df["result"].to_numpy()
        self.groups_train = self.train_df["year"].to_numpy()

        self.X_val = self.val_df[self.feature_columns].to_numpy()
        self.y_val = self.val_df["result"].to_numpy()

        self.X_test = self.test_df[self.feature_columns].to_numpy()
        self.y_test = self.test_df["result"].to_numpy()
        self.close_test = self.test_df["close"].to_numpy() if "close" in self.test_df else None

    def _get_cv(self):
        n_splits = min(len(self.train_years), 5)
        return GroupKFold(n_splits=n_splits)

    def tune_lasso(self, n_iter=25):
        pipe = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("model", linear_model.Lasso(max_iter=5000, random_state=self.random_state)),
            ]
        )
        param_dist = {"model__alpha": loguniform(1e-4, 1.0)}
        search = RandomizedSearchCV(
            pipe,
            param_distributions=param_dist,
            n_iter=n_iter,
            scoring=self.scorer,
            cv=self._get_cv(),
            n_jobs=-1,
            random_state=self.random_state,
        )
        search.fit(self.X_train, self.y_train, groups=self.groups_train)
        return search.best_estimator_, search.best_params_, -search.best_score_

    def tune_random_forest(self, n_iter=8):
        rf = ensemble.RandomForestRegressor(
            random_state=self.random_state, n_jobs=1
        )
        param_dist = {
            "n_estimators": randint(100, 220),
            "max_depth": [6, 8, 10, 12, 16],
            "min_samples_split": randint(5, 40),
            "min_samples_leaf": randint(4, 30),
            "max_features": ["sqrt", "log2"],
        }
        search = RandomizedSearchCV(
            rf,
            param_distributions=param_dist,
            n_iter=n_iter,
            scoring=self.scorer,
            cv=self._get_cv(),
            n_jobs=-1,
            random_state=self.random_state,
        )
        search.fit(self.X_train, self.y_train, groups=self.groups_train)
        return search.best_estimator_, search.best_params_, -search.best_score_

    def tune_xgboost(self, n_iter=20):
        model = xgb.XGBRegressor(
            objective="reg:pseudohubererror",
            tree_method="hist",
            random_state=self.random_state,
            n_jobs=1,
        )
        param_dist = {
            "n_estimators": randint(100, 500),
            "learning_rate": loguniform(0.01, 0.15),
            "max_depth": randint(2, 7),
            "subsample": uniform(0.6, 0.4),
            "colsample_bytree": uniform(0.6, 0.4),
            "min_child_weight": randint(1, 20),
            "reg_alpha": loguniform(1e-3, 10.0),
            "reg_lambda": loguniform(1e-2, 20.0),
            "huber_slope": uniform(8.0, 14.0),
        }
        search = RandomizedSearchCV(
            model,
            param_distributions=param_dist,
            n_iter=n_iter,
            scoring=self.scorer,
            cv=self._get_cv(),
            n_jobs=-1,
            random_state=self.random_state,
        )
        search.fit(self.X_train, self.y_train, groups=self.groups_train)
        return search.best_estimator_, search.best_params_, -search.best_score_

    def tune_svr(self, n_iter=15, exact_rbf=False):
        if exact_rbf:
            pipe = Pipeline(
                [
                    ("scaler", StandardScaler()),
                    ("model", svm.SVR(kernel="rbf", cache_size=1000)),
                ]
            )
            param_dist = {
                "model__C": loguniform(0.2, 15.0),
                "model__epsilon": uniform(0.2, 3.0),
                "model__gamma": ["scale", 0.01, 0.03, 0.05],
            }
        else:
            pipe = Pipeline(
                [
                    ("scaler", StandardScaler()),
                    (
                        "rbf_map",
                        Nystroem(
                            kernel="rbf",
                            n_components=400,
                            random_state=self.random_state,
                        ),
                    ),
                    (
                        "model",
                        svm.LinearSVR(
                            dual="auto",
                            max_iter=5000,
                            random_state=self.random_state,
                        ),
                    ),
                ]
            )
            param_dist = {
                "rbf_map__gamma": loguniform(1e-3, 0.1),
                "model__C": loguniform(0.05, 10.0),
                "model__epsilon": uniform(0.1, 3.0),
            }

        search = RandomizedSearchCV(
            pipe,
            param_distributions=param_dist,
            n_iter=n_iter,
            scoring=self.scorer,
            cv=self._get_cv(),
            n_jobs=-1,
            random_state=self.random_state,
        )
        search.fit(self.X_train, self.y_train, groups=self.groups_train)
        return search.best_estimator_, search.best_params_, -search.best_score_

    def build_ensemble(
        self,
        n_iter_lasso=25,
        n_iter_rf=8,
        n_iter_xgb=20,
        n_iter_svr=15,
        refit_all_data=True,
        save_path="cbb_model.pickle",
    ):
        print(
            f"Temporal split -> Train: {self.train_years} ({len(self.train_df)} games) | "
            f"Val (BMA): [{self.val_year}] ({len(self.val_df)} games) | "
            f"Test: [{self.test_year}] ({len(self.test_df)} games)"
        )

        tuned = []
        for name, fn, n_iter in [
            ("LASSO", self.tune_lasso, n_iter_lasso),
            ("RF", self.tune_random_forest, n_iter_rf),
            ("XGB", self.tune_xgboost, n_iter_xgb),
            ("SVR", self.tune_svr, n_iter_svr),
        ]:
            print(f"Tuning {name} ({n_iter} iterations)...")
            est, params, cv_score = fn(n_iter=n_iter)
            print(f"  {name} best CV TruncMAE(20): {cv_score:.4f} | params={params}")
            tuned.append({"name": name, "model": est, "params": params})

        val_preds = np.column_stack([m["model"].predict(self.X_val) for m in tuned])
        weights, bma_sigma = fit_bma_weights(val_preds, self.y_val, cap=self.trunc_cap)
        print("\nEM-BMA Weights (fitted on validation season", self.val_year, "):")
        for m, w in zip(tuned, weights):
            print(f"  {m['name']:6s}: {w:.4f}")
        print(f"  Estimated BMA sigma (capped): {bma_sigma:.3f}")

        test_preds = np.column_stack([m["model"].predict(self.X_test) for m in tuned])
        ensemble_test_pred = np.dot(test_preds, weights)

        rows = []
        for idx, m in enumerate(tuned):
            rows.append(
                evaluate_predictions(
                    self.y_test, test_preds[:, idx], self.close_test, label=m["name"]
                )
            )
        rows.append(
            evaluate_predictions(
                self.y_test, ensemble_test_pred, self.close_test, label="BMA_ENSEMBLE"
            )
        )
        eval_df = pd.DataFrame(rows)
        print(f"\nHeld-out Test Evaluation (Season {self.test_year}):")
        print(eval_df.to_string(index=False))

        model_list = []
        if refit_all_data:
            X_all = self.df[self.feature_columns].to_numpy()
            y_all = self.df["result"].to_numpy()
            print(f"\nRefitting all 4 tuned models on full dataset ({len(self.df)} games)...")
            for m in tuned:
                m["model"].fit(X_all, y_all)
                model_list.append({"name": m["name"], "model": m["model"]})
        else:
            model_list = [{"name": m["name"], "model": m["model"]} for m in tuned]

        ensemble_model = EnsembleModel(weights=weights, model_list=model_list)
        if save_path:
            save_cbb_model(ensemble_model, file_name=save_path)
            print(f"Saved ensemble model to {save_path}")

        return ensemble_model, eval_df


def build_cbb_clustering_model():
    cbb_data = cdl.CbbDataLayer()
    return cbb_data.create_historical_clustering_dataframe()


if __name__ == "__main__":
    print("=== Building Full CBB Model (17 features) ===")
    CbbModelBuilder(
        data_path="historical_cbb_data.csv", feature_columns=FEATURE_COLUMNS
    ).build_ensemble(save_path="cbb_model.pickle")

    print("\n=== Building Early-Season CBB Model (9 features) ===")
    CbbModelBuilder(
        data_path="historical_cbb_data.csv", feature_columns=EARLY_FEATURE_COLUMNS
    ).build_ensemble(save_path="cbb_early_model.pickle")
