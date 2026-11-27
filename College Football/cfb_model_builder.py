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

EnsembleModel = namedtuple("EnsembleModel", ["weights", "model_list"])

EARLY_FEATURE_COLUMNS = [
    "neutral", "away_short_rest", "away_long_rest", "home_short_rest", "home_long_rest",
    "f_plus", "fei", "sf_plus", "team_rankings_rating", "strength_of_schedule",
    "home_of_vs_away_df", "away_of_vs_home_df",
]

FULL_FEATURE_COLUMNS = EARLY_FEATURE_COLUMNS + [
    "away_points_per_play", "away_yards_per_play", "away_yards_per_rush", "away_yards_per_pass",
    "home_points_per_play", "home_yards_per_play", "home_yards_per_rush", "home_yards_per_pass",
    "away_sacked_percentage", "home_sacked_percentage", "away_turnovers", "home_turnovers",
]


def truncated_mae(y_true, y_pred, cap=16.0):
    diff = np.minimum(np.abs(np.asarray(y_true) - np.asarray(y_pred)), cap)
    return float(np.mean(diff))


def fit_bma_weights(val_preds_matrix, y_val, cap=16.0, max_iter=500, tol=1e-7):
    F = np.asarray(val_preds_matrix, dtype=float)
    y = np.asarray(y_val, dtype=float).reshape(-1, 1)
    n_samples, n_models = F.shape
    residuals = np.clip(y - F, -cap, cap) if cap is not None else (y - F)
    sq_err = residuals ** 2
    weights = np.full(n_models, 1.0 / n_models)
    sigma2 = float(np.mean(sq_err))

    for _ in range(max_iter):
        log_w = np.log(np.maximum(weights, 1e-15))
        log_lik = -0.5 * (np.log(2.0 * np.pi * sigma2) + sq_err / sigma2)
        log_joint = log_lik + log_w
        z = np.exp(log_joint - logsumexp(log_joint, axis=1, keepdims=True))
        new_weights = np.mean(z, axis=0)
        new_weights /= np.sum(new_weights)
        new_sigma2 = float(np.sum(z * sq_err) / n_samples)
        if np.max(np.abs(new_weights - weights)) < tol and abs(new_sigma2 - sigma2) < tol:
            weights, sigma2 = new_weights, new_sigma2
            break
        weights, sigma2 = new_weights, new_sigma2
    return weights, float(np.sqrt(sigma2))


def evaluate_predictions(y_true, y_pred, spreads=None, label=""):
    metrics = {
        "model": label,
        "trunc_mae_16": truncated_mae(y_true, y_pred, cap=16.0),
        "mae": mean_absolute_error(y_true, y_pred),
        "rmse": root_mean_squared_error(y_true, y_pred),
    }
    if spreads is not None:
        mask = pd.notna(spreads) & (y_true != spreads) & (y_pred != spreads)
        if np.any(mask):
            metrics["ats_win_pct"] = float(np.mean((y_pred[mask] > spreads[mask]) == (y_true[mask] > spreads[mask])) * 100.0)
            metrics["ats_games"] = int(np.sum(mask))
    return metrics


class _CfbUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if name == "EnsembleModel":
            return EnsembleModel
        return super().find_class(module, name)


def import_cfb_model(folder_path=None, file_name="cfb_full_model.pickle"):
    path = file_name if folder_path is None else os.path.join(folder_path, file_name)
    with open(path, "rb") as f:
        return _CfbUnpickler(f).load()


def save_cfb_model(model, folder_path=None, file_name="cfb_full_model.pickle"):
    path = file_name if folder_path is None else os.path.join(folder_path, file_name)
    with open(path, "wb") as f:
        pickle.dump(model, f)
    return path


class CfbModelBuilder:
    def __init__(self, data_path="historical_cfb_data.csv", feature_columns=None, val_year=None, test_year=None, trunc_cap=16.0, random_state=42):
        self.data_path = data_path
        self.feature_columns = feature_columns or FULL_FEATURE_COLUMNS
        self.trunc_cap = trunc_cap
        self.random_state = random_state
        self.scorer = make_scorer(truncated_mae, greater_is_better=False, cap=self.trunc_cap)

        df = pd.read_csv(self.data_path)
        years = sorted(df["year"].unique())
        self.test_year = test_year if test_year is not None else years[-1]
        self.val_year = val_year if val_year is not None else years[-2]
        self.train_years = [y for y in years if y < self.val_year]

        self.df = df
        self.train_df = df[df["year"].isin(self.train_years)].reset_index(drop=True)
        self.val_df = df[df["year"] == self.val_year].reset_index(drop=True)
        self.test_df = df[df["year"] == self.test_year].reset_index(drop=True)

        self.X_train, self.y_train = self.train_df[self.feature_columns].to_numpy(), self.train_df["result"].to_numpy()
        self.groups_train = self.train_df["year"].to_numpy()
        self.X_val, self.y_val = self.val_df[self.feature_columns].to_numpy(), self.val_df["result"].to_numpy()
        self.X_test, self.y_test = self.test_df[self.feature_columns].to_numpy(), self.test_df["result"].to_numpy()
        self.close_test = self.test_df["close"].to_numpy() if "close" in self.test_df else None

    def _get_cv(self):
        return GroupKFold(n_splits=min(len(self.train_years), 5))

    def tune_lasso(self, n_iter=25):
        pipe = Pipeline([("scaler", StandardScaler()), ("model", linear_model.Lasso(max_iter=5000, random_state=self.random_state))])
        s = RandomizedSearchCV(pipe, {"model__alpha": loguniform(1e-4, 2.0)}, n_iter=n_iter, scoring=self.scorer, cv=self._get_cv(), n_jobs=-1, random_state=self.random_state)
        s.fit(self.X_train, self.y_train, groups=self.groups_train)
        return s.best_estimator_, s.best_params_, -s.best_score_

    def tune_random_forest(self, n_iter=10):
        rf = ensemble.RandomForestRegressor(random_state=self.random_state, n_jobs=1)
        params = {"n_estimators": randint(100, 260), "max_depth": [6, 8, 10, 12, 16], "min_samples_split": randint(5, 35), "min_samples_leaf": randint(4, 25), "max_features": ["sqrt", "log2"]}
        s = RandomizedSearchCV(rf, params, n_iter=n_iter, scoring=self.scorer, cv=self._get_cv(), n_jobs=-1, random_state=self.random_state)
        s.fit(self.X_train, self.y_train, groups=self.groups_train)
        return s.best_estimator_, s.best_params_, -s.best_score_

    def tune_xgboost(self, n_iter=20):
        model = xgb.XGBRegressor(objective="reg:pseudohubererror", tree_method="hist", random_state=self.random_state, n_jobs=1)
        params = {"n_estimators": randint(100, 500), "learning_rate": loguniform(0.01, 0.15), "max_depth": randint(2, 6), "subsample": uniform(0.6, 0.4), "colsample_bytree": uniform(0.6, 0.4), "min_child_weight": randint(2, 20), "reg_alpha": loguniform(1e-3, 10.0), "reg_lambda": loguniform(1e-2, 20.0), "huber_slope": uniform(8.0, 14.0)}
        s = RandomizedSearchCV(model, params, n_iter=n_iter, scoring=self.scorer, cv=self._get_cv(), n_jobs=-1, random_state=self.random_state)
        s.fit(self.X_train, self.y_train, groups=self.groups_train)
        return s.best_estimator_, s.best_params_, -s.best_score_

    def tune_svr(self, n_iter=15):
        pipe = Pipeline([("scaler", StandardScaler()), ("rbf_map", Nystroem(kernel="rbf", n_components=400, random_state=self.random_state)), ("model", svm.LinearSVR(dual="auto", max_iter=5000, random_state=self.random_state))])
        params = {"rbf_map__gamma": loguniform(1e-3, 0.1), "model__C": loguniform(0.05, 10.0), "model__epsilon": uniform(0.1, 3.0)}
        s = RandomizedSearchCV(pipe, params, n_iter=n_iter, scoring=self.scorer, cv=self._get_cv(), n_jobs=-1, random_state=self.random_state)
        s.fit(self.X_train, self.y_train, groups=self.groups_train)
        return s.best_estimator_, s.best_params_, -s.best_score_

    def build_ensemble(self, n_iter_lasso=25, n_iter_rf=10, n_iter_xgb=20, n_iter_svr=15, refit_all_data=True, save_path="cfb_full_model.pickle"):
        tuned = []
        for name, fn, n_iter in [("LASSO", self.tune_lasso, n_iter_lasso), ("RF", self.tune_random_forest, n_iter_rf), ("XGB", self.tune_xgboost, n_iter_xgb), ("SVR", self.tune_svr, n_iter_svr)]:
            print(f"Tuning {name} ({n_iter} iterations)...")
            est, params, cv_score = fn(n_iter=n_iter)
            print(f"  {name} best CV TruncMAE(16): {cv_score:.4f}")
            tuned.append({"name": name, "model": est, "params": params})

        val_preds = np.column_stack([m["model"].predict(self.X_val) for m in tuned])
        weights, bma_sigma = fit_bma_weights(val_preds, self.y_val, cap=self.trunc_cap)
        test_preds = np.column_stack([m["model"].predict(self.X_test) for m in tuned])
        rows = [evaluate_predictions(self.y_test, test_preds[:, i], self.close_test, label=m["name"]) for i, m in enumerate(tuned)]
        rows.append(evaluate_predictions(self.y_test, np.dot(test_preds, weights), self.close_test, label="BMA_ENSEMBLE"))
        print(pd.DataFrame(rows).to_string(index=False))

        model_list = []
        if refit_all_data:
            X_all, y_all = self.df[self.feature_columns].to_numpy(), self.df["result"].to_numpy()
            for m in tuned:
                m["model"].fit(X_all, y_all)
                model_list.append({"name": m["name"], "model": m["model"]})
        else:
            model_list = [{"name": m["name"], "model": m["model"]} for m in tuned]

        ens = EnsembleModel(weights=weights, model_list=model_list)
        if save_path:
            save_cfb_model(ens, file_name=save_path)
            print(f"Saved ensemble model to {save_path}")
        return ens


if __name__ == "__main__":
    CfbModelBuilder("historical_cfb_data.csv", FULL_FEATURE_COLUMNS).build_ensemble(save_path="cfb_full_model.pickle")
    CfbModelBuilder("historical_cfb_data.csv", EARLY_FEATURE_COLUMNS).build_ensemble(save_path="cfb_model.pickle")
