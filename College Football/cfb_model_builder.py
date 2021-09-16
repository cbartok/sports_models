import pandas as pd
import numpy as np
import pickle
import os
from sklearn import linear_model, svm, tree, ensemble
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV, RandomizedSearchCV
from sklearn.metrics import mean_absolute_error, make_scorer
import xgboost as xgb
from collections import namedtuple

EnsembleModel = namedtuple('EnsembleModel', ['weights', 'model_list'])
def import_cfb_model(folder_path=None, file_name='cfb_model.pickle'):
        '''
        Load a saved cfb model
        '''
        if folder_path is None:
            with open(file_name, 'rb') as model_file:
                cfb_model = pickle.load(model_file)
        else:
            with open(os.path.join(folder_path, file_name), 'rb') as model_file:
                cfb_model = pickle.load(model_file)

        return cfb_model

class CFBModel():
    def __init__(self, training_data, training_labels, test_data, test_labels):
        self.train_data = training_data
        self.train_labels = training_labels
        self.test_data = test_data
        self.test_labels = test_labels

        self.scorer = make_scorer(truncated_loss_function, greater_is_better=False)    

    def truncated_loss_function(y_true, y_pred):
        '''
        Create a truncated loss function to train models
        This deals with outliers more effectively than MSE or MAE
        '''
        diff = np.abs(y_true - y_pred)
        ##If the difference is greater than 2 scores, set it to 16
        diff = np.where(diff > 16, 16, diff)
        return np.mean(diff)

    def linear_regression(self):
        linear_regr = linear_model.LinearRegression().fit(self.train_data, self.train_labels)
        return linear_regr

    def lasso_regression(self, cv=5, random_state=0):
        ##Lasso Regression
        lasso_model = linear_model.LassoCV(cv=cv, random_state=random_state)
        lasso_regr = lasso_model.fit(self.train_data, self.train_labels)
        return lasso_regr

    def ridge_regression(self, cv=5):
        ##Ridge Regression
        ridge_regr = linear_model.RidgeCV(cv=cv).fit(self.train_data, self.train_labels)
        return ridge_regr

    def elastic_net_regression(self, cv=5, random_state=0):
        ##Elastic Net
        en_regr = linear_model.ElasticNetCV(cv=cv, random_state=random_state).fit(self.train_data, self.train_labels)
        return en_regr

    def svr(self, gamma, C, epsilon, kernel):
        ##SVR 
        svr_model = svm.SVR(gamma=gamma, C=C, epsilon=epsilon, kernel=kernel)
        svr_regr = svr_model.fit(self.train_data, self.train_labels)
        return svr_regr

    def random_forest(self, criterion, random_state, max_features,\
                max_depth, min_samples_leaf, min_samples_split, n_estimators):
        ##Random Forest
        random_forest_model = ensemble.RandomForestRegressor(criterion=criterion, random_state=random_state, max_features=max_features,\
                                                                            max_depth=max_depth, min_samples_leaf=min_samples_leaf,\
                                                                        min_samples_split=min_samples_split, n_estimators=n_estimators)
        random_forest_regr = random_forest_model.fit(self.train_data, self.train_labels)
        return random_forest

    def xgboost(self, eval_metric, seed, colsample_bylevel,\
                colsample_bytree, learning_rate, max_depth, reg_alpha, reg_lambda):
        ##xgboost 
        dtrain = xgb.DMatrix(self.train_data, label=self.train_labels)
        dtest = xgb.DMatrix(test_features, label=test_labels)
        xgboost_model = xgb.XGBRegressor(eval_metric=eval_metric, seed=seed, colsample_bylevel=colsample_bylevel,\
                                                    colsample_bytree=colsample_bytree, learning_rate=learning_rate,\
                                                    max_depth=max_depth, reg_alpha=reg_alpha, reg_lambda=reg_lambda)
        xgboost_regr = xgboost_model.fit(self.train_data, self.train_labels)
        return xgboost_regr

    def ensemble_regression(self, model_one, model_one_name, model_two, model_two_name,\
                                  model_three, model_three_name, model_four=None, model_four_name=None,
                                                                model_five=None, model_five_name=None):
        '''
        Creates an ensemble model. Must have at least three models and no more than 5
        '''
        models = [(model_one_name, model_one), (model_two_name, model_two), (model_three_name, model_three)]

        if model_four is not None and model_four_name is not None:
            models += [(model_four_name, model_four)]
        if model_five is not None and model_five_name is not None:
            models += [(model_five_name, model_five)]

        ensemble_model = ensemble.VotingRegressor([('xgb', xgboost_model), ('svr', svr_model), ('rf', random_forest_model), ('lasso', lasso_model)])
        ensemble_regr = ensemble_model.fit(train_features, train_labels)
        return ensemble_regr

    def save_cfb_model(self, model, folder_path=None, file_name='cfb_model.pickle'):
        '''
        Save a model as a pickle
        '''
        if folder_path is None:
            with open(file_name, 'wb') as model_file:
                pickle.dump(model, model_file)
        else:
            with open(os.path.join(folder_path, file_name), 'rb') as model_file:
                pickle.dump(model, model_file)

