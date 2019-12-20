import pandas as pd
import numpy as np
import pickle
import os
from sklearn import linear_model, svm, tree, ensemble
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV, RandomizedSearchCV
from sklearn.metrics import mean_absolute_error, make_scorer
import xgboost as xgb

def import_cbb_model(folder_path=None, file_name='cbb_model.pickle'):
        '''
        Load a saved cbb model
        '''
        if folder_path is None:
            with open(file_name, 'rb') as model_file:
                cbb_model = pickle.load(model_file)
        else:
            with open(os.path.join(folder_path, file_name), 'rb') as model_file:
                cbb_model = pickle.load(model_file)

        return cbb_model
