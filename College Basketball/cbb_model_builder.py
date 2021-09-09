import pandas as pd
import numpy as np
import pickle
import os
import cbb_data_layer as cdl
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

def build_cbb_clustering_model():
    '''
    Create and save a CBB clustering model.
    This is to cluster teams based on play styles (tempo, fouls, threes, rebounding)
    '''

    ##Initialize the data layer
    cbb_data = cdl.CbbDataLayer()

    training_data = cbb_data.create_historical_clustering_dataframe()
    columns_to_include = ['possessions_per_game', 'three_point_rate', 'opp_three_point_rate', \
                       'foul_rate', 'opp_foul_rate', 'off_rebounding_percentage', 'def_rebounding_percentage']

    return training_data

build_cbb_clustering_model()
