import pandas as pd
import numpy as np
import os
import cbb_model_builder as cmb
import cbb_data_layer as cdl
from collections import namedtuple
import gspread
from oauth2client.service_account import ServiceAccountCredentials

EnsembleModel = namedtuple('EnsembleModel', ['weights', 'model_list'])

def create_ensemble_fit_table(model_list, features):
    '''
    Create a dataframe of predictions for all models in the ensemble
    '''
    forecast_fit_table = pd.DataFrame()
    for model in model_list:
        forecast_fit_table['{}_FIT'.format(model['name'])] = model['model'].predict(features)
    return forecast_fit_table

todays_date = pd.to_datetime('today') + pd.DateOffset(hours=3)

cbb_model = cmb.import_cbb_model(file_name='cbb_model.pickle')

##Initialize the data layer
cbb_data = cdl.CbbDataLayer()

##Create the dataframe for all games this week
##Set the data up to be passed into the model
data = cbb_data.create_dataframe(todays_date, todays_date)
model_data = data.copy()
model_data['neutral'] = np.where(model_data['neutral'] == 1, 1, 0)
model_data = model_data.drop(['spread', 'date', 'home_name', 'away_name'], axis=1)

game_features = np.array(model_data)

##Use the model to make predictions for this week's games
ensemble_model_predictions = create_ensemble_fit_table(cbb_model.model_list, game_features)
predictions = np.dot(ensemble_model_predictions, cbb_model.weights)

##Return the results of the prediction
results = data[['away_name', 'home_name', 'date', 'spread']].copy()
results['prediction'] = predictions
results['difference'] = results['prediction'] - results['spread']
results = results[results['date'] == todays_date.strftime('%b %#d')].drop_duplicates()
results['difference'] = results['prediction'] - results['spread']
results = results[['away_name', 'home_name', 'spread', 'prediction', 'difference']]
results.sort_values(['away_name'], inplace=True)
results.to_csv('cbb_predictions.csv', index=False)

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

key_path = os.path.join(os.path.dirname(os.getcwd()), 'google.json')
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, scope)
client = gspread.authorize(credentials)

spreadsheet = client.open('CBB Model Predictions')
with open('cbb_predictions.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(spreadsheet.id, data=content)
