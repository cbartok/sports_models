import pandas as pd
import numpy as np
import os
import cfb_model_builder as cmb
import cfb_data_layer as cdl
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

todays_date = pd.to_datetime('today')

##Import the existing model
##Use the simple model if today is earlier than 9/20
##Otherwise, use full model
if todays_date.month > 9:
    cfb_model = cmb.import_cfb_model(file_name='cfb_full_model.pickle')
    full_model = True
elif todays_date.month == 9 and todays_date.day >= 20:
    cfb_model = cmb.import_cfb_model(file_name='cfb_full_model.pickle')
    full_model = True
else:
    cfb_model = cmb.import_cfb_model(file_name='cfb_model.pickle')
    full_model = False

##Initialize the data layer
cfb_data = cdl.CfbDataLayer()

##Get the first and last day of this week
week_start_date = todays_date - pd.DateOffset(days=todays_date.weekday())
week_end_date = week_start_date + pd.DateOffset(6)

##Create the dataframe for all games this week
##Set the data up to be passed into the model
data = cfb_data.create_dataframe(week_start_date, week_end_date)
model_data = data.copy()
model_data['neutral'] = np.where(model_data['neutral'] == 1, 1, 0)
model_data = model_data.drop(['spread', 'date', 'home_name', 'away_name'], axis=1)
game_features = np.array(model_data)

if not full_model:
    #Remove all columns that are not used in the partial model
    game_features = game_features[:, 0:9]

##Use the model to make predictions for this week's games
ensemble_model_predictions = create_ensemble_fit_table(cfb_model.model_list, game_features)
predictions = np.dot(ensemble_model_predictions, cfb_model.weights)

##Return the results of the prediction
results = data[['away_name', 'home_name', 'spread']].copy()
results['prediction'] = predictions
results['difference'] = results['prediction'] - results['spread']
results.to_csv('cfb_predictions.csv', index=False)

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

key_path = os.path.join(os.path.dirname(os.getcwd()), 'google.json')
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, scope)
client = gspread.authorize(credentials)

spreadsheet = client.open('CFB Model Predictions')
with open('cfb_predictions.csv', 'r') as file_obj:
    content = file_obj.read()
    client.import_csv(spreadsheet.id, data=content)


