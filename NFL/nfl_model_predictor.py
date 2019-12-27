import pandas as pd
import numpy as np
import os
import nfl_model_builder as nmb
import nfl_data_layer as ndl

os.chdir('NFL')
todays_date = pd.to_datetime('today')
week = 17

##Import the existing model
##Use the simple model if we are in September
##Otherwise, use full model
if todays_date.month > 9:
    nfl_model = nmb.import_nfl_model(file_name='nfl_full_model.pickle')
    full_model = True
else:
    nfl_model = nmb.import_nfl_model(file_name='nfl_model.pickle')
    full_model = False

##Initialize the year
if 1 <= todays_date.month <= 2:
    year = todays_date.year - 1
else:
    year = todays_date.year

##Initialize the data layer
nfl_data = ndl.NflDataLayer()

##Get the first and last day of this week
##We start a week on Tuesday
todays_date = pd.to_datetime('today')
if todays_date.weekday() == 0:
    ##This is a Monday
    week_start_date = todays_date - pd.DateOffset(6)
else:
    week_start_date = todays_date - pd.DateOffset(days=(todays_date.weekday()-1))
week_end_date = week_start_date + pd.DateOffset(6)

##Create the dataframe for all games this week
##Set the data up to be passed into the model
data = nfl_data.create_dataframe(week, year, week_start_date, week_end_date)
model_data = data.copy()
model_data['neutral'] = np.where(model_data['neutral'] == 1, 1, 0)
model_data = model_data.drop(['spread', 'date', 'home_name', 'away_name'], axis=1)
if not full_model:
    pass
game_features = np.array(model_data)

##Use the model to make predictions for this week's games
predictions = pd.Series(nfl_model.predict(game_features))

##Return the results of the prediction
results = pd.concat([data[['away_name', 'home_name', 'spread']], predictions], axis=1)
results.columns = ['away_name', 'home_name', 'spread', 'prediction']
results['difference'] = results['prediction'] - results['spread']
results.to_csv('nfl_predictions.csv', index=False)
