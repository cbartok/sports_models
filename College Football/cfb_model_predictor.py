import pandas as pd
import numpy as np
import os
import cfb_model_builder as cmb
import cfb_data_layer as cdl

os.chdir('College Football')
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

##Use the model to make predictions for this week's games
predictions = pd.Series(cfb_model.predict(game_features))

##Return the results of the prediction
results = pd.concat([data[['away_name', 'home_name', 'spread']], predictions], axis=1)
results.columns = ['away_name', 'home_name', 'spread', 'prediction']
results['difference'] = results['prediction'] - results['spread']
results.to_csv('cfb_predictions.csv', index=False)



