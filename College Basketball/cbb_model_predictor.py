import pandas as pd
import numpy as np
import os
import cbb_model_builder as cmb
import cbb_data_layer as cdl

os.chdir('College Basketball')
todays_date = pd.to_datetime('today').normalize()

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
predictions = pd.Series(cbb_model.predict(game_features))

##Return the results of the prediction
results = pd.concat([data[['away_name', 'home_name', 'date', 'spread']], predictions], axis=1)
results.columns = ['away_name', 'home_name', 'date', 'spread', 'prediction']
results['difference'] = results['prediction'] - results['spread']
results.to_csv('cbb_predictions.csv', index=False)

