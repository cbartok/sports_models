import pandas as pd
import numpy as np
import os
import cbb_model_builder as cmb
import cbb_data_layer as cdl
from collections import namedtuple
from tkinter import *
from tkinter import ttk, messagebox

EnsembleModel = namedtuple('EnsembleModel', ['weights', 'model_list'])
cbb_model = cmb.import_cbb_model(file_name='cbb_model.pickle')

def create_ensemble_fit_table(model_list, features):
    '''
    Create a dataframe of predictions for all models in the ensemble
    '''
    forecast_fit_table = pd.DataFrame()
    for model in model_list:
        forecast_fit_table['{}_FIT'.format(model['name'])] = model['model'].predict(features)
    return forecast_fit_table


##Initialize the data layer
cbb_data = cdl.CbbDataLayer()
cbb_data.pull_teams_information()
team_list = cbb_data.team_list

def check_input(event):
    '''
    Allow users to search the combo box for specific teams
    '''
    value = event.widget.get()
    if value == '':
        event.widget['values'] = team_list
    else:
        data = []
        for item in team_list:
            if value.lower() in item.lower():
                data.append(item)

        event.widget['values'] = data

def run_model():
    '''
    Run the model given the two user-inputted teams
    '''
    away = away_combo_box.get()
    home = home_combo_box.get()
    site = 1 if site_combo_box.get() == 'Vs.' else 0

    #Check whether the teams are valid
    if away not in team_list:
        messagebox.showerror('Invalid Away Team', 'Please Select a Valid Away Team')
    elif home not in team_list:
        messagebox.showerror('Invalid Home Team', 'Please Select a Valid Home Team')

    stats_df = cbb_data.create_single_game_df(away, home, site)
    model_data = stats_df.copy()
    model_data['neutral'] = np.where(model_data['neutral'] == 1, 1, 0)
    model_data = model_data.drop(['home_name', 'away_name'], axis=1)

    game_features = np.array(model_data)
    ##Use the model to make predictions for this week's games
    ensemble_model_predictions = create_ensemble_fit_table(cbb_model.model_list, game_features)
    prediction = np.dot(ensemble_model_predictions, cbb_model.weights)

    #If neutral site, it creates two predictions
    #Get the average of the predictions
    if len(prediction) == 2:
        prediction = (prediction[0] + prediction[1]*-1) / 2
    else:
        prediction = prediction[0]

    if prediction > 0:
        messagebox.showinfo('Model Run Finished', '{} favored by {} over {}.'.format(home, prediction, away))
    else:
        prediction *= -1
        messagebox.showinfo('Model Run Finished', '{} favored by {} over {}.'.format(away, prediction, home))

root = Tk()
root.resizable(True, True)
root.title('College Basketball Model Predictor')

frame = Frame(root)
frame.pack()

#Create Comboboxes
away_combo_box = ttk.Combobox(root)
away_combo_box['values'] = team_list
away_combo_box.bind('<KeyRelease>', check_input)
away_combo_box.pack(side=LEFT)

site_combo_box = ttk.Combobox(root)
site_combo_box['values'] = ['Vs.', 'At']
site_combo_box.current(1)
site_combo_box.pack(side=LEFT)

home_combo_box = ttk.Combobox(root)
home_combo_box['values'] = team_list
home_combo_box.bind('<KeyRelease>', check_input)
home_combo_box.pack(side=LEFT)

run_button = ttk.Button(root, text='Run Model', command=run_model)
run_button.pack(side=BOTTOM)

root.mainloop()


