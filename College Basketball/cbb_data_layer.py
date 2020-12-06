from sportsreference.ncaab.teams import Teams
from sportsreference.ncaab.schedule import Schedule
from sportsreference.ncaab.boxscore import Boxscore, Boxscores
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
import requests
import os
import re
import time
import sys
import re

from bs4 import BeautifulSoup
from selenium import webdriver
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE


class CbbDataLayer():
    '''
    Class to hold all the functions that pull data for College Basketball
    '''

    def __init__(self):
        self.team_replace = {'Missouri KC':'UMKC', 'CS Fullerton':'Cal State Fullerton', 'Gardner Webb':'Gardner-Webb', 'SC Upstate':'USC Upstate',\
                            'TX Southern':'Texas Southern', 'Loyola MD':'Loyola (MD)', 'SIUE':'SIU-Edwardsville', 'F Dickinson':'Fairleigh Dickinson',\
                            'Cent Arkansas':'Central Arkansas', 'American Univ':'American', 'Ark Pine Bluff':'Arkansas-Pine Bluff', 'Bowling Green':'Bowling Green State',\
                            'UC Irvine':'UC-Irvine', 'UC Riverside':'UC-Riverside', 'Central Conn':'Central Connecticut', 'Charleston So':'Charleston Southern',\
                            'E Kentucky':'Eastern Kentucky', 'FL Atlantic':'Florida Atlantic', 'Ga Southern':'Georgia Southern', 'North Carolina':'UNC', \
                            'Miami FL':'Miami (FL)', 'Monmouth NJ':'Monmouth', 'S Dakota State':'South Dakota State', 'SE Louisiana':'Southeastern Louisiana',\
                            'Connecticut':'UConn', 'N Kentucky':'Northern Kentucky', 'Florida Intl':'Florida International', 'NC A&T':'North Carolina A&T',\
                            'Abilene Chr':'Abilene Christian', 'G Washington':'George Washington', 'SUNY Albany':'Albany (NY)', 'UAB':'Alabama-Birmingham',\
                            'CS Northridge':'Cal State Northridge', 'Mississippi':'Ole Miss', 'Massachusetts':'UMass', 'SE Missouri State':'Southeast Missouri State',\
                            'WKU':'Western Kentucky', "St John's":"St. John's (NY)", 'St Bonaventure':'St. Bonaventure', 'UTRGV':'Texas-Rio Grande Valley',\
                            'IL Chicago':'UIC', 'Boston Univ':'Boston University', 'TAM C. Christi':'Texas A&M-Corpus Christi', 'Coastal Car':'Coastal Carolina',\
                            'St Louis':'Saint Louis', 'Col Charleston':'College of Charleston', 'C Michigan':'Central Michigan', 'E Michigan':'Eastern Michigan',\
                            'MTSU':'Middle Tennessee', 'MD E Shore':'Maryland-Eastern Shore', 'E Washington':'Eastern Washington', 'CS Bakersfield':'Cal State Bakersfield',\
                            'WI Milwaukee':'Milwaukee', 'Loyola-Chicago':'Loyola (IL)', "St Mary's CA":"Saint Mary's", 'FL Gulf Coast':'Florida Gulf Coast', 'UT Arlington':'Texas-Arlington',\
                            'UC Santa Barbara':'UCSB', 'Ark Little Rock':'Little Rock', 'W Illinois':'Western Illinois', 'W Michigan':'Western Michigan', 'Kent':'Kent State',\
                            'PFW':'Purdue-Fort Wayne', 'UT San Antonio':'UTSA', 'Northwestern LA':'Northwestern State', 'S Dakota St':'South Dakota State', 'MA Lowell':'UMass-Lowell',\
                            'Pittsburgh':'Pitt', 'Houston Bap':'Houston Baptist', 'N Illinois':'Northern Illinois', 'CS Sacramento':'Sacramento State', 'Miami OH':'Miami (OH)',\
                            'Loy Marymount':'Loyola Marymount', 'W Carolina':'Western Carolina', 'LIU Brooklyn':'LIU', 'TN Martin':'UT-Martin', 'SF Austin':'Stephen F. Austin',\
                            'Kennesaw':'Kennesaw State', 'ULM':'Louisiana-Monroe', 'WI Green Bay':'Green Bay', 'NC Central':'North Carolina Central',\
                            'St Francis NY':'St. Francis (NY)', 'St Francis PA':'Saint Francis (PA)', "St Peter's":"St. Peter's", 'S Carolina State':'South Carolina State',\
                            'N Colorado':'Northern Colorado', "Mt State Mary's":"Mount St. Mary's", 'N Dakota State':'North Dakota State', 'Southern Univ':'Southern',\
                            "St Joseph's PA":"St. Joseph's", 'NE Omaha':'Omaha', 'MS Valley State':'Mississippi Valley State', 'UC Davis':'UC-Davis', 'S Illinois':'Southern Illinois',\
                            'E Illinois':'Eastern Illinois', 'North Carolina':'UNC', 'FIU':'Florida International', 'Albany':'Albany (NY)', 'Bethune Cookman':'Bethune-Cookman',\
                            "St. John's":"St. John's (NY)", 'UT Rio Grande Valley':'Texas-Rio Grande Valley', 'Illinois Chicago':'UIC', 'The Citadel':'Citadel',\
                            'Texas A&M Corpus Chris':'Texas A&M-Corpus Christi', 'Maryland Eastern Shore':'Maryland-Eastern Shore', 'N.C. State':'NC State',\
                            'Loyola Chicago':'Loyola (IL)', 'Fort Wayne':'Purdue-Fort Wayne', 'UMass Lowell':'UMass-Lowell', 'Arkansas Pine Bluff':'Arkansas-Pine Bluff',\
                            'Louisiana Lafayette':'Louisiana', 'Tennessee Martin':'UT-Martin', 'SIU Edwardsville':'SIU-Edwardsville', 'Louisiana Monroe':'Louisiana-Monroe',\
                            "Mount State Mary's":"Mount St. Mary's", "Saint Joseph's":"St. Joseph's", "St. Francis PA":"Saint Francis (PA)", "St. Francis NY":"St. Francis (NY)",\
                            "Saint Peter's":"St. Peter's", 'East Tennessee State':'ETSU', 'Prairie View A&M':'Prairie View', 'Nebraska Omaha':'Omaha', 'Grambling State':'Grambling',\
                            'N Carolina':'UNC', 'Abl Christian':'Abilene Christian', 'W Virginia':'West Virginia', 'Miss State':'Mississippi State', 'James Mad':'James Madison',\
                            'VA Tech':'Virginia Tech', 'N Mex State':'New Mexico State', 'Grd Canyon':'Grand Canyon', 'S Florida':'South Florida', 'Sacred Hrt':'Sacred Heart',\
                            'Geo Wshgtn':'George Washington', 'Beth-Cook':'Bethune-Cookman', 'Geo Mason':'George Mason', 'Cal State Nrdge':'Cal State Northridge',\
                            'San Fransco':'San Francisco', 'TX El Paso':'UTEP', 'TX Christian':'TCU', 'Boston Col':'Boston College', 'S Methodist':'SMU',\
                            'U Mass':'UMass', 'Central FL':'UCF', 'TN Tech':'Tennessee Tech', 'SE Missouri':'Southeast Missouri State', 'W Kentucky':'Western Kentucky',\
                            'St Johns':"St. John's (NY)", 'St Bonavent':'St. Bonaventure', 'TX-Pan Am':'Texas-Rio Grande Valley', 'IL-Chicago':'UIC', 'Boston U':'Boston University',\
                            'TX A&M-CC':'Texas A&M-Corpus Christi', 'Col Charlestn':'College of Charleston', 'Central Mich':'Central Michigan', 'S Mississippi':'Southern Miss',\
                            'Incar Word':'Incarnate Word', 'Jksnville State':'Jacksonville State', 'LA Tech':'Louisiana Tech', 'Middle Tenn':'Middle Tennessee', 'Maryland ES':'Maryland-Eastern Shore',\
                            'E Washingtn':'Eastern Washington', 'N Hampshire':'New Hampshire', 'CS Bakersfld':'Cal State Bakersfield', 'WI-Milwkee':'Milwaukee', 'Loyola-Chi':'Loyola (IL)',\
                            'St Marys':"Saint Mary's", 'Fla Gulf Cst':'Florida Gulf Coast', 'TX-Arlington':'Texas-Arlington', 'NC-Wilmgton':'UNC Wilmington', 'AR Lit Rock':'Little Rock',\
                            'Sam Hous State':'Sam Houston State', 'App State':'Appalachian State', 'E Carolina':'East Carolina', 'IPFW':'Purdue-Fort Wayne', 'TX-San Ant':'UTSA',\
                            'NW State':'Northwestern State', 'Mass Lowell':'UMass-Lowell', 'Wash State':'Washington State', 'Ark Pine Bl':'Arkansas-Pine Bluff',\
                            'Central Ark':'Central Arkansas', 'Northeastrn':'Northeastern', 'N Iowa':'Northern Iowa', 'GA Southern':'Georgia Southern', 'N Arizona':'Northern Arizona',\
                            'U Penn':'Penn', 'NC-Asheville':'UNC Asheville', 'S Alabama':'South Alabama', 'TN State':'Tennessee State', 'Bowling Grn':'Bowling Green State',\
                            'S Carolina':'South Carolina', 'Youngs State':'Youngstown State', 'LA Lafayette':'Louisiana', 'Sac State':'Sacramento State', \
                            'Lg Beach State':'Long Beach State', 'Loyola Mymt':'Loyola Marymount', 'Charl South':'Charleston Southern', 'Wm & Mary':'William & Mary',\
                            'LIU-Brooklyn':'LIU', 'NC-Grnsboro':'UNC Greensboro', 'Ste F Austin':'Stephen F. Austin', 'SIU Edward':'SIU-Edwardsville', 'Maryland BC':'UMBC',\
                            'Fla Atlantic':'Florida Atlantic', 'S Utah':'Southern Utah', 'LA Monroe':'Louisiana-Monroe', 'WI-Grn Bay':'Green Bay', 'St Fran (NY)':"St. Francis (NY)",\
                            'St Fran (PA)':"Saint Francis (PA)", 'Utah Val State':'Utah Valley', 'Gard-Webb':'Gardner-Webb', 'N Florida':'North Florida', \
                            'Loyola-MD':'Loyola (MD)', 'VA Military':'VMI', 'Mt State Marys':"Mount St. Mary's", 'GA Tech':'Georgia Tech', 'E Tenn State':'ETSU',\
                            'Rob Morris':'Robert Morris', 'Neb Omaha':'Omaha', 'St Josephs':"St. Joseph's", 'St Peters':"St. Peter's", 'S Car State':'South Carolina State',\
                            'Alab A&M':'Alabama A&M', 'Miss Val State':'Mississippi Valley State', 'CSFullerton':'Cal State Fullerton', 'Miami Florida':'Miami (FL)',\
                            'USCUpstate':'USC Upstate', 'Tennessee-Martin':'UT-Martin', 'Central Connecticut State':'Central Connecticut', 'Illinois-Chicago':'UIC',\
                            'Louisiana-Lafayette':'Louisiana', 'Texas A&M-CC':'Texas A&M-Corpus Christi', 'North Carolina State':'NC State', 'Arkansas-Little Rock':'Little Rock',\
                            'Detroit Mercy':'Detroit', 'Prairie View A&M':'Prairie View', "Saint Joseph's (PA)":"St. Joseph's", 'Purdue Fort Wayne':'Purdue-Fort Wayne', 'Charleston':'College of Charleston',\
                            'Kansas City':'UMKC', 'Nebraska-Omaha':'Omaha'}
        self.massey_rating_historical_dates = {2019:'20190408', 2018:'20180402', 2017:'20170403',\
                                           2016:'20160404', 2015:'20150406', 2014:'20140407', 2013:'20130408', 2012:'20120402',
                                           2011:'20110404', 2010:'20100405', 2009:'20090406', 2008:'20080407'}

    def pull_teams_information(self):
        '''
        Pulls all team for this year
        '''
        ##Try to pull the latest team data
        ##If it fails, use the last year
        try:          
            teams = Teams()
        except:
            teams = Teams(int(pd.to_datetime('now').year) -  1)
        self.all_team_data = teams.dataframes
        self.team_list = list(self.all_team_data['name'].unique())
        self.team_abbreviations = list(self.all_team_data['abbreviation'].unique())

    def pull_historical_team_info(self, year):
        '''
        Pulls all team stats for a given year
        '''
        teams = Teams(year)
        return teams.dataframes

    def create_dataframe(self, start_date, end_date):
        data = self.pull_schedule(start_date, end_date)

        ##Some locations are nans so set them to be campus games
        data['neutral'] = np.where(data['neutral'] == 1, 1, 0)

        ##Pull historical massey
        historical_massey_rankings = self.pull_massey_composite_rankings()

        away_massey = historical_massey_rankings.copy()
        home_massey = historical_massey_rankings.copy()

        away_massey.columns = ['away_' + str(col) for col in away_massey.columns]
        home_massey.columns = ['home_' + str(col) for col in home_massey.columns]

        data = data.merge(away_massey, how='left')
        data = data.merge(home_massey, how='left')

        ##Pull Ken Pom data
        historical_ken_pom = self.pull_ken_pom_data()

        away_ken_pom = historical_ken_pom.copy()
        home_ken_pom= historical_ken_pom.copy()

        away_ken_pom.columns = ['away_' + str(col) for col in away_ken_pom.columns]
        home_ken_pom.columns = ['home_' + str(col) for col in home_ken_pom.columns]

        data = data.merge(away_ken_pom, how='left')
        data = data.merge(home_ken_pom, how='left')

        ##Pull and merge teamrankings data
        historical_team_rankings_data = self.pull_team_rankings_data()

        away_team_rankings = historical_team_rankings_data.copy()
        home_team_rankings = historical_team_rankings_data.copy()

        away_team_rankings.columns = ['away_' + str(col) for col in away_team_rankings.columns]
        home_team_rankings.columns = ['home_' + str(col) for col in home_team_rankings.columns]

        data = data.merge(away_team_rankings, how='left')
        data = data.merge(home_team_rankings, how='left')

        ##Drop all rows with nans
        ##This is done so we can standardize the stats
        data.dropna(inplace=True)

        ##Standardize the numeric data and update in terms of strengths of both teams
        data = self.create_opponent_adjusted_stats(data, 10)

        ##Pull odds for the games
        ##Use reverse odds to ensure that neutral site games are included
        odds = self.pull_current_odds(start_date, end_date)
        reversed_odds = pd.DataFrame()
        reversed_odds['home_name'] = odds['away_name']
        reversed_odds['away_name'] = odds['home_name']
        reversed_odds['spread'] = -odds['spread']
        odds = pd.concat([odds, reversed_odds])

        ##Merge odds in
        data = data.merge(odds, how='left')

        ##Drop unneccessary columns
        data.drop(columns=['away_abbr', 'home_abbr'], inplace=True) 

        return data

    def pull_schedule(self, start_date, end_date):
        '''
        Pull all games for a given time period
        '''
        schedule = Boxscores(start_date, end_date).games
        full_schedule = list(schedule.values())
        full_schedule = [game for day in full_schedule for game in day]
        full_schedule = pd.DataFrame(full_schedule)

        #Clean up data by removing bad data, duplicates, and completed games
        full_schedule = full_schedule[full_schedule['away_abbr'] != full_schedule['home_abbr']]        
        full_schedule = full_schedule[pd.isnull(full_schedule['winning_abbr'])]

        schedules = []
        team_list = list(set(full_schedule['away_abbr']).union(set(full_schedule['home_abbr'])))
        ##Pull individual schedules for other factors that are not in the boxscore
        for team_abr in team_list:
            try:
                team_schedule = Schedule(team_abr).dataframe.sort_values(['game'])
                team_schedule['team_abbr'] = team_abr.lower()
                team_schedule['days_between_games'] = team_schedule['datetime'].diff() / np.timedelta64(1, 'D')
                team_schedule['neutral'] = np.where(team_schedule['location'] == 'Neutral', 1, 0)
                team_schedule = team_schedule[(team_schedule['datetime'] >= start_date) & (team_schedule['datetime'] < end_date+pd.DateOffset(1))]
                schedules.append(team_schedule[['date', 'team_abbr','boxscore_index', 'neutral', 'days_between_games']])
            except:
                ##In these cases, the team may be new to D1 and was not playing during the season we are pulling
                ##Therefore, we skip this team and continue looping through other teams
                continue

        schedules = pd.concat(schedules)
        schedules.rename(columns={'boxscore_index':'boxscore'}, inplace=True)

        ##Merge location and days_between_games
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'neutral', 'date']].drop_duplicates(), how='left', left_on=['away_abbr'], right_on=['team_abbr'])
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'date', 'days_between_games']], how='left',\
                                            left_on=['away_abbr', 'date'], right_on=['team_abbr', 'date'])
        full_schedule.rename(columns={'days_between_games':'away_off_days'}, inplace=True)
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'date', 'days_between_games']], how='left',\
                                            left_on=['home_abbr', 'date'], right_on=['team_abbr', 'date'])
        full_schedule.rename(columns={'days_between_games':'home_off_days'}, inplace=True)

        ##For model-building, create categorical variables for short rest and long rest by team
        ##Short rest is considered playing back to back days
        ##Long rest is considered a week or more between games
        full_schedule['away_short_rest'] = np.where(full_schedule['away_off_days'] == 1, 1, 0)
        full_schedule['away_long_rest'] = np.where(full_schedule['away_off_days'] > 6, 1, 0)
        full_schedule['home_short_rest'] = np.where(full_schedule['home_off_days'] == 1, 1, 0)
        full_schedule['home_long_rest'] = np.where(full_schedule['home_off_days'] > 6, 1, 0)

        full_schedule.drop(columns=['team_abbr_x', 'team_abbr_y'], inplace=True)

        ##Drop games with no date
        full_schedule.dropna(subset=['date'], inplace=True)

        ##Fix date format to match odds
        full_schedule['date'] = full_schedule['date'].apply(lambda x: x.split(",")[1].strip())

        full_schedule = full_schedule[['away_abbr', 'away_name', 'home_abbr', 'home_name',\
                                                     'date', 'neutral', 'away_short_rest', 'away_long_rest',\
                                                                                        'home_short_rest', 'home_long_rest']]
        return full_schedule

    def pull_historical_schedule(self, year):
        '''
        Pulls all games from a college basketball season
        '''
        yearly_schedule = Boxscores(datetime(year-1, 11, 1), datetime(year, 4, 15)).games
        full_schedule = list(yearly_schedule.values())
        full_schedule = [game for day in full_schedule for game in day]
        full_schedule = pd.DataFrame(full_schedule)
        full_schedule['year'] = year

        ##Clean up data by removing bad data and duplicates
        full_schedule = full_schedule[full_schedule['away_abbr'] != full_schedule['home_abbr']]
        full_schedule.drop_duplicates(subset=['boxscore'], inplace=True)

        schedules = []
        team_list = list(set(full_schedule['away_abbr']).union(set(full_schedule['home_abbr'])))
        ##Pull individual schedules for other factors that are not in the boxscore
        for team_abr in team_list:
            try:
                team_schedule = Schedule(team_abr, year).dataframe.sort_values(['datetime'])
                team_schedule['team_abbr'] = team_abr.lower()
                team_schedule['days_between_games'] = team_schedule['datetime'].diff() / np.timedelta64(1, 'D')
                team_schedule['neutral'] = np.where(team_schedule['location'] == 'Neutral', 1, 0)
                schedules.append(team_schedule[['date', 'team_abbr','boxscore_index', 'neutral', 'days_between_games']])
            except:
                ##In these cases, the team may be new to D1 and was not playing during the season we are pulling
                ##Therefore, we skip this team and continue looping through other teams
                continue

        schedules = pd.concat(schedules).reset_index(drop=True)
        schedules.rename(columns={'boxscore_index':'boxscore'}, inplace=True)

        ##Merge location and days_between_games
        full_schedule = full_schedule.merge(schedules[['boxscore', 'neutral', 'date']].drop_duplicates(), how='left', left_on=['boxscore'], right_on=['boxscore'])
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'boxscore', 'days_between_games']], how='left',\
                                            left_on=['away_abbr', 'boxscore'], right_on=['team_abbr', 'boxscore'])
        full_schedule.rename(columns={'days_between_games':'away_off_days'}, inplace=True)
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'boxscore', 'days_between_games']], how='left',\
                                            left_on=['home_abbr', 'boxscore'], right_on=['team_abbr', 'boxscore'])
        full_schedule.rename(columns={'days_between_games':'home_off_days'}, inplace=True)

        ##For model-building, create categorical variables for short rest and long rest by team
        ##Short rest is considered playing back to back days
        ##Long rest is considered a week or more between games
        full_schedule['away_short_rest'] = np.where(full_schedule['away_off_days'] == 1, 1, 0)
        full_schedule['away_long_rest'] = np.where(full_schedule['away_off_days'] > 6, 1, 0)
        full_schedule['home_short_rest'] = np.where(full_schedule['home_off_days'] == 1, 1, 0)
        full_schedule['home_long_rest'] = np.where(full_schedule['home_off_days'] > 6, 1, 0)

        full_schedule.drop(columns=['team_abbr_x', 'team_abbr_y'], inplace=True)

        ##Drop games with no date
        full_schedule.dropna(subset=['date'], inplace=True)

        ##Fix date format to match odds
        full_schedule['date'] = full_schedule['date'].apply(lambda x: x.split(",")[1].strip())

        full_schedule = full_schedule[['away_abbr', 'away_name', 'away_score', 'home_abbr', 'home_name',\
                                                     'home_score', 'date', 'year', 'neutral', 'away_short_rest', 'away_long_rest',\
                                                                                        'home_short_rest', 'home_long_rest']]
        return full_schedule

    def create_historical_data(self, year):

        historical_data = self.pull_historical_schedule(year)

        ##Some locations are nans so set them to be campus games
        historical_data['neutral'] = np.where(historical_data['neutral'] == 1, 1, 0)

        ##Pull historical massey
        historical_massey_rankings = self.pull_massey_composite_rankings(year)

        away_massey = historical_massey_rankings.copy()
        home_massey = historical_massey_rankings.copy()

        away_massey.columns = ['away_' + str(col) for col in away_massey.columns]
        home_massey.columns = ['home_' + str(col) for col in home_massey.columns]

        historical_data = historical_data.merge(away_massey, how='left')
        historical_data = historical_data.merge(home_massey, how='left')

        ##Pull Ken Pom data
        historical_ken_pom = self.pull_ken_pom_data(year)

        away_ken_pom = historical_ken_pom.copy()
        home_ken_pom= historical_ken_pom.copy()

        away_ken_pom.columns = ['away_' + str(col) for col in away_ken_pom.columns]
        home_ken_pom.columns = ['home_' + str(col) for col in home_ken_pom.columns]

        historical_data = historical_data.merge(away_ken_pom, how='left')
        historical_data = historical_data.merge(home_ken_pom, how='left')

        ##Pull and merge teamrankings data
        historical_team_rankings_data = self.pull_team_rankings_data(year)

        away_team_rankings = historical_team_rankings_data.copy()
        home_team_rankings = historical_team_rankings_data.copy()

        away_team_rankings.columns = ['away_' + str(col) for col in away_team_rankings.columns]
        home_team_rankings.columns = ['home_' + str(col) for col in home_team_rankings.columns]

        historical_data = historical_data.merge(away_team_rankings, how='left')
        historical_data = historical_data.merge(home_team_rankings, how='left')

        ##Drop all rows with nans
        ##This is done so we can standardize the stats
        historical_data.dropna(inplace=True)

        ##Standardize the numeric data and update in terms of strengths of both teams
        historical_data = self.create_opponent_adjusted_stats(historical_data, 13)

        ##Get a result column
        ##Home Score - Away Score (negative means away win)
        historical_data['result'] = historical_data['home_score'] - historical_data['away_score']

        ##Drop NaNs in result which means the game was cancelled
        historical_data.dropna(subset=['result'], inplace=True)

        return historical_data

    def create_historical_dataframe(self, first_year=2015, last_year=2019):
        '''
        Compiles a complete dataframe of stats to use to train models
        Uses specified years given        
        Note: 2015 is the first available year
        '''
        full_data = []
        ##Pull the historical data for each specified year
        for year in range(first_year, last_year+1):
            subset_data = self.create_historical_data(year)
            full_data.append(subset_data)

            ##Save after each year is completed
            partial_historical_data = pd.concat(full_data)
            partial_historical_data.to_csv('historical_cbb_data.csv', index=False)

        full_data = pd.concat(full_data)

        ##Pull in historical odds
        odds = self.pull_historical_odds()
        reversed_odds = pd.DataFrame()
        reversed_odds['home_name'] = odds['away_name']
        reversed_odds['away_name'] = odds['home_name']
        reversed_odds['close'] = -odds['close']
        reversed_odds['year'] = odds['year']
        reversed_odds['date'] = odds['date']
        odds = pd.concat([odds, reversed_odds])
        full_data = full_data.merge(odds, how='left')

        ##Save full historical data
        full_data.to_csv('historical_cbb_data_full.csv', index=False)

        ##Drop all columns that aren't used for regression and save
        full_data.drop(columns=['away_abbr', 'home_abbr', 'away_name', 'home_name', 'home_score', 'away_score', 'date'], inplace=True) 

        full_data.to_csv('historical_cbb_data.csv', index=False)

        return full_data

    def create_opponent_adjusted_stats(self, data, index_start):
        '''
        Update the dataframe to reflect differences in team level for each stat
        First, normalize all columns after a certain index        
        Then, calculate the difference between the two teams with the opposing stats.

        All of these comparitive stats will be in terms of the home team
        '''
        
        ##First ensure that all columns are numeric
        data.iloc[:,index_start:] = data.iloc[:,index_start:].apply(pd.to_numeric)        

        ##Create a new dataframe to hold the opponent-adjusted stats
        updated_data = data.iloc[:,0:index_start].copy()

        ##Create a column for the opponent adjusted stats from the original dataframe
        ##There might be a way to automate this in the future but do it manually for now
        ##If lower is an indicator of better perfomance, we subtract it from 1
        ##i.e. defensive stats
        ##Lower rank indicates a better team
        updated_data['average_ranking'] = -data['home_average_ranking'] + data['away_average_ranking']
        updated_data['team_rankings_rating'] = data['home_team_rankings_rating'] - data['away_team_rankings_rating']
        updated_data['strength_of_schedule'] = data['home_strength_of_schedule'] - data['away_strength_of_schedule']

        updated_data['home_ken_pom'] = data['home_ken_pom_offense'] + data['away_ken_pom_defense']
        updated_data['away_ken_pom'] = -data['away_ken_pom_offense'] - data['home_ken_pom_defense']

        updated_data['home_efficiency'] = data['home_offensive_efficiency'] + data['away_defensive_efficiency']
        updated_data['away_efficiency'] = -data['away_offensive_efficiency'] - data['home_defensive_efficiency']
        updated_data['home_rebounding'] = data['home_def_rebounding_percentage'] - data['away_off_rebounding_percentage']
        updated_data['away_rebounding'] = -data['away_def_rebounding_percentage'] + data['home_off_rebounding_percentage']
        updated_data['home_turnover_rate'] = -data['home_to_percentage'] - data['away_opp_to_percentage']
        updated_data['away_turnover_rate'] = data['away_to_percentage'] + data['home_opp_to_percentage']
        updated_data['home_ft_rate'] = data['home_ft_rate'] + data['away_opp_ft_rate']
        updated_data['away_ft_rate'] = -data['away_ft_rate'] - data['away_opp_ft_rate']

        return updated_data
        
    def pull_massey_composite_rankings(self, year=None):
        '''
        Pull the historical Massey composite rankings for the given year
        These rankings are the average of the model rankings from online
        '''

        ##Due to JavaScript, we need to use selenium to pull this data        
        tries = 0
        run_loop = True
        while run_loop:
            ##Wrap this in a try-except to keep trying until it works
            ##Give up after 5 tries
            try:
                browser = webdriver.Chrome(executable_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'chromedriver'))        
                if year is not None:
                    url = 'https://www.masseyratings.com/ranks?s=cb&dt=' + self.massey_rating_historical_dates[year]
                else:
                    url = 'https://www.masseyratings.com/ranks?s=cb'

                browser.get(url)
                time.sleep(15)
                page_source = browser.page_source
                soup = BeautifulSoup(page_source, 'lxml')

                table = soup.find('table', attrs={'class':'mytable'})
                ##Parse through the rows of the table to create the dataframe
                table_rows = table.find_all('tr')
                run_loop = False
            except:
                tries += 1
                print('failed on try {}'.format(tries))
                browser.quit()
                if tries >= 5:
                    sys.exit("Massey load failed")
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                row = [row[i] for i in [0, -2]]
                res.append(row)        
        
        historical_massey = pd.DataFrame(res)
        historical_massey.columns = ['name', 'average_ranking']
        browser.quit()

        
        ##Update team names to match the schedule names
        historical_massey['name'] = historical_massey['name'].apply(lambda x: x.replace(' St', ' State') if ' State' not in x else x)
        historical_massey['name'].replace(self.team_replace, inplace=True)
        historical_massey = historical_massey[historical_massey['name'] != 'Correlation']

        ##Normalize the data
        historical_massey.iloc[:,1:] = historical_massey.iloc[:,1:].apply(pd.to_numeric) 
        historical_massey.iloc[:,1:] = (historical_massey.iloc[:,1:] - np.mean(historical_massey.iloc[:,1:], axis=0))/(np.std(historical_massey.iloc[:,1:], axis=0))

        return historical_massey

    def pull_ken_pom_data(self, year=None):
        '''
        Pull KenPom Data for the given year
        '''
        ##Create the urls which we will use to webscrape
        if year is not None:
            f_url = 'https://kenpom.com/index.php?y=' + str(year)
        else:
            f_url = 'https://kenpom.com/'
        f_url = requests.get(f_url).text
        soup = BeautifulSoup(f_url, 'lxml')

        ##Find the table on the page
        f_table = soup.find('table',id='ratings-table')

        ##Parse through the rows of the table to create the dataframe
        f_table_rows = f_table.find_all('tr')
        res = []
        for tr in f_table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        ken_pom_stats = pd.DataFrame(res)

        ken_pom_stats = ken_pom_stats[[1, 5, 7, 9]]
        ken_pom_stats.columns = ['name', 'ken_pom_offense', 'ken_pom_defense', 'tempo']

        ##Clean the data
        ken_pom_stats['name'] = ken_pom_stats['name'].apply(lambda x: x.rstrip('0123456789 '))

        ##Update the team names to match the base
        ken_pom_stats['name'] = ken_pom_stats['name'].apply(lambda x: x.replace(' St.', ' State'))
        ken_pom_stats['name'].replace(self.team_replace, inplace=True)

        ##Normalize the data
        ken_pom_stats.iloc[:,1:] = ken_pom_stats.iloc[:,1:].apply(pd.to_numeric) 
        ken_pom_stats.iloc[:,1:] = (ken_pom_stats.iloc[:,1:] - np.mean(ken_pom_stats.iloc[:,1:], axis=0))/(np.std(ken_pom_stats.iloc[:,1:], axis=0))

        return ken_pom_stats

    def pull_team_rankings_data(self, year=None):
        '''
        Pull and compile all data from teamrankings.com
        '''
        ##Pull team rankings predictive rankings
        team_rankings_data = self.pull_team_rankings_ratings(year)

        ##Pull strength of schedule
        strength_of_schedule = self.pull_strength_of_schedule(year)
        team_rankings_data = team_rankings_data.merge(strength_of_schedule, how='left')

        ##Pull efficiency for and against
        off_eff = self.pull_offensive_efficiency(year)
        team_rankings_data = team_rankings_data.merge(off_eff, how='left')
        def_eff = self.pull_defensive_efficiency(year)
        team_rankings_data = team_rankings_data.merge(def_eff, how='left')

        ##Pull turnover rate for and against
        to_rate = self.pull_turnovers_per_possession(year)
        team_rankings_data = team_rankings_data.merge(to_rate, how='left')
        opp_to_rate = self.pull_opp_turnovers_per_possession(year)
        team_rankings_data = team_rankings_data.merge(opp_to_rate, how='left')

        ##Pull offensive and defensive rebounding numbers
        def_reb = self.pull_defensive_rebounding_percentage(year)
        team_rankings_data = team_rankings_data.merge(def_reb, how='left')
        off_reb = self.pull_offensive_rebounding_percentage(year)
        team_rankings_data = team_rankings_data.merge(off_reb, how='left')

        ##Pull free throw rate for and against
        ft_rate = self.pull_ft_rate(year)
        team_rankings_data = team_rankings_data.merge(ft_rate, how='left')
        opp_ft_rate = self.pull_opp_ft_rate(year)
        team_rankings_data = team_rankings_data.merge(opp_ft_rate, how='left')

        ##Update the team names to match the base
        team_rankings_data['name'] = team_rankings_data['name'].apply(lambda x: x.replace(' St', ' State') if ' State' not in x else x)
        team_rankings_data['name'].replace(self.team_replace, inplace=True)

        ##Normalize the data
        team_rankings_data.iloc[:,1:] = team_rankings_data.iloc[:,1:].apply(pd.to_numeric) 
        team_rankings_data.iloc[:,1:] = (team_rankings_data.iloc[:,1:] - np.mean(team_rankings_data.iloc[:,1:], axis=0))/(np.std(team_rankings_data.iloc[:,1:], axis=0))

        return team_rankings_data

    def pull_team_rankings_ratings(self, year=None):
        '''
        Pull team rankings predicted rankings for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/ranking/predictive-by-other?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/ranking/predictive-by-other'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        team_rankings = pd.DataFrame(res)
        team_rankings = team_rankings[[1,2]]
        team_rankings.columns = ['name', 'team_rankings_rating']
        
        ##Get rid of the record in the team name
        team_rankings['name'] = team_rankings['name'].apply(lambda x: x[0:x.rfind(' (')])

        return team_rankings

    def pull_strength_of_schedule(self, year=None):
        '''
        Pull strength of schedule for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/ranking/schedule-strength-by-other?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/ranking/schedule-strength-by-other'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        strengh_of_schedule = pd.DataFrame(res)
        strengh_of_schedule = strengh_of_schedule[[1,2]]
        strengh_of_schedule = strengh_of_schedule[strengh_of_schedule[2] != '--']
        strengh_of_schedule.columns = ['name', 'strength_of_schedule']
        
        ##Get rid of the record in the team name
        strengh_of_schedule['name'] = strengh_of_schedule['name'].apply(lambda x: x[0:x.rfind(' (')])

        return strengh_of_schedule

    def pull_offensive_efficiency(self, year=None):
        '''
        Pull offensive efficiency percentage for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/offensive-efficiency?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/offensive-efficiency'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        efficiency = pd.DataFrame(res)
        efficiency = efficiency[[1,2]]
        efficiency = efficiency[efficiency[2] != '--']
        efficiency.columns = ['name', 'offensive_efficiency']
        
        ##Convert string to float
        efficiency['offensive_efficiency'] = efficiency['offensive_efficiency'].astype('float')

        return efficiency

    def pull_defensive_efficiency(self, year=None):
        '''
        Pull defensive efficiency for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/defensive-efficiency?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/defensive-efficiency'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        efficiency = pd.DataFrame(res)
        efficiency = efficiency[[1,2]]
        efficiency = efficiency[efficiency[2] != '--']
        efficiency.columns = ['name', 'defensive_efficiency']
        
        ##Convert string to float
        efficiency['defensive_efficiency'] = efficiency['defensive_efficiency'].astype('float')

        return efficiency

    def pull_turnovers_per_possession(self, year=None):
        '''
        Pull turnovers per possession for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/turnovers-per-possession?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/turnovers-per-possession'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        to_percentage = pd.DataFrame(res)
        to_percentage = to_percentage[[1,2]]
        to_percentage = to_percentage[to_percentage[2] != '--']
        to_percentage.columns = ['name', 'to_percentage']
        
        ##Convert string to float
        to_percentage['to_percentage'] = to_percentage['to_percentage'].str.rstrip('%').astype('float') / 100.0

        return to_percentage

    def pull_opp_turnovers_per_possession(self, year=None):
        '''
        Pull opponent turnovers per possession for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/opponent-turnovers-per-possession?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/opponent-turnovers-per-possession'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        to_percentage = pd.DataFrame(res)
        to_percentage = to_percentage[[1,2]]
        to_percentage = to_percentage[to_percentage[2] != '--']
        to_percentage.columns = ['name', 'opp_to_percentage']
        
        ##Convert string to float
        to_percentage['opp_to_percentage'] = to_percentage['opp_to_percentage'].str.rstrip('%').astype('float') / 100.0

        return to_percentage

    def pull_defensive_rebounding_percentage(self, year=None):
        '''
        Pull defensive rebounding percentage for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/defensive-rebounding-pct?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/defensive-rebounding-pct'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        rebounding = pd.DataFrame(res)
        rebounding = rebounding[[1,2]]
        rebounding = rebounding[rebounding[2] != '--']
        rebounding.columns = ['name', 'def_rebounding_percentage']
        
        ##Convert string to float
        rebounding['def_rebounding_percentage'] = rebounding['def_rebounding_percentage'].str.rstrip('%').astype('float') / 100.0

        return rebounding

    def pull_offensive_rebounding_percentage(self, year=None):
        '''
        Pull offensive rebounding percentage for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/offensive-rebounding-pct?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/offensive-rebounding-pct'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        rebounding = pd.DataFrame(res)
        rebounding = rebounding[[1,2]]
        rebounding = rebounding[rebounding[2] != '--']
        rebounding.columns = ['name', 'off_rebounding_percentage']
        
        ##Convert string to float
        rebounding['off_rebounding_percentage'] = rebounding['off_rebounding_percentage'].str.rstrip('%').astype('float') / 100.0

        return rebounding

    def pull_ft_rate(self, year=None):
        '''
        Pull free throw rate for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/free-throw-rate?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/free-throw-rate'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        ft_rate = pd.DataFrame(res)
        ft_rate = ft_rate[[1,2]]
        ft_rate = ft_rate[ft_rate[2] != '--']
        ft_rate.columns = ['name', 'ft_rate']
        
        ##Convert string to float
        ft_rate['ft_rate'] = ft_rate['ft_rate'].str.rstrip('%').astype('float') / 100.0

        return ft_rate

    def pull_opp_ft_rate(self, year=None):
        '''
        Pull opponent free throw rate for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/opponent-free-throw-rate?date=' + str(year)+ '-05-01'
        else:
            url = 'https://www.teamrankings.com/ncaa-basketball/stat/opponent-free-throw-rate'
        url = requests.get(url).text
        soup = BeautifulSoup(url, 'lxml')

        table = soup.find('table', attrs={'class':'tr-table datatable scrollable'})

        ##Parse through the rows of the table to create the dataframe
        table_rows = table.find_all('tr')
        res = []
        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        ft_rate = pd.DataFrame(res)
        ft_rate = ft_rate[[1,2]]
        ft_rate = ft_rate[ft_rate[2] != '--']
        ft_rate.columns = ['name', 'opp_ft_rate']
        
        ##Convert string to float
        ft_rate['opp_ft_rate'] = ft_rate['opp_ft_rate'].str.rstrip('%').astype('float') / 100.0

        return ft_rate

    def pull_historical_odds(self, subfolder_name=r'\CBB Historical Odds'):
        '''
        Compile historical closing odds from excel files from sportsbookreviewsonline
        The historical odds were stored as xlsx files
        Note: subfolder name containing the xlsx files must be passed in
        '''

        ##Navigate to the sub folder and get list all files
        odds_path = os.getcwd() + subfolder_name
        odds_files = pd.Series(os.listdir(odds_path))

        odds = []
        for odds_file in odds_files:
            ##Get the year from the odds_file name
            year = int(re.search('\d{4}', odds_file)[0]) + 1

            ##Read in the odds file
            raw_odds = pd.read_excel(os.path.join(odds_path, odds_file))
            raw_odds.columns = raw_odds.columns.str.lower()

            ##Ensure that the closing odds are numeric
            ##Also change pick-em lines to 0
            raw_odds['close'] = np.where(raw_odds['close'] == 'pk', 0, raw_odds['close'])
            raw_odds['close'] = pd.to_numeric(raw_odds['close'], errors='coerce')

            ##Split the team name by capital letters
            ##i.e. TexasState to Texas State
            raw_odds['team'] = raw_odds['team'].apply(lambda x: re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', str(x)))

            ##Visitors have even numbered indices and home have odd number
            visitors = raw_odds[::2].copy().reset_index(drop=True)
            home = raw_odds[1::2].copy().reset_index(drop=True)
            visitors.rename(columns={'team':'away_name', 'close':'away_close'}, inplace=True)
            home.rename(columns={'team':'home_name', 'close':'home_close'}, inplace=True)
            visitors = visitors[['date', 'away_name', 'away_close']]
            home = home[['home_name', 'home_close']]

            ##Merge the games into one dataframe
            odds_subset = visitors.merge(home, left_index=True, right_index=True)
            odds_subset.dropna(inplace=True)

            ##The smaller closing line is the spread rather than the O/U
            ##If the away team is favored, negate the line to match with normal format
            odds_subset['close'] = np.where(odds_subset['away_close'] <= odds_subset['home_close'], -odds_subset['away_close'], odds_subset['home_close'])
            odds_subset = odds_subset[['away_name', 'home_name', 'close', 'date']]
            odds_subset['year'] = year
            odds_subset['date'] = str(year) + odds_subset['date'].astype(str).str.zfill(4)
            odds_subset['date'] = pd.to_datetime(odds_subset['date']).apply(lambda x: x.strftime('%b %d').replace(" 0", " "))
            odds.append(odds_subset)

        odds = pd.concat(odds)

        ##Update team names so we can merge the odds into the main dataframe
        odds['away_name'].replace(self.team_replace, inplace=True)
        odds['home_name'].replace(self.team_replace, inplace=True)

        return odds

    def pull_current_odds(self, start_date, end_date):
        '''
        Pull all odds for a given timeframe
        '''

        ##We can only pull for future games
        ##Therefore, we change the start date to today if the start_date is in the past
        if start_date < pd.to_datetime('today'):
            start_date = pd.to_datetime('today')

        date_range = pd.date_range(start_date, end_date+pd.DateOffset(1))
        full_odds = []
        for day in date_range:
            daily_odds = self.pull_current_daily_odds(day)
            full_odds.append(daily_odds)

        full_odds = pd.concat(full_odds)
        
        ##Remove ranks from the team names
        full_odds['home_name'] = full_odds['home_name'].apply(lambda x: re.sub('^\\([\\d]+\\)\\xa0', '', x))
        full_odds['away_name'] = full_odds['away_name'].apply(lambda x: re.sub('^\\([\\d]+\\)\\xa0', '', x))

        ##Update team names
        full_odds['home_name'].replace(self.team_replace, inplace=True)
        full_odds['away_name'].replace(self.team_replace, inplace=True)

        return full_odds

    def pull_current_daily_odds(self, games_date):
        '''
        Pull all odds for the given date from sportsbook review
        '''
        url = 'https://classic.sportsbookreview.com/betting-odds/ncaa-basketball/?date=' + str(games_date.date()).replace('-','')
        raw_data = requests.get(url)
        soup = BeautifulSoup(raw_data.text, 'html.parser')
        if soup.find_all('div', id='OddsGridModule_14'):
            soup = soup.find_all('div', id='OddsGridModule_14')[0]
        else:
            ##No games on this day
            return pd.DataFrame(columns=['away_name', 'home_name', 'spread'])

        daily_odds = self.parse_daily_odds(soup)

        ##Replace pick-em with 0s
        daily_odds['spread'] = np.where(daily_odds['spread'].str.contains('PK'), '0', daily_odds['spread'])

        ##Remove the + from the spread column and convert strings to numerics
        daily_odds['spread'] = pd.to_numeric(daily_odds['spread'].map(lambda x: x.strip('+')))

        return daily_odds
        

    def parse_daily_odds(self, soup):
        '''
        Take the raw odds and convert to dataframe
        '''
        ##Initialize the dataframe
        daily_odds = []
        number_of_games = len(soup.find_all('div', attrs = {'class':'el-div eventLine-rotation'}))
        for game_number in range(number_of_games):
            away_team = soup.find_all('div', attrs = {'class':'el-div eventLine-team'})[game_number].find_all('div')[0].get_text().strip()
            home_team = soup.find_all('div', attrs = {'class':'el-div eventLine-team'})[game_number].find_all('div')[1].get_text().strip()
            ##Pinnacle is Book ID 238
            pinnacle_odds = soup.find_all('div', attrs = {'class':'el-div eventLine-book', 'rel':'238'})[game_number].find_all('div')[0].get_text().strip()
            five_dimes_odds = soup.find_all('div', attrs = {'class':'el-div eventLine-book', 'rel':'19'})[game_number].find_all('div')[0].get_text().strip()
            ##Get the spread number only so we only need the away team's odds
            odds = pinnacle_odds.replace(u'\xa0',' ').replace(u'\xbd','.5')
            odds = odds[:odds.find(' ')]
            secondary_odds = five_dimes_odds.replace(u'\xa0',' ').replace(u'\xbd','.5')
            secondary_odds = secondary_odds[:secondary_odds.find(' ')]
            daily_odds.append({'away_name':away_team, 'home_name':home_team, 'spread':odds, 'spread2':secondary_odds})

        daily_odds = pd.DataFrame(daily_odds)
        ##Fill in missing spread with secondary spread
        daily_odds['spread'] = np.where(daily_odds['spread'] == '', daily_odds['spread2'], daily_odds['spread'])
        daily_odds.drop('spread2', axis=1, inplace=True)
        return daily_odds
