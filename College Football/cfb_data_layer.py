from sportsreference.ncaaf.teams import Teams
from sportsreference.ncaaf.schedule import Schedule
from sportsreference.ncaaf.boxscore import Boxscore, Boxscores
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


class CfbDataLayer():
    '''
    Class to hold all functions that pull data from outside sources for College Football
    '''   

    def __init__(self):
        self.massey_rating_historical_dates = {2018:'20190107', 2017:'20180108', 2016:'20170109',\
                                           2015:'20160111', 2014:'20150112', 2013:'20140106', 2012:'20130107', 2011:'20120109',
                                           2010:'20110110', 2009:'20100107', 2008:'20090108', 2007:'20080107'}
        self.timeout = 25
        ##Combining all team replace dicts
        ##There are some duplicate key-values but it doesn't really matter
        self.team_replace = {'Bowling Green':'Bowling Green State', 'BYU':'Brigham Young', 'UL-Lafayette':'Louisiana',\
                            'UL-Monroe':'Louisiana-Monroe', 'Miami-FL':'Miami (FL)', 'Miami-OH':'Miami (OH)',\
                            'Middle Tennessee':'Middle Tennessee State', 'NC State':'North Carolina State', \
                            'Pittsburgh':'Pitt', 'Southern Miss':'Southern Mississippi', 'TCU':'Texas Christian', \
                            'Central Florida':'UCF', 'Miami':'Miami (FL)', 'N.C. State':'North Carolina State', 'Miami (Ohio)':'Miami (OH)',\
                            'Bowling Green':'Bowling Green State', 'Miami FL':'Miami (FL)', 'BYU':'Brigham Young', 'UT San Antonio':'UTSA', 'Kent':'Kent State',\
                            'N Illinois':'Northern Illinois', 'C Michigan':'Central Michigan', 'FL Atlantic':'Florida Atlantic', 'Coastal Car':'Coastal Carolina', \
                            'Mississippi':'Ole Miss', 'MTSU':'Middle Tennessee State', 'WKU':'Western Kentucky', 'W Michigan':'Western Michigan', 'Florida Intl':'Florida International',\
                            'E Michigan':'Eastern Michigan', 'ULM':'Louisiana-Monroe', 'TCU':'Texas Christian', 'Ga Southern':'Georgia Southern', 'Miami OH':'Miami (OH)', 'Pittsburgh':'Pitt',\
                            'Southern Miss':'Southern Mississippi', 'NC State':'North Carolina State', \
                            'Pittsburgh U':'Pitt', 'Lafayette':'Louisiana', 'Cincinnati U':'Cincinnati', 'No Illinois':'Northern Illinois', \
                            'Miami Ohio':'Miami (OH)', 'Florida Intl':'Florida International', 'Tex San Antonio':'UTSA', 'NCState':'North Carolina State',\
                            'Buffalo U':'Buffalo', 'ULLafayette':'Louisiana', 'ULMonroe':'Louisiana-Monroe', 'Mississippi St':'Mississippi State',\
                            'Southern Miss':'Southern Mississippi', 'TCU':'Texas Christian', 'BYU':'Brigham Young', 'Washington U':'Washington',\
                            'Miami Florida':'Miami (FL)', 'Bowling Green':'Bowling Green State', 'Central Florida':'UCF', 'Mississippi':'Ole Miss', \
                            'Minnesota U':'Minnesota', 'Middle Tenn St':'Middle Tennessee State', 'Houston U':'Houston', 'Tennessee U':'Tennessee',\
                            'Arizona U':'Arizona', 'Pittsburgh':'Pitt', 'Mid Tennessee State':'Middle Tennessee State', 'Appalachian St':'Appalachian State',\
                            'UL-Monroe':'Louisiana-Monroe', 'Kent':'Kent State', 'So Mississippi':'Southern Mississippi', 'VA Tech':'Virginia Tech', \
                            'App State':'Appalachian State', 'TX-San Ant':'UTSA', 'U Mass':'Massachusetts', 'N Carolina':'North Carolina', 'Central Mich':'Central Michigan',\
                            'S Methodist':'SMU', 'Fla Atlantic':'Florida Atlantic', 'Bowling Grn':'Bowling Green State', 'LA Tech':'Louisiana Tech', 'W Virginia':'West Virginia',\
                            'Middle Tenn':'Middle Tennessee State', 'Wash State':'Washington State', 'W Kentucky':'Western Kentucky', 'Central FL':'UCF', 'N Mex State':'New Mexico State',\
                            'Miss State':'Mississippi State', 'TX El Paso':'UTEP', 'S Alabama':'South Alabama', 'GA Tech':'Georgia Tech', 'LA Monroe':'Louisiana-Monroe', \
                            'TX Christian':'Texas Christian', 'GA Southern':'Georgia Southern', 'S Florida':'South Florida', 'Boston Col':'Boston College', 'E Carolina':'East Carolina',\
                            'S Carolina':'South Carolina', 'S Mississippi':'Southern Mississippi', 'LA Lafayette':'Louisiana', 'Louisiana-Lafayette':'Louisiana', 'Southern Methodist':'SMU'}

    def pull_teams_information(self):
        '''
        Pulls all team for this year
        '''
        ##Try to pull the latest team data
        ##If it fails, use the last year
        try:          
            teams = Teams()
        except:
            teams = Teams(int(pd.to_datetime('now').year) - 1)
        self.all_team_data = teams.dataframes
        self.team_list = list(self.all_team_data['name'].unique())
        self.team_abbreviations = list(self.all_team_data['abbreviation'].unique())

    def pull_historical_team_info(self, year):
        '''
        Pulls all team stats for a given year
        '''
        teams = Teams(year)
        return teams.dataframes

    def create_dataframe(self, start_day, end_day):
        '''
        Compile a complete dataframe for the games for the given time period
        '''
        data = self.pull_schedule(start_day, end_day)

        ##Drop all games that were played already
        data['neutral'] = np.where(data['neutral'] == 1, 1, 0)

        ##Pull and merge advanced stats
        ##Data is pulled from football outsiders
        advanced_stats = self.pull_football_outsiders_stats()

        away_advanced_stats = advanced_stats.copy()
        home_advanced_stats = advanced_stats.copy()

        away_advanced_stats.columns = ['away_' + str(col) for col in away_advanced_stats.columns]
        home_advanced_stats.columns = ['home_' + str(col) for col in home_advanced_stats.columns]

        data = data.merge(away_advanced_stats, how='left')
        data = data.merge(home_advanced_stats, how='left')

        ##Pull and merge massey composite rankings
        massey_rankings = self.pull_massey_composite_rankings()

        away_massey = massey_rankings.copy()
        home_massey = massey_rankings.copy()

        away_massey.columns = ['away_' + str(col) for col in away_massey.columns]
        home_massey.columns = ['home_' + str(col) for col in home_massey.columns]

        data = data.merge(away_massey, how='left')
        data = data.merge(home_massey, how='left')

        ##Pull and merge teamrankings data
        team_rankings_data = self.pull_team_rankings_data()

        away_team_rankings = team_rankings_data.copy()
        home_team_rankings = team_rankings_data.copy()

        away_team_rankings.columns = ['away_' + str(col) for col in away_team_rankings.columns]
        home_team_rankings.columns = ['home_' + str(col) for col in home_team_rankings.columns]

        data = data.merge(away_team_rankings, how='left')
        data = data.merge(home_team_rankings, how='left')

        ##Drop all rows with nans
        ##This is done so we can standardize the stats
        data.dropna(inplace=True)

        ##Standardize all stats columns
        data.iloc[:,10:] = data.iloc[:,10:].apply(pd.to_numeric)
        data.iloc[:,10:] = data.iloc[:,10:].apply(stats.zscore)

        ##Pull odds for the games
        ##Use reverse odds to ensure that neutral site games are included
        odds = self.pull_current_odds(start_day, end_day)
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

    def create_historical_dataframe(self, first_year=2007, last_year=2018):
        '''
        Compiles a complete dataframe of stats to use to train models
        Uses specified years given         
        '''
        full_data = []
        ##Pull the historical data for each specified year
        for year in range(first_year, last_year+1):
            subset_data = self.create_historical_data(year)
            full_data.append(subset_data)

            ##Save after each year is completed
            partial_historical_data = pd.concat(full_data)
            partial_historical_data.to_csv('historical_cfb_data.csv', index=False)

        full_data = pd.concat(full_data)

        ##Fix date format to match odds
        full_data['date'] = full_data['date'].apply(lambda x: x.split(",")[0])

        ##Pull the historical odds
        ##Use reverse odds to ensure that neutral site games are included
        odds = self.pull_historical_odds()
        reversed_odds = pd.DataFrame()
        reversed_odds['home_name'] = odds['away_name']
        reversed_odds['away_name'] = odds['home_name']
        reversed_odds['close'] = -odds['close']
        reversed_odds['year'] = odds['year']
        odds = pd.concat([odds, reversed_odds])

        ##Merge odds in
        full_data = full_data.merge(odds, how='left')

        ##Save full historical data
        full_data.to_csv('historical_cfb_data_full.csv', index=False)

        ##Drop all columns that aren't used for regression and save
        full_data.drop(columns=['away_abbr', 'home_abbr', 'away_name', 'home_name', 'home_score', 'away_score', 'date'], inplace=True) 

        full_data.to_csv('historical_cfb_data.csv', index=False)

        return full_data

    def create_historical_data(self, year):
        '''
        Creates the historical dataframe for the given year
        Each row in the dataframe is a game with many different stats for each of the participating teams that year
        The stats have been standardized for the given year
        '''
        ##Pull the historical schedule and team stats for the given year
        historical_data = self.pull_historical_schedule(year)        

        ##Some locations are nans so set them to be campus games
        historical_data['neutral'] = np.where(historical_data['neutral'] == 1, 1, 0)

        ##Pull and merge historical advanced stats
        ##Data is pulled from football outsiders
        historical_advanced_stats = self.pull_football_outsiders_stats(year)

        away_advanced_stats = historical_advanced_stats.copy()
        home_advanced_stats = historical_advanced_stats.copy()

        away_advanced_stats.columns = ['away_' + str(col) for col in away_advanced_stats.columns]
        home_advanced_stats.columns = ['home_' + str(col) for col in home_advanced_stats.columns]

        historical_data = historical_data.merge(away_advanced_stats, how='left')
        historical_data = historical_data.merge(home_advanced_stats, how='left')

        ##Pull and merge massey composite rankings
        historical_massey_rankings = self.pull_massey_composite_rankings(year)

        away_massey = historical_massey_rankings.copy()
        home_massey = historical_massey_rankings.copy()

        away_massey.columns = ['away_' + str(col) for col in away_massey.columns]
        home_massey.columns = ['home_' + str(col) for col in home_massey.columns]

        historical_data = historical_data.merge(away_massey, how='left')
        historical_data = historical_data.merge(home_massey, how='left')

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

        ##Standardize all stats columns
        historical_data.iloc[:,13:] = historical_data.iloc[:,13:].apply(pd.to_numeric)
        historical_data.iloc[:,13:] = historical_data.iloc[:,13:].apply(stats.zscore)

        ##Get a result column
        ##Home Score - Away Score (negative means away win)
        historical_data['result'] = historical_data['home_score'] - historical_data['away_score']

        ##Drop NaNs in result which means the game was cancelled
        historical_data.dropna(subset=['result'], inplace=True)

        return historical_data

    def pull_schedule(self, start_day, end_day):
        '''
        Pull all games betweena given time period
        '''
        schedule = Boxscores(start_day, end_day).games
        full_schedule = list(schedule.values())
        full_schedule = [game for day in full_schedule for game in day]
        full_schedule = pd.DataFrame(full_schedule)

        ##Clean up data by removing bad data, duplicates, and completed games
        full_schedule = full_schedule[full_schedule['away_abbr'] != full_schedule['home_abbr']]
        full_schedule.drop_duplicates(subset=['boxscore'], inplace=True)
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
                schedules.append(team_schedule[['date', 'team_abbr','boxscore_index', 'neutral', 'days_between_games']])
            except:
                ##In these cases, the team may be new to D1 and was not playing during the season we are pulling
                ##Therefore, we skip this team and continue looping through other teams
                continue

        schedules = pd.concat(schedules)
        schedules.rename(columns={'boxscore_index':'boxscore'}, inplace=True)

        ##Merge location and days_between_games
        full_schedule = full_schedule.merge(schedules[['date', 'boxscore', 'neutral']].drop_duplicates(), how='left')
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'boxscore', 'days_between_games']], how='left',\
                                            left_on=['away_abbr', 'boxscore'], right_on=['team_abbr', 'boxscore'])
        full_schedule.rename(columns={'days_between_games':'away_off_days'}, inplace=True)
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'boxscore', 'days_between_games']], how='left',\
                                            left_on=['home_abbr', 'boxscore'], right_on=['team_abbr', 'boxscore'])
        full_schedule.rename(columns={'days_between_games':'home_off_days'}, inplace=True)

        ##For model-building, create categorical variables for short rest and long rest by team
        ##Short rest is considered less than a full week off
        ##Long rest is considered more than 10 days between games
        full_schedule['away_short_rest'] = np.where(full_schedule['away_off_days'] < 6, 1, 0)
        full_schedule['away_long_rest'] = np.where(full_schedule['away_off_days'] > 10, 1, 0)
        full_schedule['home_short_rest'] = np.where(full_schedule['home_off_days'] < 6, 1, 0)
        full_schedule['home_long_rest'] = np.where(full_schedule['home_off_days'] > 10, 1, 0)

        ##Drop unnecessary columns
        full_schedule.drop(columns=['team_abbr_x', 'team_abbr_y'], inplace=True)

        full_schedule = full_schedule[['away_abbr', 'away_name','home_abbr', 'home_name',\
                                                      'date', 'neutral', 'away_short_rest', 'away_long_rest',\
                                                                                        'home_short_rest', 'home_long_rest']]
        return full_schedule

    def pull_historical_schedule(self, year):
        '''
        Pulls all games from a college football season
        '''
        yearly_schedule = Boxscores(datetime(year, 8, 15), datetime(year+1, 1, 15)).games
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
                team_schedule = Schedule(team_abr, year).dataframe.sort_values(['game'])
                team_schedule['team_abbr'] = team_abr.lower()
                team_schedule['days_between_games'] = team_schedule['datetime'].diff() / np.timedelta64(1, 'D')
                team_schedule['neutral'] = np.where(team_schedule['location'] == 'Neutral', 1, 0)
                schedules.append(team_schedule[['date', 'team_abbr','boxscore_index', 'neutral', 'days_between_games']])
            except:
                ##In these cases, the team may be new to D1 and was not playing during the season we are pulling
                ##Therefore, we skip this team and continue looping through other teams
                continue

        schedules = pd.concat(schedules)
        schedules.rename(columns={'boxscore_index':'boxscore'}, inplace=True)

        ##Merge location and days_between_games
        full_schedule = full_schedule.merge(schedules[['date', 'boxscore', 'neutral']].drop_duplicates(), how='left')
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'boxscore', 'days_between_games']], how='left',\
                                            left_on=['away_abbr', 'boxscore'], right_on=['team_abbr', 'boxscore'])
        full_schedule.rename(columns={'days_between_games':'away_off_days'}, inplace=True)
        full_schedule = full_schedule.merge(schedules[['team_abbr', 'boxscore', 'days_between_games']], how='left',\
                                            left_on=['home_abbr', 'boxscore'], right_on=['team_abbr', 'boxscore'])
        full_schedule.rename(columns={'days_between_games':'home_off_days'}, inplace=True)

        ##For model-building, create categorical variables for short rest and long rest by team
        ##Short rest is considered less than a full week off
        ##Long rest is considered more than 10 days between games
        full_schedule['away_short_rest'] = np.where(full_schedule['away_off_days'] < 6, 1, 0)
        full_schedule['away_long_rest'] = np.where(full_schedule['away_off_days'] > 10, 1, 0)
        full_schedule['home_short_rest'] = np.where(full_schedule['home_off_days'] < 6, 1, 0)
        full_schedule['home_long_rest'] = np.where(full_schedule['home_off_days'] > 10, 1, 0)

        ##Drop unnecessary columns
        full_schedule.drop(columns=['team_abbr_x', 'team_abbr_y'], inplace=True)
        full_schedule = full_schedule[['away_abbr', 'away_name', 'away_score', 'home_abbr', 'home_name',\
                                                     'home_score', 'date', 'year', 'neutral', 'away_short_rest', 'away_long_rest',\
                                                                                        'home_short_rest', 'home_long_rest']]
        return full_schedule

    def pull_football_outsiders_stats(self, year=None):
        '''
        Pulls advanced stats for a given year from football outsiders using beautifulsoup
        I use F+/-, FEI, and S/P+
        Unfortunately, using the webscraping means that I have to hardcode in values to make it work correctly
        '''
        ##Create the urls which we will use to webscrape
        if year is not None:
            f_url = 'https://www.footballoutsiders.com/stats/fplus/' + str(year)
        else:
            f_url = 'https://www.footballoutsiders.com/stats/fplus/'
        f_url = requests.get(f_url).text
        soup = BeautifulSoup(f_url, 'lxml')

        ##Find the table on the page
        ##The most recent table has a different class than past tables
        ##If the first find does not work, we try the second
        f_table = soup.find('table', attrs={'class':'sticky-headers sortable stats'})
        if f_table is None:
            f_table = soup.find('table', attrs={'class':'stats'})

        ##Parse through the rows of the table to create the dataframe
        f_table_rows = f_table.find_all('tr')
        res = []
        for tr in f_table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        historical_fo_stats = pd.DataFrame(res)
        
        ##Different years have different formats
        ##Format the data based on how the data is returned    
        if year is None:
            historical_fo_stats = historical_fo_stats[[0,2,4,6]]
        elif year == 2018:
            historical_fo_stats = historical_fo_stats[[0,3,7,9]]
        elif year == 2014 or year < 2013:
            historical_fo_stats = historical_fo_stats[[1,3,4,6]]
            historical_fo_header = historical_fo_stats.iloc[0]
            historical_fo_stats = historical_fo_stats.iloc[1:]
            historical_fo_stats.columns = historical_fo_header.str.lower()
            historical_fo_stats = historical_fo_stats[historical_fo_stats['team'] != 'Team']
        elif year == 2013:
            historical_fo_stats = historical_fo_stats[[1,3,12,14]]
            historical_fo_header = historical_fo_stats.iloc[0]
            historical_fo_stats = historical_fo_stats.iloc[1:]
            historical_fo_stats.columns = historical_fo_header.str.lower()
            historical_fo_stats = historical_fo_stats[historical_fo_stats['team'] != 'Team']
        else:
            historical_fo_stats = historical_fo_stats[[0,3,5,7]]
            historical_fo_header = historical_fo_stats.iloc[0]
            historical_fo_stats = historical_fo_stats.iloc[1:]
            historical_fo_stats.columns = historical_fo_header.str.lower()
            historical_fo_stats = historical_fo_stats[historical_fo_stats['team'] != 'Team']
        
        historical_fo_stats.columns = ['name', 'f+/-', 's&p+', 'fei']
        ##F+/- is formatted as a percentage string so convert to a decimal
        ##Also convert all of the string numbers to numerics
        historical_fo_stats['f+/-'] = historical_fo_stats['f+/-'].str.rstrip('%').astype('float') / 100.0
        historical_fo_stats.iloc[:,1:] = historical_fo_stats.iloc[:,1:].apply(pd.to_numeric, errors='coerce')

        ##Replace the correct team names to ensure we can merge with the schedule data
        historical_fo_stats['name'].replace(self.team_replace, inplace=True)

        return historical_fo_stats

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
                    url = 'https://www.masseyratings.com/ranks?s=cf&dt=' + self.massey_rating_historical_dates[year]
                else:
                    url = 'https://www.masseyratings.com/ranks?s=cf'

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

        return historical_massey

    def pull_team_rankings_data(self, year=None):
        '''
        Pull and compile all data from teamrankings.com
        '''
        ##Pull team rankings predictive rankings
        team_rankings_data = self.pull_team_rankings_ratings(year)

        ##Pull strength of schedule
        strength_of_schedule = self.pull_strength_of_schedule(year)
        team_rankings_data = team_rankings_data.merge(strength_of_schedule, how='left')

        ##Pull all stats
        points_per_play = self.pull_points_per_play(year)
        team_rankings_data = team_rankings_data.merge(points_per_play, how='left')

        opponent_points_per_play = self.pull_opponent_points_per_play(year)
        team_rankings_data = team_rankings_data.merge(opponent_points_per_play, how='left')

        yards_per_play = self.pull_yards_per_play(year)
        team_rankings_data = team_rankings_data.merge(yards_per_play, how='left')

        opponent_yards_per_play = self.pull_opponent_yards_per_play(year)
        team_rankings_data = team_rankings_data.merge(opponent_yards_per_play, how='left')

        yards_per_rush = self.pull_yards_per_rush(year)
        team_rankings_data = team_rankings_data.merge(yards_per_rush, how='left')

        opponent_yards_per_rush = self.pull_opponent_yards_per_rush(year)
        team_rankings_data = team_rankings_data.merge(opponent_yards_per_rush, how='left')

        yards_per_pass = self.pull_yards_per_pass(year)
        team_rankings_data = team_rankings_data.merge(yards_per_pass, how='left')

        opponent_yards_per_pass = self.pull_opponent_yards_per_pass(year)
        team_rankings_data = team_rankings_data.merge(opponent_yards_per_pass, how='left')

        ##Update the team names to match the base
        team_rankings_data['name'] = team_rankings_data['name'].apply(lambda x: x.replace(' St', ' State') if ' State' not in x else x)
        team_rankings_data['name'].replace(self.team_replace, inplace=True)

        return team_rankings_data


    def pull_team_rankings_ratings(self, year=None):
        '''
        Pull team rankings predicted rankings for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/ranking/predictive-by-other?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/ranking/predictive-by-other'
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
            url = 'https://www.teamrankings.com/college-football/ranking/schedule-strength-by-other?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/ranking/schedule-strength-by-other'
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
        strengh_of_schedule.columns = ['name', 'strength_of_schedule']
        
        ##Get rid of the record in the team name
        strengh_of_schedule['name'] = strengh_of_schedule['name'].apply(lambda x: x[0:x.rfind(' (')])

        return strengh_of_schedule

    def pull_points_per_play(self, year=None):
        '''
        Pull points per play for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/points-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/points-per-play'
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

        points_per_play = pd.DataFrame(res)
        points_per_play = points_per_play[[1,2]]
        points_per_play.columns = ['name', 'points_per_play']

        return points_per_play

    def pull_opponent_points_per_play(self, year=None):
        '''
        Pull opponent points per play for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-points-per-play'
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

        opponent_points_per_play = pd.DataFrame(res)
        opponent_points_per_play = opponent_points_per_play[[1,2]]
        opponent_points_per_play.columns = ['name', 'opponent_points_per_play']

        return opponent_points_per_play

    def pull_yards_per_play(self, year=None):
        '''
        Pull yards per play for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/yards-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/yards-per-play'
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

        yards_per_play = pd.DataFrame(res)
        yards_per_play = yards_per_play[[1,2]]
        yards_per_play.columns = ['name', 'yards_per_play']

        return yards_per_play

    def pull_opponent_yards_per_play(self, year=None):
        '''
        Pull opponent yards per play for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-play'
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

        opponent_yards_per_play = pd.DataFrame(res)
        opponent_yards_per_play = opponent_yards_per_play[[1,2]]
        opponent_yards_per_play.columns = ['name', 'opponent_yards_per_play']

        return opponent_yards_per_play

    def pull_yards_per_rush(self, year=None):
        '''
        Pull yards per rush for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/yards-per-rush-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/yards-per-rush-attempt'
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

        yards_per_rush = pd.DataFrame(res)
        yards_per_rush = yards_per_rush[[1,2]]
        yards_per_rush.columns = ['name', 'yards_per_rush']

        return yards_per_rush

    def pull_opponent_yards_per_rush(self, year=None):
        '''
        Pull opponent yards per rush for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-rush-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-rush-attempt'
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

        opponent_yards_per_rush = pd.DataFrame(res)
        opponent_yards_per_rush = opponent_yards_per_rush[[1,2]]
        opponent_yards_per_rush.columns = ['name', 'opponent_yards_per_rush']

        return opponent_yards_per_rush

    def pull_yards_per_pass(self, year=None):
        '''
        Pull yards per pass for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/yards-per-pass-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/yards-per-pass-attempt'
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

        yards_per_pass = pd.DataFrame(res)
        yards_per_pass = yards_per_pass[[1,2]]
        yards_per_pass.columns = ['name', 'yards_per_pass']

        return yards_per_pass

    def pull_opponent_yards_per_pass(self, year=None):
        '''
        Pull opponent yards per pass for the specified year
        '''
        if year is not None:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-pass-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/college-football/stat/opponent-yards-per-pass-attempt'
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

        opponent_yards_per_pass = pd.DataFrame(res)
        opponent_yards_per_pass = opponent_yards_per_pass[[1,2]]
        opponent_yards_per_pass.columns = ['name', 'opponent_yards_per_pass']

        return opponent_yards_per_pass

    def pull_historical_odds(self, subfolder_name=r'\CFB Historical Odds'):
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
            year = int(re.search('\d{4}', odds_file)[0])

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
            odds_subset['date'] = pd.to_datetime(odds_subset['date'].astype(str).str.zfill(4), format='%m%d').apply(lambda x: x.strftime('%b %d').replace(" 0", " "))
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

        date_range = pd.date_range(start_date, end_date)
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
        url = 'https://classic.sportsbookreview.com/betting-odds/college-football/?date=' + str(games_date.date()).replace('-','')
        raw_data = requests.get(url)
        soup = BeautifulSoup(raw_data.text, 'html.parser')
        if soup.find_all('div', id='OddsGridModule_6'):
            soup = soup.find_all('div', id='OddsGridModule_6')[0]
        else:
            ##No games on this day
            return pd.DataFrame(columns=['away_name', 'home_name', 'spread'])

        daily_odds = self.parse_daily_odds(soup)

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
            away_odds = soup.find_all('div', attrs = {'class':'el-div eventLine-book', 'rel':'238'})[game_number].find_all('div')[0].get_text().strip()
            home_odds = soup.find_all('div', attrs = {'class':'el-div eventLine-book', 'rel':'238'})[game_number].find_all('div')[1].get_text().strip()
            ##Get the spread number only so we only need the away team's odds
            odds = away_odds.replace(u'\xa0',' ').replace(u'\xbd','.5')
            odds = odds[:odds.find(' ')]
            daily_odds.append({'away_name':away_team, 'home_name':home_team, 'spread':odds})

        daily_odds = pd.DataFrame(daily_odds)
        return daily_odds

