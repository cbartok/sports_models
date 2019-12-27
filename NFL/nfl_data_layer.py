from sportsreference.nfl.teams import Teams
from sportsreference.nfl.schedule import Schedule
from sportsreference.nfl.boxscore import Boxscore, Boxscores
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
import requests
import os
import re
from bs4 import BeautifulSoup

class NflDataLayer():
    '''
    Class to hold all functions that pull data from outside sources for College Football
    '''

    def __init__(self):
        self.team_replace = {'LARams': 'Los Angeles Rams', 'LAChargers': 'Los Angeles Chargers',\
                        'NYGiants':'New York Giants', 'NYJets':'New York Jets', 'St.Louis':'St. Louis', 'Los Angeles': 'Los Angeles Rams',
                        'Buffalo Bills': 'Buffalo', 'New York': 'New York Giants', 'Houston Texans':'Houston', 'ne':'nwe', 'no':'nor',\
                        'ind':'clt', 'bal':'rav', 'lac':'sdg', 'ari':'crd', 'gb':'gnb',\
                        'oak':'rai', 'ten':'oti', 'tb':'tam', 'sf':'sfo', 'lar':'ram', 'hou':'htx', 'kc':'kan', 'stl':'ram', 'sd':'sdg',
                        'jac':'jax', 'larm':'ram', 'lach':'sdg', 'LA Chargers':'Los Angeles Chargers', 'LA Rams':'Los Angeles Rams',\
                        'L.A. Chargers':'Los Angeles Chargers', 'L.A. Rams':'Los Angeles Rams'}

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

    def create_dataframe(self, week, year, start_day, end_day):
        '''
        Compile a complete dataframe for the games for the given time period
        '''
        data = self.pull_schedule(week, year)

        ##Keep only the city name for the name
        ##Except for cities with multiple teams
        data['away_name'] = data['away_name'].apply(lambda x: x.rsplit(' ', 1)[0] if x.rsplit(' ', 1)[0] not in ['Los Angeles', 'New York'] else x)
        data['home_name'] = data['home_name'].apply(lambda x: x.rsplit(' ', 1)[0] if x.rsplit(' ', 1)[0] not in ['Los Angeles', 'New York'] else x)

        ##Pull and merge advanced stats
        ##Data is pulled from football outsiders
        advanced_stats = self.pull_football_outsiders_stats()

        away_advanced_stats = advanced_stats.copy()
        home_advanced_stats = advanced_stats.copy()

        away_advanced_stats.columns = ['away_' + str(col) for col in away_advanced_stats.columns]
        home_advanced_stats.columns = ['home_' + str(col) for col in home_advanced_stats.columns]

        data = data.merge(away_advanced_stats, how='left')
        data = data.merge(home_advanced_stats, how='left')

        ##Pull and merge teamrankings data
        team_rankings_data = self.pull_team_rankings_data(year)

        away_team_rankings = team_rankings_data.copy()
        home_team_rankings = team_rankings_data.copy()

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
        odds = self.pull_current_odds(start_day, end_day)
        reversed_odds = pd.DataFrame()
        reversed_odds['home_name'] = odds['away_name']
        reversed_odds['away_name'] = odds['home_name']
        odds = pd.concat([odds, reversed_odds])

        ##Merge odds in
        data = data.merge(odds, how='left')

        ##Drop all columns that aren't needed
        data.drop(columns=['away_abbr', 'home_abbr'], inplace=True)

        return data

    def create_historical_dataframe(self, first_year=2007, last_year=2018):
        '''
        Compiles a complete dataframe of stats to use to train models
        Uses specified years given 
        Note:2007 is the first year with historical odds
        '''
        full_data = []
        ##Pull the historical data for each specified year
        for year in range(first_year, last_year+1):
            subset_data = self.create_historical_data(year)
            full_data.append(subset_data)

            ##Save after each year is completed
            partial_historical_data = pd.concat(full_data)
            partial_historical_data.to_csv('historical_nfl_data.csv', index=False)

        full_data = pd.concat(full_data)

        ##Pull the historical odds
        ##Use reverse odds to ensure that neutral site games are included
        odds = self.pull_historical_odds()
        reversed_odds = pd.DataFrame()
        reversed_odds['home_name'] = odds['away_name']
        reversed_odds['away_name'] = odds['home_name']
        reversed_odds['close'] = -odds['close']
        reversed_odds['year'] = odds['year']
        reversed_odds['date'] = odds['date']
        odds = pd.concat([odds, reversed_odds])

        ##Merge odds in
        full_data = full_data.merge(odds, how='left')

        ##Save full historical data
        full_data.to_csv('historical_nfl_data_full.csv', index=False)

        ##Drop all columns that aren't used for regression and save
        full_data.drop(columns=['away_abbr', 'home_abbr', 'away_name', 'home_name', 'home_score', 'away_score', 'date'], inplace=True) 

        full_data.to_csv('historical_nfl_data.csv', index=False)

        return full_data

    def create_opponent_adjusted_stats(self, data, index_start):
        '''
        Update the dataframe to reflect differences in team level for each stat
        First, standardize all columns after a certain index
        By standardize, we normalize each cell by subtracting the min value of the column and dividing by the range
        This transforms the data to be between 0 and 1
        Then, calculate the difference between the two teams with the opposing stats.

        All of these comparitive stats will be in terms of the home team
        '''
        
        ##First ensure that all columns are numeric
        data.iloc[:,index_start:] = data.iloc[:,index_start:].apply(pd.to_numeric)

        ##Normalize the data
        data.iloc[:,index_start:] = (data.iloc[:,index_start:] - np.mean(data.iloc[:,index_start:], axis=0))/(np.std(data.iloc[:,index_start:], axis=0))

        ##Create a new dataframe to hold the opponent-adjusted stats
        updated_data = data.iloc[:,0:index_start].copy()

        ##Create a column for the opponent adjusted stats from the original dataframe
        ##There might be a way to automate this in the future but do it manually for now 
        ##Note we have to vary the signs to ensure that we calculate everything correctly       
        updated_data['dvoa'] = data['home_dvoa'] - data['away_dvoa']
        updated_data['home_off_dvoa'] = data['home_off_dvoa'] + data['away_def_dvoa']
        updated_data['away_off_dvoa'] = data['away_off_dvoa'] - data['home_def_dvoa']
        updated_data['sp_dvoa'] = data['home_sp_dvoa'] - data['away_sp_dvoa']
        updated_data['team_rankings_rating'] = data['home_team_rankings_rating'] - data['away_team_rankings_rating']
        updated_data['strength_of_schedule'] = data['home_strength_of_schedule'] - data['away_strength_of_schedule']

        ##Lower defensive stats indicates a better performance
        updated_data['away_points_per_play'] = -data['away_points_per_play'] - data['home_opponent_points_per_play']
        updated_data['away_yards_per_play'] = -data['away_yards_per_play'] - data['home_opponent_yards_per_play']
        updated_data['away_yards_per_rush'] = -data['away_yards_per_rush'] - data['home_opponent_yards_per_rush']
        updated_data['away_yards_per_pass'] = -data['away_yards_per_pass'] - data['home_opponent_yards_per_pass']
        updated_data['home_points_per_play'] = data['home_points_per_play'] + data['away_opponent_points_per_play']
        updated_data['home_yards_per_play'] = data['home_yards_per_play'] + data['away_opponent_yards_per_play']
        updated_data['home_yards_per_rush'] = data['home_yards_per_rush'] + data['away_opponent_yards_per_rush']
        updated_data['home_yards_per_pass'] = data['home_yards_per_pass'] + data['away_opponent_yards_per_pass']
        
        return updated_data

    def pull_schedule(self, week, year):
        '''
        Pull all games betweena given time period
        '''
        schedule = Boxscores(week, year).games
        full_schedule = list(schedule.values())
        full_schedule = [game for day in full_schedule for game in day]
        full_schedule = pd.DataFrame(full_schedule)

        ##Clean up data by removing bad data and duplicates
        full_schedule = full_schedule[full_schedule['away_abbr'] != full_schedule['home_abbr']]
        full_schedule.drop_duplicates(subset=['boxscore'], inplace=True)

        schedules = []
        team_list = list(set(full_schedule['away_abbr']).union(set(full_schedule['home_abbr'])))
        ##Pull individual schedules for other factors that are not in the boxscore
        for team_abr in team_list:
            team_schedule = Schedule(team_abr, year).dataframe.sort_values(['week'])
            team_schedule['team_abbr'] = team_abr.lower()
            team_schedule['days_between_games'] = team_schedule['datetime'].diff() / np.timedelta64(1, 'D')
            team_schedule['neutral'] = np.where(team_schedule['location'] == 'Neutral', 1, 0)
            schedules.append(team_schedule[['date', 'team_abbr','boxscore_index', 'neutral', 'days_between_games']])

        schedules = pd.concat(schedules)
        schedules.rename(columns={'boxscore_index':'boxscore'}, inplace=True)

        ##Merge location and days_between_games
        full_schedule = full_schedule.merge(schedules[['boxscore', 'neutral', 'date']].drop_duplicates(), how='left')
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

        full_schedule = full_schedule[['date', 'away_abbr', 'away_name', 'home_abbr', 'home_name',\
                                                     'neutral', 'away_short_rest', 'away_long_rest',\
                                                                                        'home_short_rest', 'home_long_rest']]
        return full_schedule

    def pull_historical_schedule(self, year):
        '''
        Pulls all games from a NFL season
        '''
        yearly_schedule = Boxscores(1, year, 21).games
        full_schedule = list(yearly_schedule.values())
        full_schedule = [game for day in full_schedule for game in day]
        full_schedule = pd.DataFrame(full_schedule)
        full_schedule['year'] = year

        schedules = []
        team_list = list(set(full_schedule['away_abbr']).union(set(full_schedule['home_abbr'])))
        ##Pull individual schedules for other factors that are not in the boxscore
        for team_abr in team_list:
            try:
                team_schedule = Schedule(team_abr, year).dataframe.sort_values(['week'])
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
        full_schedule = full_schedule.merge(schedules[['boxscore', 'neutral', 'date']].drop_duplicates(), how='left')
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

        full_schedule = full_schedule[['date', 'away_abbr', 'away_name', 'away_score', 'home_abbr', 'home_name',\
                                                     'home_score', 'year', 'neutral', 'away_short_rest', 'away_long_rest',\
                                                                                        'home_short_rest', 'home_long_rest']]
        return full_schedule

    def create_historical_data(self, year):
        '''
        Creates the historical dataframe for the given year
        Each row in the dataframe is a game with many different stats for each of the participating teams that year
        The stats have been standardized for the given year
        '''
        historical_data = self.pull_historical_schedule(year)       

        ##Keep only the city name for the name
        ##Except for cities with multiple teams
        historical_data['away_name'] = historical_data['away_name'].apply(lambda x: x.rsplit(' ', 1)[0] if x.rsplit(' ', 1)[0] not in ['Los Angeles', 'New York'] else x)
        historical_data['home_name'] = historical_data['home_name'].apply(lambda x: x.rsplit(' ', 1)[0] if x.rsplit(' ', 1)[0] not in ['Los Angeles', 'New York'] else x)

        ##Pull and merge historical advanced stats
        ##Data is pulled from football outsiders
        historical_advanced_stats = self.pull_football_outsiders_stats(year)

        away_advanced_stats = historical_advanced_stats.copy()
        home_advanced_stats = historical_advanced_stats.copy()

        away_advanced_stats.columns = ['away_' + str(col) for col in away_advanced_stats.columns]
        home_advanced_stats.columns = ['home_' + str(col) for col in home_advanced_stats.columns]

        historical_data = historical_data.merge(away_advanced_stats, how='left')
        historical_data = historical_data.merge(home_advanced_stats, how='left')

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

        return historical_data

    def pull_football_outsiders_stats(self, year=None):
        '''
        Pulls advanced stats for a given year from football outsiders using beautifulsoup
        Unfortunately, webscraping means that some values need to be hardcoded
        '''
        ##Create the urls which we will use to webscrape
        ##Add the year to the end of the url
        if year is not None:
            dvoa = 'https://www.footballoutsiders.com/stats/teameff/' + str(year)
            off_dvoa = 'https://www.footballoutsiders.com/stats/teamoff/' + str(year)
            def_dvoa = 'https://www.footballoutsiders.com/stats/teamdef/' + str(year)
            st_dvoa = 'https://www.footballoutsiders.com/stats/teamst/' + str(year)
        else:
            dvoa = 'https://www.footballoutsiders.com/stats/teameff'
            off_dvoa = 'https://www.footballoutsiders.com/stats/teamoff'
            def_dvoa = 'https://www.footballoutsiders.com/stats/teamdef'
            st_dvoa = 'https://www.footballoutsiders.com/stats/teamst'

        dvoa = requests.get(dvoa).text
        soup = BeautifulSoup(dvoa, 'lxml')

        ##Find the table on the page
        ##The most recent table has a different class than past tables
        ##If the first find does not work, we try the second
        dvoa_table = soup.find('table', attrs={'class':'sticky-headers sortable stats'})
        if dvoa_table is None:
            dvoa_table = soup.find('table', attrs={'class':'stats'})

        ##Parse through the rows of the table to create the dataframe
        dvoa_table_rows = dvoa_table.find_all('tr')
        res = []
        for tr in dvoa_table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        historical_fo_stats = pd.DataFrame(res)
        
        ##Format the data based on how the data is returned
        ##Also remove bad rows
        if year is None or year >= 2018:            
            historical_fo_stats = historical_fo_stats[[1,2,7,9,11]]
        else:
            historical_fo_stats = historical_fo_stats[[1,2,6,8,10]]
            historical_fo_stats = historical_fo_stats.iloc[1:]
            historical_fo_stats.columns = ['abbr', 'dvoa', 'off_dvoa', 'def_dvoa', 'sp_dvoa',]
            historical_fo_stats = historical_fo_stats[historical_fo_stats['off_dvoa'] != 'OFF.RANK']

        historical_fo_stats.columns = ['abbr', 'dvoa', 'off_dvoa', 'def_dvoa', 'sp_dvoa',]
        historical_fo_stats = historical_fo_stats[~historical_fo_stats['dvoa'].isin(['LASTYEAR', 'LAST\n\t\t\tYEAR'])]

        ##DVOA is formatted as a percentage so we need to convert it to a float and add 1 to get the percent better than the median team
        historical_fo_stats.iloc[:, 1:] = historical_fo_stats.iloc[:, 1:].apply(lambda x: x.str.rstrip('%').astype('float') / 100.0)
        historical_fo_stats.iloc[:,1:] = historical_fo_stats.iloc[:,1:].apply(pd.to_numeric, errors='coerce')
        historical_fo_stats.iloc[:,1:] = (historical_fo_stats.iloc[:,1:] - np.mean(historical_fo_stats.iloc[:,1:], axis=0))/(np.std(historical_fo_stats.iloc[:,1:], axis=0))

        ##Make abbreviations lowercase to match the format from sportsreference
        historical_fo_stats['abbr'] = historical_fo_stats['abbr'].str.lower()

        ##Replace the correct team names to ensure we can merge with the schedule data
        historical_fo_stats['abbr'].replace(self.team_replace, inplace=True)

        return historical_fo_stats

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
            url = 'https://www.teamrankings.com/nfl/ranking/predictive-by-other?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/ranking/predictive-by-other'
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
            url = 'https://www.teamrankings.com/nfl/ranking/schedule-strength-by-other?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/ranking/schedule-strength-by-other'
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
            url = 'https://www.teamrankings.com/nfl/stat/points-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/points-per-play'
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
            url = 'https://www.teamrankings.com/nfl/stat/opponent-points-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/opponent-points-per-play'
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
            url = 'https://www.teamrankings.com/nfl/stat/yards-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/yards-per-play'
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
            url = 'https://www.teamrankings.com/nfl/stat/opponent-yards-per-play?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/opponent-yards-per-play'
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
            url = 'https://www.teamrankings.com/nfl/stat/yards-per-rush-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/yards-per-rush-attempt'
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
            url = 'https://www.teamrankings.com/nfl/stat/opponent-yards-per-rush-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/opponent-yards-per-rush-attempt'
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
            url = 'https://www.teamrankings.com/nfl/stat/yards-per-pass-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/yards-per-pass-attempt'
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
            url = 'https://www.teamrankings.com/nfl/stat/opponent-yards-per-pass-attempt?date=' + str(year+1)+ '-02-01'
        else:
            url = 'https://www.teamrankings.com/nfl/stat/opponent-yards-per-pass-attempt'
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

    def pull_historical_odds(self, subfolder_name=r'\NFL Historical Odds'):
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
            odds_subset['date'] = pd.to_datetime(odds_subset['date'].astype(str).str.zfill(4), format='%m%d').apply(lambda x: x.strftime('%B %d').replace(" 0", " "))
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

        ##Update team names
        full_odds['home_name'].replace(self.team_replace, inplace=True)
        full_odds['away_name'].replace(self.team_replace, inplace=True)

        return full_odds

    def pull_current_daily_odds(self, games_date):
        '''
        Pull all odds for the given date from sportsbook review
        '''
        url = 'https://classic.sportsbookreview.com/betting-odds/nfl-football/?date=' + str(games_date.date()).replace('-','')
        raw_data = requests.get(url)
        soup = BeautifulSoup(raw_data.text, 'html.parser')
        if soup.find_all('div', id='OddsGridModule_16'):
            soup = soup.find_all('div', id='OddsGridModule_16')[0]
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
