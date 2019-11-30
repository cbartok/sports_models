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


class CbbDataLayer():
    '''
    Class to hold all the functions that pull data for College Basketball
    '''

    def __init__(self):
        self.team_replace = {}
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

    def pull_historical_schedule(self, year):
        '''
        Pulls all games from a college football season
        '''
        yearly_schedule = Boxscores(datetime(year, 11, 1), datetime(year+1, 4, 15)).games
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
        full_schedule = full_schedule.merge(schedules[['boxscore', 'location']].drop_duplicates(), how='left')
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

        full_schedule = full_schedule[['date', 'away_abbr', 'away_name', 'away_score', 'home_abbr', 'home_name',\
                                                     'home_score', 'year', 'neutral', 'away_short_rest', 'away_long_rest',\
                                                                                        'home_short_rest', 'home_long_rest']]
        return full_schedule

    def create_historical_data(self, year):

        historical_schedule = self.pull_historical_schedule(year)

        ##Some locations are nans so set them to be campus games
        historical_data['neutral'] = np.where(historical_data['neutral'] == 1, 1, 0)

        ##Pull historical massey
        historical_massey = self.pull_massey_composite_rankings(year)


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
        f_table = soup.find('table', attrs={'class':'ratings-table'})

        ##Parse through the rows of the table to create the dataframe
        f_table_rows = f_table.find_all('tr')
        res = []
        for tr in f_table_rows:
            td = tr.find_all('td')
            row = [tr.text.strip() for tr in td if tr.text.strip()]
            if row:
                res.append(row)

        ken_pom_stats = pd.DataFrame(res)
