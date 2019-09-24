from sportsreference.ncaab.teams import Teams
from sportsreference.ncaab.schedule import Schedule
from sportsreference.ncaab.boxscore import Boxscore, Boxscores
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
import requests
from bs4 import BeautifulSoup


class CbbDataLayer():

    def pull_teams_information(self):
        '''
        Pulls all team for this year
        '''
        teams = Teams()
        self.all_team_data = teams.dataframes
        self.team_list = list(self.all_team_data['name'].unique())
        self.team_abbreviations = list(self.all_team_data['abbreviation'].unique())

    def pull_historical_team_info(self, year):
        '''
        Pulls all team stats for a given year
        '''
        teams = Teams(year)
        return teams.dataframes

    def pull_todays_schedule(self):
        '''
        Pulls all games for today
        '''
        games_todays = Boxscores(pd.to_datetime('today')).games
        for _, games in games_todays.items():
            return pd.DataFrame(games)

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
        ##Pull individual schedules for other factors that are not in the boxscore
        for team_abr in self.team_abbreviations:
            team_schedule = Schedule(team_abr, year).dataframe.sort_values(['game'])
            team_schedule['team_abbr'] = team_abr.lower()
            team_schedule['days_between_games'] = team_schedule['datetime'].diff() / np.timedelta64(1, 'D')
            team_schedule['location'] = np.where(team_schedule['location'] == 'Neutral', 'Neutral', 'Non-Neutral')
            schedules.append(team_schedule[['team_abbr','boxscore_index', 'location', 'days_between_games']])

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

        full_schedule.drop(columns=['team_abbr_x', 'team_abbr_y'], inplace=True)

        full_schedule = full_schedule[['away_abbr', 'away_name', 'away_score', 'home_abbr', 'home_name',\
                                                     'home_score', 'year', 'location', 'away_off_days', 'home_off_days']]
        return full_schedule

    def create_historical_data(self, year):

        historical_schedule = self.pull_historical_schedule(year)
        historical_team_info = self.pull_historical_team_info(year)
        historical_team_info.rename(columns={'abbreviation':'abbr'}, inplace=True)
        historical_team_info.drop(columns=['wins', 'losses', 'conference', 'conference_wins',\
                            'conference_losses', 'win_percentage', 'conference_win_percentage', 'games'], inplace=True)
        historical_team_info['abbr'] = historical_team_info['abbr'].str.lower()

        away_team_info = historical_team_info.copy()
        home_team_info = historical_team_info.copy()

        away_team_info.columns = ['away_' + str(col) for col in away_team_info.columns]
        home_team_info.columns = ['home_' + str(col) for col in home_team_info.columns]

        historical_data = historical_schedule.merge(away_team_info, how='left')
        historical_data = historical_data.merge(home_team_info, how='left')

        ##Drop games against non-D1 oppponents
        historical_data.dropna(subset=['away_first_downs'], inplace=True)

        return historical_data

