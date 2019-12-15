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
                            'State Francis NY':'St. Francis (NY)', 'State Francis PA':'Saint Francis (PA)', "St Peter's":"St. Peter's", 'S Carolina State':'South Carolina State',\
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
                            'Alab A&M':'Alabama A&M', 'Miss Val State':'Mississippi Valley State'}
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
        full_schedule = full_schedule.merge(schedules[['boxscore', 'neutral']].drop_duplicates(), how='left', left_on=['boxscore'], right_on=['boxscore'])
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

        full_schedule = full_schedule[['away_abbr', 'away_name', 'away_score', 'home_abbr', 'home_name',\
                                                     'home_score', 'year', 'neutral', 'away_short_rest', 'away_long_rest',\
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

        historical_data.to_csv('test.csv', index=False)

        return historical_data


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

        ken_pom_stats = ken_pom_stats[[1, 5, 7]]
        ken_pom_stats.columns = ['name', 'ken_pom_offense', 'ken_pom_defense']

        ##Clean the data
        ken_pom_stats['name'] = ken_pom_stats['name'].apply(lambda x: x.rstrip('0123456789 '))

        ##Update the team names to match the base
        ken_pom_stats['name'] = ken_pom_stats['name'].apply(lambda x: x.replace(' St.', ' State'))
        ken_pom_stats['name'].replace(self.team_replace, inplace=True)

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

        ##Update the team names to match the base
        team_rankings_data['name'] = team_rankings_data['name'].apply(lambda x: x.replace(' St', ' State') if ' State' not in x else x)
        team_rankings_data['name'].replace(self.team_replace, inplace=True)

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
        strengh_of_schedule.columns = ['name', 'strength_of_schedule']
        
        ##Get rid of the record in the team name
        strengh_of_schedule['name'] = strengh_of_schedule['name'].apply(lambda x: x[0:x.rfind(' (')])

        return strengh_of_schedule

x = CbbDataLayer()
x.create_historical_data(2018)
