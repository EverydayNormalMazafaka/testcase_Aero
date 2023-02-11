import pandas as pd
import requests
import schedule
import time
import pyodbc
import sqlalchemy as sa

basic_url = 'https://statsapi.web.nhl.com'
server = 'DESKTOP-S8JARD4'
db = 'testbase'

def teams_df():
    res = requests.get(basic_url + '/api/v1/teams').json()
    id_list = []
    name_list = []
    for x in res['teams']:
        id_list.append(x['id']) 
        name_list.append(x['name'])
    df = pd.DataFrame(list(zip(name_list, id_list)),columns = ['Name','Id'])
    
    return df

def add_statistic(df):
    stats = pd.DataFrame()
    for index, row in df.iterrows():
        team = str(row['Id'])
        url = basic_url + f'/api/v1/teams/{team}/stats'
        response = requests.get(url).json()
        stat_dict = response['stats'][0]['splits'][0]['stat']
        row = pd.concat([row, pd.Series(stat_dict)])
        stats = stats.append(row, ignore_index=True)
        
    return stats

def insertor(data):
    if not data.empty:
        cnxn = pyodbc.connect(fr'Driver=SQL Server;Server={server};Database={db};Trusted_Connection=yes;')
        cursor = cnxn.cursor()
        columns_name = ','.join(data.columns.values)
        columns  = '(' + columns_name + ')'
        cursor.execute("truncate table NHL_statistic")
        for index, row in data.iterrows():
            cursor.execute("INSERT INTO NHL_statistic " + columns + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.Name, row.Id, row.gamesPlayed, row.wins, row.losses, row.ot, row.pts, row.ptPctg, row.goalsPerGame, row.goalsAgainstPerGame, row.evGGARatio, row.powerPlayPercentage, row.powerPlayGoals, row.powerPlayGoalsAgainst, row.powerPlayOpportunities, row.penaltyKillPercentage, row.shotsPerGame, row.shotsAllowed, row.winScoreFirst, row.winOppScoreFirst, row.winLeadFirstPer, row.winLeadSecondPer, row.winOutshootOpp, row.winOutshotByOpp, row.faceOffsTaken, row.faceOffsWon, row.faceOffsLost, row.faceOffWinPercentage, row.shootingPctg, row.savePctg)
        cnxn.commit()
        cursor.close()
        
        return 'Lucky insert'
    else:
      
        return 'No data to insert'

def main():
    df = teams_df()
    print(df.head(10))
    data = add_statistic(df)
    print(data.head(10))
    res = insertor(data)
    print(res)

schedule.every().day.at('12:00').do(main)
schedule.every().day.at('00:00').do(main)

while True:
    schedule.run_pending()
    time.sleep(1)
