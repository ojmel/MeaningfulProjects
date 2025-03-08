import re
import statsapi
import mlbstatsapi
import pandas as pd

from datetime import datetime, timedelta
from numpy import mean, median



def team_stats(game_id):
    game_data = statsapi.boxscore_data(game_id)
    home_dict={'name':game_data['teamInfo']['home']['teamName']}
    away_dict={'name':game_data['teamInfo']['away']['teamName']}

    home_id = game_data['teamInfo']['home']['id']
    away_id = game_data['teamInfo']['away']['id']
    home_stats=mlb.get_team_stats(home_id, ['season', 'seasonAdvanced'], ['hitting','pitching'],**{'season': 2024})
    away_stats=mlb.get_team_stats(away_id, ['season', 'seasonAdvanced'], ['hitting','pitching'],**{'season': 2024})
    home_hits=home_stats['hitting']['season']
    away_hits= away_stats['hitting']['season']
    home_pitch = home_stats['pitching']['season']
    away_pitch = away_stats['pitching']['season']

    home_dict['runs']=home_hits.splits[0].stat.__dict__['runs']
    away_dict['runs'] = away_hits.splits[0].stat.__dict__['runs']
    home_dict['SO']=home_hits.splits[0].stat.__dict__['strikeouts']
    away_dict['SO'] = away_hits.splits[0].stat.__dict__['strikeouts']
    home_dict['hits']=home_hits.splits[0].stat.__dict__['hits']
    away_dict['hits'] = away_hits.splits[0].stat.__dict__['hits']
    home_dict['BB'] = home_hits.splits[0].stat.__dict__['baseonballs']
    away_dict['BB'] = away_hits.splits[0].stat.__dict__['baseonballs']
    home_dict['ER'] = home_pitch.splits[0].stat.__dict__['runs']
    away_dict['ER'] = away_pitch.splits[0].stat.__dict__['runs']
    return home_dict,away_dict
today=datetime.now().strftime("%Y-%m-%d")
tomorrow=(datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")

def get_pitching_lastxgames(pitcher_id,hit_rank:pd.DataFrame,num_of_games=5):
    aggregate_stats={}

    games = mlb.get_player_stats(pitcher_id, stats=['gameLog'], groups=['pitching']).get('pitching',{}).get('gamelog',mlbstatsapi.mlb_api.Stat(type='gameLog',group='pitching',totalsplits=1)).__getattribute__('splits')

    if games.__len__() >= num_of_games:
        games=games[-num_of_games:]
        aggregate_stats['name']=games[0].player.fullname
        aggregate_stats['team'] = games[-1].team.name
        runs=mean(tuple(game.stat.runs for game in games))
        SO = mean(tuple(game.stat.strikeouts for game in games))
        hits = median(tuple(game.stat.hits for game in games))
        IP= mean(tuple(float(game.stat.inningspitched) for game in games))
        SO_rank=median(tuple(get_hit_rank_position(hit_rank,game.opponent['name']) for game in games))
        run_rank=median(tuple(get_hit_rank_position(hit_rank,game.opponent['name'],'R') for game in games))
        aggregate_stats['runs']=runs
        aggregate_stats['R_rank'] = run_rank
        aggregate_stats['SO'] = SO
        aggregate_stats['SO_rank']=SO_rank
        aggregate_stats['HIP'] = hits/IP
        aggregate_stats['IP'] = IP
        aggregate_stats['score']=SO+IP-(aggregate_stats['HIP']*runs)
    else:
        aggregate_stats['name'] = ''
        aggregate_stats['team'] = ''
        aggregate_stats['runs'] = 0
        aggregate_stats['R_rank'] = 0
        aggregate_stats['SO'] = 0
        aggregate_stats['SO_rank']=0
        aggregate_stats['HIP'] = 0
        aggregate_stats['IP'] = 0
        aggregate_stats['score'] = 0
    return aggregate_stats
# game.__getattribute__('ishome') game_data.gamedata.teams.away.name
# # Loop through the games to find starting pitchers

def get_hit_rank_position(hit_rank:pd.DataFrame,team:str,category='SO'):
    hit_rank=hit_rank.sort_values(category)
    return hit_rank.index.get_loc(team)

def get_pitch_ranking():
    stat_url='https://www.mlb.com/stats/team/pitching?timeframe=-29'
    if (response := requests.get(stat_url)).status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        seen_set=set()
        columns=[]
        for column in soup.find('thead').findAll('abbr'):
            column = re.search(r'>(.*)<', str(column)).group(1)
            if column not in seen_set:
                columns.append(column)
                seen_set.add(column)
        data_table = pd.DataFrame(columns=columns)
        for team in soup.find('tbody').findAll('tr'):
            new_row=[]
            team_name=team.find('th').find('a')['aria-label']
            new_row.append(team_name)
            for stat in team.findAll('td'):
                new_row.append(stat.text)
            data_table=data_table._append({column:value for column,value in zip(columns,new_row)}, ignore_index=True)
        data_table = data_table.set_index('TEAM').infer_objects()
        data_table['ER'] = data_table['ER'].astype(int) / data_table['G'].astype(int)
        data_table.to_csv('pitch_rank.csv')
        data_table = pd.read_csv('pitch_rank.csv', index_col='TEAM')
        return data_table

def get_hitting_ranking():
    stat_url='https://www.mlb.com/stats/team?timeframe=-29'
    if (response := requests.get(stat_url)).status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        seen_set=set()
        columns=[]
        for column in soup.find('thead').findAll('abbr'):
            column = re.search(r'>(.*)<', str(column)).group(1)
            if column not in seen_set:
                columns.append(column)
                seen_set.add(column)
        data_table = pd.DataFrame(columns=columns)
        for team in soup.find('tbody').findAll('tr'):
            new_row=[]
            team_name=team.find('th').find('a')['aria-label']
            new_row.append(team_name)
            for stat in team.findAll('td'):
                new_row.append(stat.text)
            data_table=data_table._append({column:value for column,value in zip(columns,new_row)}, ignore_index=True)
        data_table=data_table.set_index('TEAM')
        data_table['R']=data_table['R'].astype(int)/data_table['G'].astype(int)
        data_table.to_csv('hit_rank.csv')
        data_table=pd.read_csv('hit_rank.csv',index_col='TEAM')
        return data_table


def pitcher_table(make_run_table=True):
    table=pd.DataFrame()
    run_table=pd.DataFrame(columns=['home','home_pot','away','away_pot'])
    hit_rank = get_hitting_ranking()
    pitch_rank = get_pitch_ranking()
    for game in schedule:
        run_pot={}
        game_id = game.gamepk
        game_data = mlb.get_game(game_id)
        away_name=game_data.gamedata.teams.away.name
        home_name=game_data.gamedata.teams.home.name
        pitchers=game_data.gamedata.probablepitchers
        if home_pitcher:=pitchers.home:
            home_stats = get_pitching_lastxgames(home_pitcher.id,hit_rank)
            home_stats['side'] = 'home'
            home_stats['opp'] = away_name
            home_stats['opp_SO(1)'] = get_hit_rank_position(hit_rank,away_name)
            home_stats['opp_R(30)'] = get_hit_rank_position(hit_rank, away_name,'R')
            table=pd.concat([table,pd.DataFrame(home_stats,index=[0])], ignore_index=True)

        if away_pitcher:=pitchers.away:
            away_stats=get_pitching_lastxgames(away_pitcher.id,hit_rank)
            away_stats['side']='away'
            away_stats['opp']=home_name
            away_stats['opp_SO(1)'] = get_hit_rank_position(hit_rank, home_name)
            away_stats['opp_R(30)'] = get_hit_rank_position(hit_rank, home_name,'R')
            table=pd.concat([table,pd.DataFrame(away_stats,index=[0])], ignore_index=True)

        if away_pitcher and home_pitcher:
            run_pot['home'] = home_name
            run_pot['home_pot'] = hit_rank.loc[home_name, 'R'] + pitch_rank.loc[away_name, 'ER'] +away_stats['runs']
            run_pot['away'] = away_name
            run_pot['away_pot'] = hit_rank.loc[away_name, 'R'] + pitch_rank.loc[home_name, 'ER'] + home_stats['runs']
            run_table = pd.concat([run_table, pd.DataFrame(run_pot, index=[0])], ignore_index=True)
    run_table.to_csv('run_pot.csv')
    table.to_csv('todays_pitchers.csv')
    return table
if __name__=='_main__':
    mlb = mlbstatsapi.Mlb()
    schedule = mlb.get_scheduled_games_by_date(start_date=today, end_date=today)
    pitcher_table()
