from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor

import statsapi
from datetime import datetime, timedelta
from numpy import mean

from MLB.mlb_database import insert_game, create_db_connection, logon_dict, get_table_column



def get_game_ids(date:str=datetime.now().strftime("%Y-%m-%d")):
    schedule = statsapi.schedule(start_date=date, end_date=date)
    game_ids = [game['game_id'] for game in schedule]
    return game_ids


def get_fielding_stats(player_id, season='2024'):
    stats = statsapi.get('person',
                         {'personId': player_id, 'hydrate': f'stats(group=[fielding],type=[season],season={season})'})
    try:
        fp = float(stats.get('people')[0].get('stats')[0].get('splits')[0].get('stat').get('fielding'))
    except TypeError as e:
        print('no stats')
        return None
    if fp <= 0:
        return None
    return fp


def get_top_three_batters_ops(game_id):
    game_data = statsapi.boxscore_data(game_id)

    home_team = game_data['home']
    away_team = game_data['away']

    home_name = game_data['teamInfo']['home']['teamName']
    away_name = game_data['teamInfo']['away']['teamName']

    home_players = home_team['players']
    away_players = away_team['players']
    home_ops = {player['person']['fullName']:float(player['seasonStats']['batting']['avg'])*player['seasonStats']['batting']['atBats'] for player in home_players.values() if
                'battingOrder' in player.keys()}
    home_ops = sorted(home_ops.items(),key=lambda item:item[1],reverse=True)
    away_ops = {player['person']['fullName']:float(player['seasonStats']['batting']['avg'])*player['seasonStats']['batting']['atBats'] for player in away_players.values() if
                'battingOrder' in player.keys()}
    away_ops = sorted(away_ops.items(), key=lambda item: item[1],reverse=True)
    if away_ops and home_ops:
        return away_name,home_name, away_ops[0:2],home_ops[0:2],away_ops[-1],home_ops[-1]
    else:
        return


def get_winning_team(game_id):
    # Retrieve game schedule information
    schedule_info = statsapi.schedule(game_id=game_id)

    if not schedule_info:
        return "Game not found"

    game = schedule_info[0]
    home_team, away_team = game['home_name'], game['away_name']
    home_score, away_score = game['home_score'], game['away_score']
    game_status = game['status']

    # Determine the winner
    if game_status == 'Final':
        return int(home_score > away_score)
    else:
        return 2
def use_thread_pool(function,iterable_of_iterables):
    with ThreadPoolExecutor(max_workers=len(iterable_of_iterables[0])) as executor:
        executor.map(function, *iterable_of_iterables)

def use_process_pool(function,iterable_of_iterables,num_workers):
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results=list(executor.map(function, *iterable_of_iterables))
    if results:
        return results
def get_lineup_data(game_id):
    game_data = statsapi.boxscore_data(game_id)
    season = game_data['gameId'][0:4]

    home_team = game_data['home']
    away_team = game_data['away']
    home_name=game_data['teamInfo']['home']['teamName']
    away_name = game_data['teamInfo']['away']['teamName']

    home_players = home_team['players']
    away_players = away_team['players']

    home_ops = [float(player['seasonStats']['batting']['ops']) for player in home_players.values() if
                'battingOrder' in player.keys()]

    away_ops = [float(player['seasonStats']['batting']['ops']) for player in away_players.values() if
                'battingOrder' in player.keys()]
    home_fp = [get_fielding_stats(player['person']['id'], season) for player in home_players.values() if
               'battingOrder' in player.keys()]
    away_fp = [get_fielding_stats(player['person']['id'], season) for player in away_players.values() if
               'battingOrder' in player.keys()]

    home_fp = [fp for fp in home_fp if isinstance(fp, float)]
    away_fp = [fp for fp in away_fp if isinstance(fp, float)]

    away_starting_pitcher=game_data.get('awayPitchers')[1].get('personId')
    away_era=game_data['away']['players'][f'ID{away_starting_pitcher}']['seasonStats']['pitching']['era']
    if not away_era[0].isalnum():
        away_era=3.0
    home_starting_pitcher = game_data.get('homePitchers')[1].get('personId')
    home_era = game_data['home']['players'][f'ID{home_starting_pitcher}']['seasonStats']['pitching']['era']
    if not home_era[0].isalnum():
        home_era=3.0
    winner=get_winning_team(game_id)
    return game_id,away_name, home_name, mean(away_ops), mean(home_ops), mean(away_fp), mean(
        home_fp), float(away_era), float(home_era), winner

def get_baseball_season_dates(year: int):
    delta = timedelta(days=1)

    start = datetime(year, 3, 28)
    end = datetime(year, 9, 28)
    dates = set()
    while start <= end:
        # add current date to list by converting  it to iso format
        dates.add(start.strftime("%Y-%m-%d"))
        # increment start date by timedelta
        start += delta
    sql_connect = create_db_connection(*logon_dict.values(), 'mlb')
    table_dates = set(
        date[0].strftime("%Y-%m-%d") for date in get_table_column(sql_connect, 'games', 'date'))
    dates = dates.difference(table_dates)
    return dates

if __name__ == '__main__':
    for date in get_baseball_season_dates(2023):
        try:
            print(date)
            game_ids=get_game_ids(date)
            with ThreadPoolExecutor(max_workers=len(game_ids)) as executor:
                game_info=list(executor.map(get_lineup_data,  game_ids))
            sql_connects = tuple(
                create_db_connection(*logon_dict.values(), 'mlb') for _ in range(len(game_info)))
            insert_function = lambda connection, game: insert_game('games', connection, *game, date)
            with ThreadPoolExecutor(max_workers=len(game_info)) as executor:
                executor.map(insert_function, sql_connects, game_info)
        except Exception as e:
            print(e)
