import re
from datetime import datetime, timedelta
from types import NoneType
import requests
from bs4 import BeautifulSoup
import html_to_json
import statsapi
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import MLB.mlb_database
#pip install html-to-json

class Team(Enum):
    away = 0
    home = 1

#just gonna use statsapi, also need to get stats from the relevant year

# machine learning
# be able to look at latest date and contineu on
# I just realized im always looking up the current stat
def get_batter_stat(player_json: dict, stat='obp'):
    player_id, _ = get_player_id(player_json)
    if not look_up_player(player_id)['stats']:
        return
    player_stat = look_up_player(player_id)['stats'][0]['stats'][stat]
    return float(player_stat)





def get_player_id(player_json: dict):
    player_name = player_json['_value']
    player_id = player_json['_attributes']['href'].split('-')[-1]
    return player_id, player_name


def get_lineup_average(player_jsons: list[dict], function):
    lineup_avg = list(stat for stat in map(function, player_jsons) if not isinstance(stat, NoneType))
    return lineup_avg, sum(lineup_avg) / len(lineup_avg)


def get_lineup_jsons(match_soup: BeautifulSoup, home_away: int):
    lineup = match_soup.find('ol',
                             class_=f"starting-lineups__team starting-lineups__team--{str(Team(home_away).name)}").findAll(
        'a')
    return [convert_html(player_html)['a'][0] for player_html in lineup]


def look_up_player(player_id, group='hitting'):
    return statsapi.player_stat_data(player_id, group=group)


def get_win_percent(match_soup: BeautifulSoup, home_or_away: int):
    record_str = str(match_soup.find('div',
                                     class_=f"starting-lineups__team-logo starting-lineups__team-logo--{str(Team(home_or_away).name)}").find(
        'div', class_="starting-lineups__team-record").contents[0])
    W, L = (int(str_) for str_ in re.findall(r"\d+", record_str))
    return W / (W + L)


def convert_html(html):
    html_str = str(html)
    return html_to_json.convert(html_str)


def get_pitcher_stat(player_soup: BeautifulSoup, stat='era'):
    player_json = convert_html(player_soup.find('a'))['a'][0]
    player_id, _ = get_player_id(player_json)
    if not look_up_player(player_id)['stats']:
        return 1
    return float(look_up_player(player_id, 'pitching')['stats'][0]['stats'][stat])


def get_fielder_stat(player_json: dict, stat='fielding'):
    player_id, _ = get_player_id(player_json)
    if not look_up_player(player_id)['stats']:
        return
    return float(look_up_player(player_id, 'fielding')['stats'][0]['stats'][stat])


def lineup_soup(match_soup: BeautifulSoup):
    home_soup = match_soup.find('span', class_="starting-lineups__team-name starting-lineups__team-name--home")
    away_soup = match_soup.find('span', class_="starting-lineups__team-name starting-lineups__team-name--away")
    home_team = convert_html(home_soup.contents[1])['a'][0]['_value']
    away_team = convert_html(away_soup.contents[1])['a'][0]['_value']
    home_lineup = get_lineup_jsons(match_soup, 1)
    if not home_lineup:
        print("No Lineup")
        return 1
    away_lineup = get_lineup_jsons(match_soup, 0)
    _, home_avg_obp = get_lineup_average(home_lineup, get_batter_stat)
    _, away_avg_obp = get_lineup_average(away_lineup, get_batter_stat)
    _, home_fielding_percent = get_lineup_average(home_lineup, get_fielder_stat)
    _, away_fielding_percent = get_lineup_average(away_lineup, get_fielder_stat)
    home_record = get_win_percent(match_soup, 1)
    away_record = get_win_percent(match_soup, 0)
    away_pitcher_era, home_pitcher_era = (get_pitcher_stat(player_soup) for
                                          player_soup in match_soup.findAll('div', 'starting-lineups__pitcher-name'))
    away_pred = away_avg_obp * away_record * home_pitcher_era * away_fielding_percent
    home_pred = home_avg_obp * home_record * away_pitcher_era * home_fielding_percent
    predicted_winner = int(away_pred < home_pred )
    actual_winner = get_winning_team(int(match_soup['data-gamepk']))
    # print(predicted_winner,actual_winner, away_pred, home_pred, away_team, home_team)
    return away_team, home_team, away_avg_obp, home_avg_obp, away_fielding_percent, home_fielding_percent, away_pitcher_era, home_pitcher_era,actual_winner


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
        return int(home_score>away_score)
    else:
        return 2


def scrape_lineups(url: str):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    if response.status_code == 200:
        with ThreadPoolExecutor(max_workers=len(soup.findAll('div', class_="starting-lineups__matchup"))) as executor:
            results = list(executor.map(lineup_soup, soup.findAll('div', class_="starting-lineups__matchup")))
        return results


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
    sql_connect = mlb_database.create_db_connection(*mlb_database.LOGON_DICT.values(), 'mlb')
    table_dates = set(
        date[0].strftime("%Y-%m-%d") for date in mlb_database.get_table_column(sql_connect, 'games', 'date'))
    dates = dates.difference(table_dates)
    return dates


def get_predictions(date:str=datetime.now().strftime("%Y-%m-%d")):
    lineup_http = rf"https://www.mlb.com/starting-lineups/{date}"
    scrape_lineups(lineup_http)


if __name__ == "__main__":
    get_predictions()

    # for dates in get_baseball_season_dates(2023):
    #     lineup_http = rf"https://www.mlb.com/starting-lineups/{dates}"
    #     print(lineup_http)
    #     try:
    #         preds = scrape_lineups(lineup_http)
    #         sql_connects = tuple(
    #             mlb_database.create_db_connection(*mlb_database.logon_dict.values(), 'mlb') for _ in range(len(preds)))
    #         insert_game = lambda connection, pred: mlb_database.insert_game('games', connection, *pred, dates)
    #         with ThreadPoolExecutor(max_workers=len(preds)) as executor:
    #             executor.map(insert_game, sql_connects, preds)
    #     except Exception as e:
    #         print(e)
