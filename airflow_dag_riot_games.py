from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from datetime import datetime, timedelta
import logging
import requests
import os

def get_summoner_id(summoner_name, region, riot_api_key):
    url = f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner_name}"
    headers = {"X-Riot-Token": riot_api_key}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to get summoner ID. Status Code: {response.status_code}")
    
    data = response.json()
    return data['puuid']

def get_matchlist(puuid, region, riot_api_key):
    url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    headers = {"X-Riot-Token": riot_api_key}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to get matchlist. Status Code: {response.status_code}")
    
    match_ids = response.json()
    return match_ids

def get_match_details(match_id, region, riot_api_key):
    url = f"https://{region}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": riot_api_key}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to get match details. Status Code: {response.status_code}")
    
    match_details = response.json()
    return match_details

def grab_data_with_logging(logger, data, key):
    value = data.get(key)
    if value is None:
        logger.warning(f"The key {key} is missing from the data")
    return value

def get_riot_data(**kwargs):
    summoner_name = "boosblues"
    region = "na1"
    riot_api_key = os.environ.get('RIOT_API_KEY')

    # riot key error check
    if not riot_api_key:
        raise ValueError("RIOT_API_KEY environment variable is not set")

    puuid = get_summoner_id(summoner_name, riot_api_key)
    match_ids = get_matchlist(puuid, riot_api_key)
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    game_stats_list = []
    champion_stats_list = []
    kda_stats_list = []
    damage_stats_list = []
    misc_stats_list = []
    
    # need to handle some way of duplicate matches if i rerun the dag
    for match_id in match_ids:
        match_details = get_match_details(match_id, riot_api_key)
        
        # DATA EXTRACTION ----------------------------------------------------------
        
        # grab correct participant
        try:
            participants = match_details['metadata']['participants']
            index = participants.index(puuid)
        except ValueError:
            logger.error("Participant ID not found in the list")
            # Handle the error, e.g., by skipping to the next iteration, or setting index to None
            index = None
        except KeyError:
            logger.error("The structure of the JSON is not as expected. 'metadata' or 'participants' key is missing.")
            # Handle the error as appropriate
            index = None
            
        # Grab specific data
        # GAME STATS -------------------------------------
        if index is not None:
            # Grab specific data
            try:
                info = match_details.get("info", {})
                gameCreation = info.get("gameCreation")
                if gameCreation is None:
                    logger.warning("The 'gameCreation' key is missing from 'info'.")
                
                participant_data = info.get("participants", [])[index] if info else None
                if participant_data:
                    # create new dictionaries
                    
                    # GAME STATS -------------------------------------
                    win = grab_data_with_logging(logger, participant_data, 'win')
                    gameEndedInSurrender = grab_data_with_logging(logger, participant_data, 'gameEndedInSurrender')
                    timePlayed = grab_data_with_logging(logger, participant_data, 'timePlayed')
                    gameMode = grab_data_with_logging(logger, participant_data, 'gameMode')
                    lane = grab_data_with_logging(logger, participant_data, 'lane')
                    role = grab_data_with_logging(logger, participant_data, 'role')
                    game_stats = (win, gameEndedInSurrender, timePlayed, gameMode, lane, role)
                    
                    # CHAMPION STATS -------------------------------------
                    championName = grab_data_with_logging(logger, participant_data, 'championName')
                    champLevel = grab_data_with_logging(logger, participant_data, 'champLevel')
                    champion_stats = (championName, champLevel)
                    
                    # KILL/ASSIST/DEATH STATS -------------------------------------
                    kills = grab_data_with_logging(logger, participant_data, 'kills')
                    assists = grab_data_with_logging(logger, participant_data, 'assists')
                    deaths = grab_data_with_logging(logger, participant_data, 'deaths')
                    totalTimeSpentDead = grab_data_with_logging(logger, participant_data, 'totalTimeSpentDead')
                    doubleKills = grab_data_with_logging(logger, participant_data, 'doubleKills')
                    tripleKills = grab_data_with_logging(logger, participant_data, 'tripleKills')
                    quadraKills = grab_data_with_logging(logger, participant_data, 'quadraKills')
                    pentaKills = grab_data_with_logging(logger, participant_data, 'pentaKills')
                    totalMinionsKilled = grab_data_with_logging(logger, participant_data, 'totalMinionsKilled')
                    totalAllyJungleMinionsKilled = grab_data_with_logging(logger, participant_data, 'totalAllyJungleMinionsKilled')
                    dragonKills = grab_data_with_logging(logger, participant_data, 'dragonKills')
                    baronKills = grab_data_with_logging(logger, participant_data, 'baronKills')
                    largestMultiKill = grab_data_with_logging(logger, participant_data, 'largestMultiKill')
                    largestKillingSpree = grab_data_with_logging(logger, participant_data, 'largestKillingSpree')
                    kda_stats = (kills, assists, deaths, totalTimeSpentDead, doubleKills, tripleKills, quadraKills, pentaKills, 
                                 totalMinionsKilled, totalAllyJungleMinionsKilled, dragonKills, baronKills, largestMultiKill, 
                                 largestKillingSpree)
                    
                    # DAMAGE STATS -------------------------------------
                    physicalDamageDealt = grab_data_with_logging(logger, participant_data, 'physicalDamageDealt')
                    physicalDamageDealtToChampions = grab_data_with_logging(logger, participant_data, 'physicalDamageDealtToChampions')
                    physicalDamageTaken = grab_data_with_logging(logger, participant_data, 'physicalDamageTaken')
                    magicDamageDealt = grab_data_with_logging(logger, participant_data, 'magicDamageDealt')
                    magicDamageDealtToChampions = grab_data_with_logging(logger, participant_data, 'magicDamageDealtToChampions')
                    magicDamageTaken = grab_data_with_logging(logger, participant_data, 'magicDamageTaken')
                    trueDamageDealt = grab_data_with_logging(logger, participant_data, 'trueDamageDealt')
                    trueDamageDealtToChampions = grab_data_with_logging(logger, participant_data, 'trueDamageDealtToChampions')
                    trueDamageTaken = grab_data_with_logging(logger, participant_data, 'trueDamageTaken')
                    totalDamageDealt = grab_data_with_logging(logger, participant_data, 'totalDamageDealt')
                    totalDamageDealtToChampions = grab_data_with_logging(logger, participant_data, 'totalDamageDealtToChampions')
                    totalDamageTaken = grab_data_with_logging(logger, participant_data, 'totalDamageTaken')
                    totalHeal = grab_data_with_logging(logger, participant_data, 'totalHeal')
                    totalHealsOnTeammates = grab_data_with_logging(logger, participant_data, 'totalHealsOnTeammates')
                    totalTimeCCDealt = grab_data_with_logging(logger, participant_data, 'totalTimeCCDealt')
                    largestCriticalStrike = grab_data_with_logging(logger, participant_data, 'largestCriticalStrike')
                    damageSelfMitigated = grab_data_with_logging(logger, participant_data, 'damageSelfMitigated')
                    damageDealtToBuildings = grab_data_with_logging(logger, participant_data, 'damageDealtToBuildings')
                    damageDealtToObjectives = grab_data_with_logging(logger, participant_data, 'damageDealtToObjectives')
                    damageDealtToTurrets = grab_data_with_logging(logger, participant_data, 'damageDealtToTurrets')
                    damage_stats = (physicalDamageDealt, physicalDamageDealtToChampions, physicalDamageTaken, magicDamageDealt, magicDamageDealtToChampions,
                                    magicDamageTaken, trueDamageDealt, trueDamageDealtToChampions, trueDamageTaken, totalDamageDealt, totalDamageDealtToChampions,
                                    totalDamageTaken, totalHeal, totalHealsOnTeammates, totalTimeCCDealt, largestCriticalStrike, damageSelfMitigated,
                                    damageDealtToBuildings, damageDealtToObjectives, damageDealtToTurrets)
                    
                    # MISC STATS -------------------------------------
                    goldEarned = grab_data_with_logging(logger, participant_data, 'goldEarned')
                    goldSpent = grab_data_with_logging(logger, participant_data, 'goldSpent')
                    itemsPurchased = grab_data_with_logging(logger, participant_data, 'itemsPurchased')
                    visionScore = grab_data_with_logging(logger, participant_data, 'visionScore')
                    visionWardsBoughtInGame = grab_data_with_logging(logger, participant_data, 'visionWardsBoughtInGame')
                    sightWardsBoughtInGame = grab_data_with_logging(logger, participant_data, 'sightWardsBoughtInGame')
                    wardsKilled = grab_data_with_logging(logger, participant_data, 'wardsKilled')
                    wardsPlaced = grab_data_with_logging(logger, participant_data, 'wardsPlaced')
                    misc_stats = (goldEarned, goldSpent, itemsPurchased, visionScore, visionWardsBoughtInGame, sightWardsBoughtInGame, 
                                  wardsKilled, wardsPlaced)
                    
                    game_stats_list.append(game_stats)
                    champion_stats_list.append(champion_stats)
                    kda_stats_list.append(kda_stats)
                    damage_stats_list.append(damage_stats)
                    misc_stats_list.append(misc_stats)
                    
                else:
                    logger.error("Participants data not found or index out of range.")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {str(e)}")
                
    # Push data out
    list_data = [game_stats_list, champion_stats_list, kda_stats_list, damage_stats_list, misc_stats_list]
    kwargs['ti'].xcom_push(key='list_data', value=list_data)

def sep_data(list_data):
    game_stats = list_data[0]
    champion_stats = list_data[1]
    kda_stats = list_data[2]
    damage_stats = list_data[3]
    misc_stats = list_data[4]
    return game_stats, champion_stats, kda_stats, damage_stats, misc_stats

def add_data_database(**kwargs):
    # Grab data
    ti = kwargs['ti']
    list_data = ti.xcom_pull(task_ids='get_riot_data_task', key='list_data')
    game_stats, champion_stats, kda_stats, damage_stats, misc_stats = sep_data(list_data)
    
    hook = SqliteHook(sqlite_conn_id='lol_summoner_matches_sqlite')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        
        for index in range(len(game_stats)):
            # GAME STATS
            insert_stmt = '''
            INSERT INTO game_stats
            (gameCreation,win,gameEndedInSurrender,timePlayed,gameMode,lane,role) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
            '''.strip()
            game_data = game_stats[index]
            cursor.execute(insert_stmt, game_data)
            game_id = cursor.lastrowid
            
            # CHAMPION STATS
            insert_stmt = '''
            INSERT INTO champion_stats
            (game_id,championName,champLevel) 
            VALUES (?, ?, ?)
            '''.strip()
            champion_data = (game_id,) + champion_stats[index]
            cursor.execute(insert_stmt, champion_data)
            
            # KDA STATS
            insert_stmt = '''
            INSERT INTO kda_stats
            (game_id,kills,assists,deaths,totalTimeSpentDead,doubleKills,tripleKills,
            quadraKills,pentaKills,totalMinionsKilled,totalAllyJungleMinionsKilled,dragonKills,
            baronKills,largestMultiKill,largestKillingSpree) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''.strip()
            kda_data = (game_id,) + kda_stats[index]
            cursor.execute(insert_stmt, kda_data)
        
            # DAMAGE STATS
            insert_stmt = '''
            INSERT INTO damage_stats
            (game_id,physicalDamageDealt,physicalDamageDealtToChampions,physicalDamageTaken,
            magicDamageDealt,magicDamageDealtToChampions,magicDamageTaken,trueDamageDealt,
            trueDamageDealtToChampions,trueDamageTaken,totalDamageDealt,totalDamageDealtToChampions,
            totalDamageTaken,totalHeal,totalHealsOnTeammates,totalTimeCCDealt,largestCriticalStrike,
            damageSelfMitigated,damageDealtToBuildings,damageDealtToObjectives,damageDealtToTurrets) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''.strip()
            damage_data = (game_id,) + damage_stats[index]
            cursor.execute(insert_stmt, damage_data)
            
            # MISC STATS
            insert_stmt = '''
            INSERT INTO misc_stats
            (game_id,goldEarned,goldSpent,itemsPurchased,visionScore,visionWardsBoughtInGame,
            sightWardsBoughtInGame,wardsKilled,wardsPlaced) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''.strip()
            misc_data = (game_id,) + misc_stats[index]
            cursor.execute(insert_stmt, misc_data)
        
        # Commit the transaction
        conn.commit()
    except Exception as e:
        print("Error:", str(e))
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# START ---------------------------------------------------------------------------------------------------

with DAG(
    'riot_games_api',
        default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Riot API calls DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tages=['example']
) as dag:
    
    get_riot_match_history_data_task = PythonOperator(
        task_id='get_riot_data_task',
        python_callable=get_riot_data,
        dag=dag,
    )

    add_riot_data_task = PythonOperator(
        task_id='process_riot_data_task',
        python_callable=add_data_database,
        dag=dag,
    )
    
    copy_wsl_db_win = BashOperator(
        task_id='copy_file_wsl_win',
        depends_on_past=False,
        bash_command='cp /home/alextran/projects/SQLite-databases/summoner-game-data.db /mnt/c/Projects/airflow_projects/summoner_game_data/',
         # override the retries parameter with 3
        retries=3,
    )
    
    get_riot_match_history_data_task >> add_riot_data_task >> copy_wsl_db_win
    