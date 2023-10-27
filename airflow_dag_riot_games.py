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
    
    current_batch = []
    
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
                    
                    # CHAMPION STATS -------------------------------------
                    championName = grab_data_with_logging(logger, participant_data, 'championName')
                    champLevel = grab_data_with_logging(logger, participant_data, 'champLevel')
                    
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
                    
                    # MISC STATS ------------------------------------- 8
                    goldEarned = grab_data_with_logging(logger, participant_data, 'goldEarned')
                    goldSpent = grab_data_with_logging(logger, participant_data, 'goldSpent')
                    itemsPurchased = grab_data_with_logging(logger, participant_data, 'itemsPurchased')
                    visionScore = grab_data_with_logging(logger, participant_data, 'visionScore')
                    visionWardsBoughtInGame = grab_data_with_logging(logger, participant_data, 'visionWardsBoughtInGame')
                    sightWardsBoughtInGame = grab_data_with_logging(logger, participant_data, 'sightWardsBoughtInGame')
                    wardsKilled = grab_data_with_logging(logger, participant_data, 'wardsKilled')
                    wardsPlaced = grab_data_with_logging(logger, participant_data, 'wardsPlaced')
                    
                else:
                    logger.error("Participants data not found or index out of range.")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {str(e)}")
    return current_batch

def extract_data_from_dict(batch_data_list, index):
    for batch_list in batch_data_list:
        
    game_stats_batch_dict = batch_data_list[index]
    for key, value in game_stats_batch_dict.items():

def add_data_database(batch_data_list):
    hook = SqliteHook(sqlite_conn_id='lol_summoner_matches_sqlite')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        # GAME STATS insert
        insert_stmt = '''
        INSERT INTO game_stats
        (gameCreation,win,gameEndedInSurrender,timePlayed,gameMode,lane,role) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        '''.strip()
        # Execute the GAME STATS insert statement
        game_stats_batch_dict = batch_data_list[0]
        for key, value in game_stats_batch_dict.items():
        
        cursor.executemany(insert_stmt, batch_data)
            
        
        
        
        
        
        
        # Commit the transaction
        conn.commit()
    except Exception as e:
        # If an error occurs, rollback the transaction
        conn.rollback()
        print("Error:", str(e))
    
    finally:
        # Close the cursor and connection
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
        bash_command='cp /home/yourusername/somefile.txt /mnt/c/Users/YourWindowsUsername/Desktop/',
         # override the retries parameter with 3
        retries=3,
    )
    
    get_riot_match_history_data_task >> add_riot_data_task >> copy_wsl_db_win
    