from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sqlite3
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

def grab_data_with_logging_dictionary(logger, dict, data, key):
    value = data.get(key)
    if value is None:
        logger.warning(f"The key {key} is missing from the data")
    dict[key] = value
    return dict

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
                    game_stats = dict()
                    champion_stats = dict()
                    kda_stats = dict()
                    damage_stats = dict()
                    misc_stats = dict()
                    
                    # GAME STATS -------------------------------------
                    game_stats = grab_data_with_logging_dictionary(logger, game_stats, participant_data, 'win')
                    game_stats = grab_data_with_logging_dictionary(logger, game_stats, participant_data, 'gameEndedInSurrender')
                    game_stats = grab_data_with_logging_dictionary(logger, game_stats, participant_data, 'timePlayed')
                    game_stats = grab_data_with_logging_dictionary(logger, game_stats, participant_data, 'gameMode')
                    game_stats = grab_data_with_logging_dictionary(logger, game_stats, participant_data, 'lane')
                    game_stats = grab_data_with_logging_dictionary(logger, game_stats, participant_data, 'role')
                    
                    # CHAMPION STATS -------------------------------------
                    champion_stats = grab_data_with_logging_dictionary(logger, champion_stats, participant_data, 'championName')
                    champion_stats = grab_data_with_logging_dictionary(logger, champion_stats, participant_data, 'champLevel')
                    
                    # KILL/ASSIST/DEATH STATS -------------------------------------
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'kills')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'assists')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'deaths')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'totalTimeSpentDead')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'doubleKills')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'tripleKills')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'quadraKills')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'pentaKills')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'totalMinionsKilled')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'totalAllyJungleMinionsKilled')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'dragonKills')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'baronKills')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'largestMultiKill')
                    kda_stats = grab_data_with_logging_dictionary(logger, kda_stats, participant_data, 'largestKillingSpree')
                    
                    # DAMAGE STATS -------------------------------------
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'physicalDamageDealt')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'physicalDamageDealtToChampions')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'physicalDamageTaken')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'magicDamageDealt')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'magicDamageDealtToChampions')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'magicDamageTaken')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'trueDamageDealt')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'trueDamageDealtToChampions')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'trueDamageTaken')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'totalDamageDealt')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'totalDamageDealtToChampions')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'totalDamageTaken')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'totalHeal')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'totalHealsOnTeammates')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'totalTimeCCDealt')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'largestCriticalStrike')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'damageSelfMitigated')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'damageDealtToBuildings')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'damageDealtToObjectives')
                    damage_stats = grab_data_with_logging_dictionary(logger, damage_stats, participant_data, 'damageDealtToTurrets')
                    
                    # MISC STATS ------------------------------------- 8
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'goldEarned')
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'goldSpent')
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'itemsPurchased')
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'visionScore')
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'visionWardsBoughtInGame')
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'sightWardsBoughtInGame')
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'wardsKilled')
                    misc_stats = grab_data_with_logging_dictionary(logger, misc_stats, participant_data, 'wardsPlaced')
                    
                    # insert batch
                    batch = [game_stats, champion_stats, kda_stats, damage_stats, misc_stats]
                    current_batch.append(batch)
                else:
                    logger.error("Participants data not found or index out of range.")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {str(e)}")
    return current_batch

def add_data_database(**kwargs):
    task_instance = kwargs['ti']
    data = task_instance.xcom_pull(task_ids='get_riot_data_task')
    # Do something with the data
    print(data)

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
    