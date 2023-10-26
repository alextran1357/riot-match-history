from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    
    # need to handle some way of duplicate matches if i rerun the dag
    for match_id in match_ids:
        match_details = get_match_details(match_id, riot_api_key)
        
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
                    # GAME STATS -------------------------------------
                    win = participant_data.get('win')
                    gameEndedInSurrender = participant_data.get('gameEndedInSurrender')
                    timePlayed = participant_data.get('timePlayed')
                    gameMode = participant_data.get('gameMode')
                    lane = participant_data.get('lane')
                    role = participant_data.get('role')
                    
                    # CHAMPION STATS -------------------------------------
                    championName = participant_data.get('championName')
                    champLevel = participant_data.get('champLevel')
                    
                    # KILL/ASSIST/DEATH STATS -------------------------------------
                    kills = participant_data.get('kills')
                    assists = participant_data.get('assists')
                    deaths = participant_data.get('deaths')
                    totalTimeSpentDead = participant_data.get('totalTimeSpentDead')
                    doubleKills = participant_data.get('doubleKills')
                    tripleKills = participant_data.get('tripleKills')
                    quadraKills = participant_data.get('quadraKills')
                    pentaKills = participant_data.get('pentaKills')
                    totalMinionsKilled = participant_data.get('totalMinionsKilled')
                    totalAllyJungleMinionsKilled = participant_data.get('totalAllyJungleMinionsKilled')
                    dragonKills = participant_data.get('dragonKills')
                    baronKills = participant_data.get('baronKills')
                    largestMultiKill = participant_data.get('largestMultiKill')
                    largestKillingSpree = participant_data.get('largestKillingSpree')
                    
                    # DAMAGE STATS -------------------------------------
                    physicalDamageDealt = participant_data.get('physicalDamageDealt')
                    physicalDamageDealtToChampions = participant_data.get('physicalDamageDealtToChampions')
                    physicalDamageTaken = participant_data.get('physicalDamageTaken')
                    magicDamageDealt = participant_data.get('magicDamageDealt')
                    magicDamageDealtToChampions = participant_data.get('magicDamageDealtToChampions')
                    magicDamageTaken = participant_data.get('magicDamageTaken')
                    trueDamageDealt = participant_data.get('trueDamageDealt')
                    trueDamageDealtToChampions = participant_data.get('trueDamageDealtToChampions')
                    trueDamageTaken = participant_data.get('trueDamageTaken')
                    totalDamageDealt = participant_data.get('totalDamageDealt')
                    totalDamageDealtToChampions = participant_data.get('totalDamageDealtToChampions')
                    totalDamageTaken = participant_data.get('totalDamageTaken')
                    totalHeal = participant_data.get('totalHeal')
                    totalHealsOnTeammates = participant_data.get('totalHealsOnTeammates')
                    totalTimeCCDealt = participant_data.get('totalTimeCCDealt')
                    largestCriticalStrike = participant_data.get('largestCriticalStrike')
                    damageSelfMitigated = participant_data.get('damageSelfMitigated')
                    damageDealtToBuildings = participant_data.get('damageDealtToBuildings')
                    damageDealtToObjectives = participant_data.get('damageDealtToObjectives')
                    damageDealtToTurrets = participant_data.get('damageDealtToTurrets')
                    
                    # MISC STATS ------------------------------------- 8
                    goldEarned = participant_data.get('goldEarned')
                    goldSpent = participant_data.get('goldSpent')
                    itemsPurchased = participant_data.get('itemsPurchased')
                    visionScore = participant_data.get('visionScore')
                    visionWardsBoughtInGame = participant_data.get('visionWardsBoughtInGame')
                    sightWardsBoughtInGame = participant_data.get('sightWardsBoughtInGame')
                    wardsKilled = participant_data.get('wardsKilled')
                    wardsPlaced = participant_data.get('wardsPlaced')
                else:
                    logger.error("Participants data not found or index out of range.")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {str(e)}")

def process_riot_data(**kwargs):
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

    process_riot_data_task = PythonOperator(
        task_id='process_riot_data_task',
        python_callable=process_riot_data,
        dag=dag,
    )
    
    t2 = BashOperator(
        task_id='copy_file_wsl_win',
        depends_on_past=False,
        bash_command='cp /home/yourusername/somefile.txt /mnt/c/Users/YourWindowsUsername/Desktop/',
         # override the retries parameter with 3
        retries=3,
    )
    
    get_riot_match_history_data_task >> process_riot_data_task >> t2
    