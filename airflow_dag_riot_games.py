from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
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
    
    # need to handle some way of duplicate matches if i rerun the dag
    for match_id in match_ids:
        match_details = get_match_details(match_id, riot_api_key)
        # Grab specific data
        '''
        check puuid
        
        GAME STATS -------------------------------------
        gameCreation - long
        win - boolean
        gameEndedInSurrender - boolean
        timePlayed - int
        gameMode - string
        lane - string
        role - string
        
        CHAMPION STATS -------------------------------------
        championName - string
        champLevel - int
        
        KILL/ASSIST/DEATH STATS -------------------------------------
        kills - int
        assists - int
        deaths - int
        totalTimeSpentDead - int
        doubleKills - int
        tripleKills - int
        quadraKills - int
        pentaKills - int
        totalMinionsKilled - int
        totalAllyJungleMinionsKilled - int
        dragonKills - int
        baronKills - int
        largestMultiKill - int
        largestKillingSpree - int
        
        DAMAGE STATS -------------------------------------
        physicalDamageDealt - int
        physicalDamageDealtToChampions - int
        physicalDamageTaken - int
        magicDamageDealt - int	
        magicDamageDealtToChampions - int	
        magicDamageTaken - int	
        trueDamageDealt - int
        trueDamageDealtToChampions - int
        trueDamageTaken - int
        totalDamageDealt - int
        totalDamageDealtToChampions - int
        totalDamageTaken - int
        totalHeal - int
        totalHealsOnTeammates - int
        totalTimeCCDealt - int
        largestCriticalStrike - int
        damageSelfMitigated - int
        damageDealtToBuildings - int
        damageDealtToObjectives - int
        damageDealtToTurrets - int
        
        MISC STATS -------------------------------------
        goldEarned - int
        goldSpent - int
        itemsPurchased - int
        visionScore - int
        visionWardsBoughtInGame - int
        sightWardsBoughtInGame - int
        wardsKilled - int
        wardsPlaced - int
        '''

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
    