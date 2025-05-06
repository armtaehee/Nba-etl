from balldontlie import BalldontlieAPI
from balldontlie.exceptions import RateLimitError
import pandas as pd
import time
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': days_ago(0),
    'email': ["armchairja@gmail.com"],
}

dag = DAG(
    'nba-etl-to-csv',
    default_args=default_args,
    description='Extract NBA game data and save as CSV',
    schedule_interval=timedelta(days=1),
)

def extract():
    api = BalldontlieAPI(api_key="05de7c55-7d1c-44c2-93be-c5f0629d9b9d")
    games = []
    next_cursor = None

    while True:
        try:
            response = api.nba.games.list(cursor=next_cursor, seasons=[2024])
            games.extend(response.data)
            if not response.meta.next_cursor:
                break
            next_cursor = response.meta.next_cursor
        except RateLimitError as e:
            print(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
            time.sleep(60)

    return games

def transform(**context):
    data = context['ti'].xcom_pull(task_ids='extract')
    Id, Date, Season, Home, HomeScore, Visitor, VisitorScore = [], [], [], [], [], [], []

    for i in data:
        Id.append(i.id)
        Date.append(i.date)
        Season.append(i.season)
        Home.append(i.home_team.full_name)
        HomeScore.append(i.home_team_score)
        Visitor.append(i.visitor_team.full_name)
        VisitorScore.append(i.visitor_team_score)

    df = pd.DataFrame({
        "ID": Id,
        "Date": Date,
        "Season": Season,
        "Home": Home,
        "Home_Score": HomeScore,
        "Visitor": Visitor,
        "Visitor_Score": VisitorScore,
    })
    return df.to_json()  

def load(**context):
    json_data = context['ti'].xcom_pull(task_ids='transform')
    df = pd.read_json(json_data)

    file_path = "../nba_games.csv"  
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
