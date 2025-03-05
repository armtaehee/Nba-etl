from balldontlie import BalldontlieAPI
import pandas as pd
import numpy as np
import requests
import numpy as np
from bs4 import BeautifulSoup
from balldontlie.exceptions import RateLimitError
import json
import time
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pandas as pd
import sys
import io
import psycopg2
import requests

default_args={
    'owner':'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': days_ago(0),
    'e-mail': "armchairja@gmail.com",
}
dag = DAG(
    'nba-etl',
    default_args=default_args,
    description='nba-api-etl-dag',
    schedule_interval=timedelta(days=1),
)
def extract():
    api=BalldontlieAPI(api_key="05de7c55-7d1c-44c2-93be-c5f0629d9b9d")
    games=[]
    next_cursor = None
    
    while True:
        try:
            response=api.nba.games.list(cursor=next_cursor,seasons=[2024])
            games.extend(response.data)
            if not response.meta.next_cursor: 
                break
            next_cursor = response.meta.next_cursor
        except RateLimitError as e:
            print(f"Rate limit exceeded: {e}. Retrying in 60 seconds...")
            time.sleep(60)

    return games

def transform(data):
    Id=[]
    Date=[]
    Season=[]
    Home=[]
    HomeScore=[]
    Visitor=[]
    VisitorScore=[]
    for i in data:
        Id.append(i.id)
        Date.append(i.date)
        Season.append(i.season)
        Home.append(i.home_team.full_name)
        HomeScore.append(i.home_team_score)
        Visitor.append(i.visitor_team.full_name) 
        VisitorScore.append(i.visitor_team_score)
    df=pd.DataFrame({
        "ID":Id,
        "Date":Date,
        "Season":Season,
        "Home":Home,
        "Home_Score":HomeScore,
        "Visitor":Visitor,
        "Visitor_Score":VisitorScore,
    })
    return df

def load(df):
    db_name = "Nba_game"
    db_user = "postgres"
    db_password = "KONG4785"
    db_host = "localhost"  
    db_port = "5432"  

    try:
        engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        df.to_sql("nba_games", engine, if_exists="replace", index=False)
        print("Data successfully loaded to the PostgreSQL database.")
    except Exception as e:
        print(f"An error occurred while loading data: {e}")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

extract_task >> transform_task >> load_task

