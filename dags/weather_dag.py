import datetime
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import psycopg2
import os

def extract_data(**kwargs):
    ti = kwargs['ti']
    API_KEY = 'Here is your API KEY'
    url = f'https://api.tomorrow.io/v4/timelines?apikey={API_KEY}'
    p = {'location': [53.9, 27.5], \
          'fields' : [ \
            "precipitationIntensity", \
            "precipitationType", \
            "windSpeed", \
            "windGust", \
            "windDirection", \
            "temperature", \
            "temperatureApparent", \
            "cloudCover", \
            "cloudBase", \
            "cloudCeiling", \
            "weatherCode", \
                    ], \
          'timesteps' : '1h', \
          'units' : 'metric', \
     }
    
    response = requests.get(url, params=p)
    
    if response.status_code == 200:
        data = response.json()
        ti.xcom_push(key='weather_info_json', value=data)

def transform_data(**kwargs):

    ti = kwargs['ti']
    data = ti.xcom_pull(key='weather_info_json', task_ids='extracting_data')

    intervals = data['data']['timelines'][0]['intervals']
    
    data_list = []
    for interval in intervals:
        interval_data = {
            'startTime': "'" + interval['startTime'][:10] + "'",
            'hour': "'" + str(interval['startTime'][11:16]) + "'",
            'cloudBase': interval['values']['cloudBase'],
            'cloudCeiling': interval['values']['cloudCeiling'],
            'cloudCover': interval['values']['cloudCover'],
            'precipitationIntensity': interval['values']['precipitationIntensity'],
            'precipitationType': interval['values']['precipitationType'],
            'temperature': interval['values']['temperature'],
            'temperatureApparent': interval['values']['temperatureApparent'],
            'weatherCode': interval['values']['weatherCode'],
            'windDirection': interval['values']['windDirection'],
            'windGust': interval['values']['windGust'],
            'windSpeed': interval['values']['windSpeed']
        }
        data_list.append(interval_data)

    df = pd.DataFrame(data_list)
    
    ti.xcom_push(key='weather_data_transform', value=df)

def load_data(**kwargs):
    
    ti = kwargs['ti']
    data = ti.xcom_pull(key='weather_data_transform', task_ids='transform_data')
    data = data.fillna('NULL')
    
    query = "INSERT INTO WeatherInfo(" + ", ".join(list(data.columns)) + ") VALUES "

    for i, row in data.iterrows():
        print(row)
        string = "( "
        string += ", ".join(list(map(str, row))) + "),"
        query += string

    query = query.rstrip(',')

    table_query = """CREATE TABLE IF NOT EXISTS WeatherInfo(
                 startTime DATE,
                 hour VARCHAR(5),
                 cloudBase NUMERIC(5, 2),
                 cloudCeiling NUMERIC(5, 2),
                 cloudCover NUMERIC(5, 2),
                 precipitationIntensity INT,
                 precipitationType INT,
                 temperature NUMERIC(5, 2),
                 temperatureApparent NUMERIC(5, 2),
                 weatherCode INT,
                 windDirection NUMERIC(5, 2),
                 windGust NUMERIC(5, 2),
                 windSpeed NUMERIC(5, 2)
        )"""

    connection = psycopg2.connect(
        database='Here is your database name',
        user='Here is your user name',
        password='Here is your password for user',
        host='postgres',
    )

    cursor = connection.cursor()
    
    cursor.execute(table_query)
    connection.commit()

    cursor.execute(query)
    connection.commit()

    connection.close()



with DAG(
    dag_id="weather_ETL",
    start_date=datetime.datetime(2023, 10, 18),
    schedule="@daily",
    catchup=False
) as dag:
    extracting_data = PythonOperator(
        task_id = 'extracting_data',
        python_callable=extract_data
    )

    transforming_data = PythonOperator(
        task_id = 'transform_data',
        python_callable=transform_data
    )

    loading_data = PythonOperator(
        task_id = 'load_data',
        python_callable=load_data
    )

    extracting_data >> transforming_data >> loading_data