import json
import csv
import os

from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from multi_query_http import MultiQueryHttpOperator

from airflow.providers.http.sensors.http import HttpSensor

CITIES = ['Surat' , 'Bangalore', 'Chennai' , 'Delhi', 'Punjab', 'Mumbai', 'Ahmedabad', 'Kolkata', 'Hyderabad'] #, 'Jaipur']


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 5, 18),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


def write_to_csv(**kwargs):
    """
    Pull data from XCom

    Process and Write weather data to csv
    """

    ti = kwargs['ti']
    jsondata = ti.xcom_pull(task_ids='get_weather_data')

    weather_data = []

    fieldnames = ["main", "description", "temp", "feels_like", "temp_min", "temp_max", "pressure", "humidity", "clouds", "dt", "name"]

    for data in jsondata:
        data = json.loads(data)
        tempdata = {**data['weather'][0], **data['main'], 'clouds': data['clouds']['all'], 'dt': data['dt'], 'name': data['name']}

        tempdata = {key: value for key, value in tempdata.items() if key in fieldnames}
        weather_data.append(tempdata)

    with open('/opt/airflow/dags/weather_files/latest-weather-data.csv', 'w') as f_output:
        dict_writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        dict_writer.writeheader()
        dict_writer.writerows(weather_data)


with DAG('weather_dag', default_args=default_args, schedule_interval='0 6 * * *', catchup=True) as dag:

    # Check if API is available
    is_weather_data_available = HttpSensor(
        task_id="is_weather_data_available",
        http_conn_id="rapid_api_connection",
        endpoint="/weather?q=Surat,IN",
        method='GET',
        response_check=lambda response: "weather" in response.text,
        poke_interval=5,
        timeout=20
    )

    queries = []
    for city in CITIES:
        queries.append('q=' + city + ",IN")

    # get weather data fro given cities using custom operator and push them into XCOM
    get_weather_data = MultiQueryHttpOperator(
        task_id='get_weather_data',
        http_conn_id='rapid_api_connection',
        endpoint="/weather",
        queries=queries,
        method='GET',
    )

    fill_up_weather_data_to_csv = PythonOperator(
        task_id='fill_up_weather_data_to_csv',
        dag=dag,
        python_callable=write_to_csv,
        provide_context=True,
    )

    # Create weather_data table
    create_weather_table = PostgresOperator(
        task_id="create_weather_table",
        sql="""
            CREATE TABLE IF NOT EXISTS weather_data (
            main VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            temp NUMERIC NOT NULL,
            feels_like NUMERIC NOT NULL,
            temp_min NUMERIC NOT NULL,
            temp_max NUMERIC NOT NULL,
            pressure NUMERIC NOT NULL,
            humidity NUMERIC NOT NULL,
            clouds NUMERIC NOT NULL,
            dt VARCHAR NOT NULL,
            name VARCHAR NOT NULL)
          """,
    )

    # Load weather data csv into the table
    copy_weather_data_to_table = PostgresOperator(
        task_id="copy_weather_data_to_table",
        sql="""
            COPY weather_data
            FROM '/opt/airflow/dags/weather_files/latest-weather-data.csv' 
            DELIMITER ',' 
            CSV HEADER;
          """,
    )
    
    # Independent tasks
    create_weather_table

    # Dependent tasks
    is_weather_data_available >> get_weather_data >> fill_up_weather_data_to_csv >> copy_weather_data_to_table
