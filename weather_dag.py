from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import json
import pandas as pd
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 24),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def kelvin_to_fahrenheit(temp_in_k):
    temp_in_f = (temp_in_k - 273.15) * (9/5) + 32
    return temp_in_f

def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids = "extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                    "Description": weather_description,
                    "Temperature (F)": temp_farenheit,
                    "Feels Like (F)": feels_like_farenheit,
                    "Minimun Temp (F)":min_temp_farenheit,
                    "Maximum Temp (F)": max_temp_farenheit,
                    "Pressure": pressure,
                    "Humidty": humidity,
                    "Wind Speed": wind_speed,
                    "Time of Record": time_of_record,
                    "Sunrise (Local Time)":sunrise_time,
                    "Sunset (Local Time)": sunset_time                        
                    }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = 'current_weather_data_sydney_' + dt_string + '.csv'
    df_data.to_csv(file_name, index=False)
    s3 = boto3.resource('s3')
    s3.Bucket('airflow-project-20230924').upload_file(file_name, f"airflow-project-20230924/{file_name}")
          
with DAG(
    'weather_dag',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False) as dag:

    is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weather_api',
        endpoint = '/data/2.5/weather?q=Sydney&appid=a273e4392c77a643fdf064d86b7a0b14'
    )

    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weather_api',
        endpoint = '/data/2.5/weather?q=Sydney&appid=a273e4392c77a643fdf064d86b7a0b14',
        method = 'GET',
        response_filter = lambda r: json.loads(r.text),
        log_response = True
    )

    transform_weather_data = PythonOperator(
        task_id = 'transform_weather_data',
        python_callable = transform_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_weather_data
