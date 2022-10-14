from datetime import datetime
import logging
import requests
import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

"""
[Full Refresh DAG]
openweather API를 이용하여, 
현재일 기준 7일 간 서울의 기온 정보를 저장 하는 파이프라인 구축 
"""


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.set_session(autocommit=True)
    return conn


def extract(**context):
    logging.info("Extract started")

    key = context["params"]["key"]
    url = context["params"]["url"]
    logging.info(url + key)

    f = requests.get(url + key)
    f_json = f.json()

    logging.info("Extract done")
    return f_json


def transform(**context):
    logging.info("transform started")
    json = context['task_instance'].xcom_pull(key="return_value", task_ids="extract")
    daily = json["daily"]
    lines = []

    # 날짜 , 낮온도, 최소온도, 최대온도 추출
    for d in daily[1:]:
        lines.append((
            datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d'),
            d["temp"]["day"],
            d["temp"]["min"],
            d["temp"]["max"]
        ))

    logging.info(f"transform done :: {lines}")
    return lines


def load(**context):
    logging.info("load started")
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    conn = get_Redshift_connection()
    cur = conn.cursor()

    sql = f"BEGIN;DELETE FROM {schema}.{table};"
    for l in lines:
        (dt, day, min, max) = l
        sql += f"INSERT INTO {schema}.{table} VALUES ('{dt}', '{day}', '{min}', '{max}');"
    sql += "END;"

    try:
        logging.info(sql)
        cur.execute(sql)
    except (Exception, psycopg2.DatabaseError) as err:
        logging.warning(err)
        cur.execute("ROLLBACK;")
    finally:
        conn.close()
        logging.info("load done")


dag_seoul_weather = DAG(
    dag_id='seoul_weather_v1',
    start_date=datetime(2022, 10, 10),
    schedule_interval='0 2 * * *',
    catchup=False)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'key': Variable.get("open_weather_api_key"),
        'url': Variable.get("open_weather_seoul_api")
    },
    dag=dag_seoul_weather)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag_seoul_weather)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'gracia10',
        'table': 'weather_forecast'
    },
    dag=dag_seoul_weather)

extract >> transform >> load
