from datetime import datetime
import logging
import requests
import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

"""
[Incremental Update DAG] 
openweather API를 이용하여, 
현재일 기준 7일 간 서울의 기온 정보를 저장 하는 파이프라인 구축 
"""


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    return conn.cursor()


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
    lines = []

    # 날짜 , 낮온도, 최소온도, 최대온도 추출
    for d in json["daily"][1:]:
        ymd = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        lines.append("('{}',{},{},{})".format(ymd, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

    logging.info(f"transform done :: {lines}")
    return lines


def load(**context):
    logging.info("load started")
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    cur = get_Redshift_connection()

    # 임시 테이블 생성
    create_sql = f"DROP TABLE IF EXISTS {schema}.temp_{table};" \
                 f"CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS );" \
                 f"INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};"
    logging.info(create_sql)
    try:
        cur.execute(create_sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as err:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블 데이터 입력
    insert_sql = f"INSERT INTO {schema}.temp_{table} VALUES " + ",".join(lines)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as err:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 모든 레코드 제거 후, 중복을 없앤 데이터 입력
    alter_sql = f"DELETE FROM {schema}.{table};" \
                f"INSERT INTO {schema}.{table} " \
                f"SELECT date, temp, min_temp, max_temp, created_date " \
                f"FROM " \
                f"(SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq " \
                f"FROM {schema}.temp_{table}) " \
                f"WHERE seq = 1;"
    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as err:
        cur.execute("ROLLBACK;")
        raise
    finally:
        logging.info("load done")


dag_seoul_weather = DAG(
    dag_id='seoul_weather_v2',
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
