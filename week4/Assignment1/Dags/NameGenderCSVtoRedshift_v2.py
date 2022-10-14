from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2


def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "gracia10"
    redshift_pass = "Gracia10!1"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
    return conn


def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    logging.info("Extract done")
    return (f.text)


def transform(text):
    logging.info("transform started")
    lines = text.split("\n")[1:]
    logging.info("transform done")
    return lines


def load(lines):
    logging.info("load started")
    conn = get_Redshift_connection()
    cur = conn.cursor()
    sql = "BEGIN;DELETE FROM gracia10.name_gender;"
    for l in lines:
        if l != '':
            (name, gender) = l.split(",")
            sql += f"INSERT INTO gracia10.name_gender VALUES ('{name}', '{gender}');"
    sql += "END;"
    logging.info(sql)
    try:
        cur.execute(sql)
    except (Exception, psycopg2.DatabaseError) as err:
        logging.warning(err)
        cur.execute("ROLLBACK;")
    finally:
        logging.info("load done")
        conn.close()


def etl(**context):
    link = context["params"]["url"]

    task_instance = context['task_instance']
    logging.info(task_instance)

    execution_date = context['execution_date']
    logging.info(execution_date)

    data = extract(link)
    lines = transform(data)
    load(lines)


dag_second_assignment = DAG(
    dag_id='second_assignment_v2',
    start_date=datetime(2022, 10, 10),
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    })

task = PythonOperator(
    task_id='perform_etl',
    python_callable=etl,
    params={
        'url': "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    },
    dag=dag_second_assignment)
