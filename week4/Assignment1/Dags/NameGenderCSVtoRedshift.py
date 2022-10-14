from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
import psycopg2


def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    user = "gracia10"
    password = "Gracia10!1"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={password} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()


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
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM gracia10.name_gender;"
    for l in lines:
        if l != '':
            (name, gender) = l.split(",")
            sql += f"INSERT INTO gracia10.name_gender VALUES ('{name}', '{gender}');"
    sql += "END;"
    cur.execute(sql)
    logging.info(sql)
    logging.info("load done")


def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)


dag_second_assignment = DAG(
    dag_id='second_assignment',
    catchup=False,
    start_date=datetime(2022, 10, 10),
    schedule_interval='0 2 * * *')

task = PythonOperator(
    task_id='perform_etl',
    python_callable=etl,
    dag=dag_second_assignment)
