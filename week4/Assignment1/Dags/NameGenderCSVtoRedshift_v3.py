from airflow import DAG
from airflow.models import Variable
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


def extract(**context):
    logging.info("Extract started")
    logging.info(context['task_instance'])

    link = context["params"]["url"]
    f = requests.get(link)
    logging.info("Extract done")
    return (f.text)


def transform(**context):
    logging.info("transform started")
    logging.info(context['task_instance'])

    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")[1:]
    logging.info("transform done")
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
        if l != '':
            (name, gender) = l.split(",")
            sql += f"INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"
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


dag_second_assignment = DAG(
    dag_id='second_assignment_v3',
    start_date=datetime(2022, 10, 10),
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    })

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'url': Variable.get("csv_url")
    },
    dag=dag_second_assignment)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag_second_assignment)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'gracia10',
        'table': 'name_gender'
    },
    dag=dag_second_assignment)

extract >> transform >> load
