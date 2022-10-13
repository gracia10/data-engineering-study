import psycopg2
import requests
import configparser
import logging

logging.basicConfig(level=logging.INFO)


def get_redshift_connection():
    config = configparser.ConfigParser()
    config.read('../config/config.ini')
    conn = psycopg2.connect(f"dbname={config['redshift']['dbname']} "
                            f"user={config['redshift']['user']} "
                            f"host={config['redshift']['host']} "
                            f"password={config['redshift']['password']} "
                            f"port={config['redshift']['port']}")
    conn.set_session(autocommit=True)
    return conn


def extract(url):
    logging.info("Extract started")
    req = requests.get(url)
    logging.info("Extract done")
    return req.text


def transform(text):
    logging.info("transform started")
    header = "name,gender"
    lines = text.split("\n")
    lines = lines[1:] if lines[0].lower() == header else lines
    logging.info("transform done")
    return lines


def load(lines):
    logging.info("load started")
    with get_redshift_connection() as conn:
        with conn.cursor() as cur:
            try:
                sql = "DELETE FROM gracia10.name_gender"
                cur.execute(sql)

                for r in lines:
                    if r != '':
                        (name, gender) = r.split(",")
                        sql = f"INSERT INTO gracia10.name_gender VALUES ('{name}', '{gender}')"
                        cur.execute(sql)
            except psycopg2.DatabaseError as err:
                logging.warning(f"[DatabaseError]{err} :: sql -> {sql}")
    conn.close()
    logging.info("load done")


link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
data = extract(link)
lines = transform(data)
load(lines)
