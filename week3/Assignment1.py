import psycopg2
import requests

""""
1. 헤더가 레코드로 추가되는 문제 해결하기
2. Idempotent하게 잡을 만들기 (full refresh잡이라고 가정)
3. (Optional) Transaction을 사용해보기
"""


def get_redshift_connection():
    """
    Redshift connection 함수
    """
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    user = "gracia10"
    password = "Gracia10!1"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={password} port={port}")
    conn.set_session(autocommit=True)
    return conn


def extract(url):
    req = requests.get(url)
    return req.text


def transform(text):
    lines = text.split("\n")
    header = "name,gender"
    return lines[1:] if lines[0].lower() == header else lines


def load(lines):
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
                print(f"[DatabaseError]{err} :: sql -> {sql}")
    conn.close()


link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
data = extract(link)
lines = transform(data)
load(lines)
