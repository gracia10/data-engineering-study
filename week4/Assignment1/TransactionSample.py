import psycopg2
import configparser
import logging

logging.basicConfig(level=logging.INFO)

"""
트랜잭션 구현 실습
"""


def get_redshift_connection(autocommit):
    config = configparser.ConfigParser()
    config.read('../config/config.ini')
    conn = psycopg2.connect(f"dbname={config['redshift']['dbname']} "
                            f"user={config['redshift']['user']} "
                            f"host={config['redshift']['host']} "
                            f"password={config['redshift']['password']} "
                            f"port={config['redshift']['port']}")
    conn.set_session(autocommit=autocommit)
    return conn


# INSERT SQL을 autocommit=False로 실행
def case1():
    logging.info("INSERT SQL을 autocommit=False로 실행")
    conn = get_redshift_connection(False)
    cur = conn.cursor()
    cur.execute("DELETE FROM gracia10.name_gender;")
    cur.execute("INSERT INTO gracia10.name_gender VALUES ('case1', 'Female');")
    cur.execute("SELECT * FROM gracia10.name_gender LIMIT 10;")
    res = cur.fetchall()
    for r in res:
        print(r)
    cur.execute("COMMIT;")
    conn.close()


# INSERT SQL을 autocommit=False로 실행하고 try/except로 컨트롤하기
def case2():
    logging.info("INSERT SQL을 autocommit=False로 실행하고 try/except로 컨트롤하기")
    conn = get_redshift_connection(False)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM gracia10.name_gender;")
        cur.execute("INSERT INTO gracia10.name_gender VALUES ('case2', 'Female');")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
        conn.rollback()
    finally:
        conn.close()


# INSERT SQL을 autocommit=True로 실행하고 SQL로 컨트롤하기
def case3():
    conn = get_redshift_connection(True)
    cur = conn.cursor()
    cur.execute("BEGIN;")
    cur.execute("DELETE FROM gracia10.name_gender;")
    cur.execute("INSERT INTO gracia10.name_gender VALUES ('case3', 'Male');")
    cur.execute("END;")


# INSERT SQL을 autocommit=True로 실행하고 try/except로 컨트롤하기
def case4():
    logging.info("INSERT SQL을 autocommit=True로 실행하고 try/except로 컨트롤하기")
    conn = get_redshift_connection(True)
    cur = conn.cursor()
    cur.execute("DELETE FROM gracia10.name_gender;")
    try:
        cur.execute("BEGIN;")
        cur.execute("DELETE FROM gracia10.name_gender;")
        cur.execute("INSERT INTO gracia10.name_gender VALUES ('case4', 'Female');")
        cur.execute("END;")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)
        cur.execute("ROLLBACK;")
    finally:
        conn.close()


# 잘못된 SQL을 중간에 실행해보기
def case5():
    logging.info("잘못된 SQL을 중간에 실행해보기")
    conn = get_redshift_connection(True)
    cur = conn.cursor()
    cur.execute("BEGIN;")
    cur.execute("DELETE FROM gracia10.name_gender;")
    cur.execute("INSERT INTO gracia10.name_gender VALUES ('fail_case', 'NO!!!!!!!!!!');")
    cur.execute("END;")


# name_gender 10건 조회
def select_name_gender_list(info):
    conn = get_redshift_connection(False)
    cur = conn.cursor()
    logging.info(f"{info} name_gender list")
    cur.execute("SELECT * FROM gracia10.name_gender LIMIT 10;")
    res = cur.fetchall()
    for r in res:
        logging.info(r)


select_name_gender_list("Start")
case5()
select_name_gender_list("End")
