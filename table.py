import datetime
import os
import time

import psycopg2
import requests
import schedule as schedule

import config as cfg

DATA_TIME = datetime.datetime.now()

# func return dict from api url
def get_data_from_api():
    # init API connection
    response = requests.get(cfg.API)
    # check connection status
    print(f'status API connect : {response.status_code}')
    if response.status_code != 200:
        data = response.text
        # print(f'data from API :\n{data}')
        pass
    else:
        data = response.json()['data']  # dict type
        # print(f'data from API :\n{data}')
        return data


# connect to the cluster in YC VM
def create_connection_to_psql(host, port, sslmode, dbname, user, password):
    conn = psycopg2.connect(f"""
            host={host}
            port={port}
            sslmode={sslmode}
            dbname={dbname}
            user={user}
            password={password}
            target_session_attrs=read-write
            """)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('SELECT version()')
        print(f'[INFO] : {DATA_TIME} Server version : \n{cur.fetchone()}')
        return conn


def create_table():
    try:
        conn = create_connection_to_psql(
            cfg.HOST,
            cfg.PORT,
            cfg.SSLMODE,
            cfg.DBNAME,
            cfg.USER,
            cfg.PASSWORD
        )
        conn.autocommit = True
    # create table
        with conn.cursor() as cur:
            sql_query = f"""
            CREATE TABLE IF NOT EXISTS {cfg.DB_TABLE_NAME}(
            id VARCHAR, 
            symbol VARCHAR, 
            currencySymbol VARCHAR, 
            type VARCHAR, 
            rateUsd VARCHAR
            );"""
            cur.execute(sql_query)
            print(f'[INFO] : {DATA_TIME} Table created successful')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} Error connection : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} Psql connection closed')


def insert_data_to_table():
    try:
        conn = create_connection_to_psql(
            cfg.HOST,
            cfg.PORT,
            cfg.SSLMODE,
            cfg.DBNAME,
            cfg.USER,
            cfg.PASSWORD
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            sql_query = f"""
            INSERT INTO {cfg.DB_TABLE_NAME} 
            VALUES(
            %(id)s, 
            %(symbol)s, 
            %(currencySymbol)s, 
            %(type)s, 
            %(rateUsd)s
            );"""
            data = (get_data_from_api())
            cur.execute(sql_query, data)
            print(f'''[INFO] : {DATA_TIME} Data RATE_USD <{data['rateUsd']}> inserted in the table <{cfg.DB_TABLE_NAME}>''')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} Error connection : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} Psql connection closed')


count = 0

if __name__ == '__main__':
    print('')
    # create_table()
    while True:
        count += 1
        print(f'Loading data.......... \nNumber of loading {count}')
        insert_data_to_table()
        time.sleep(30)