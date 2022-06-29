import datetime
import psycopg2
import requests
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
        print(f'data from API :\n{data}')
        pass
    else:
        data = response.json()['data']  # dict type

        print(f'data from API :\n{data}')
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


def create_table_query():
    return f"""CREATE TABLE IF NOT EXISTS {cfg.DB_TABLE_NAME}(
                id VARCHAR ,
                symbol VARCHAR,
                currencySymbol VARCHAR ,
                type VARCHAR ,
                rateUsd VARCHAR);"""


def insert_query():
    return f"""INSERT INTO {cfg.DB_TABLE_NAME}
    VALUES (%(id)s, %(symbol)s, %(currencySymbol)s, %(type)s, %(rateUsd)s);"""


# func include connection to DB
def handle_table():
    data = (get_data_from_api())

    # try connection to DB
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
            cur.execute(create_table_query())
            print(f'[INFO] : {DATA_TIME} Table created successful')

        # insert data into table
        with conn.cursor() as cur:
            cur.execute(insert_query(), data)
            print(f'[INFO] : {DATA_TIME} Data <{data}> inserted in the table <{cfg.DB_TABLE_NAME}>')

    except Exception as e:
        print(f'[INFO] : {DATA_TIME} Error connection : ', e)
    finally:
        if conn:
            conn.close()
            print(f'[INFO] : {DATA_TIME} Psql connection closed')


if __name__ == '__main__':
    handle_table()
