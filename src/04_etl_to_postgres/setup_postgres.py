import os
import sys
sys.path.append('..')
import psycopg2
from dotenv import load_dotenv

from src.utils import run_sql

load_dotenv()

POSTGRES_DB = os.getenv('postgres_db')
POSTGRES_HOST = os.getenv('postgres_host')
POSTGRES_PORT = os.getenv('postgres_port')
POSTGRES_USER = os.getenv('postgres_user')
POSTGRES_PASSWORD = os.getenv('postgres_password')
POSTGRES_TABLE = 'test'

CREATE_DDL = f'CREATE TABLE {POSTGRES_TABLE} (id serial PRIMARY KEY, url varchar(2000), key varchar(2000), count_python integer, count_java integer);'
DELETE_DDL = f'DROP TABLE IF EXISTS {POSTGRES_TABLE}'

def run_ddl(ddl):
    with psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
    ) as conn:
       run_sql(conn, ddl)

def recreate_table():
    run_ddl(DELETE_DDL)
    run_ddl(CREATE_DDL)

if __name__ == '__main__':
    recreate_table()
