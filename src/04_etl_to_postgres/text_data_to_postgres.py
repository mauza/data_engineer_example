import os
import sys
sys.path.append('..')
import psycopg2
from time import sleep
from dotenv import load_dotenv

from src.utils import run_sql

load_dotenv()

POSTGRES_DB = os.getenv('postgres_db')
POSTGRES_HOST = os.getenv('postgres_host')
POSTGRES_PORT = os.getenv('postgres_port')
POSTGRES_USER = os.getenv('postgres_user')
POSTGRES_PASSWORD = os.getenv('postgres_password')
POSTGRES_TABLE = 'test'

DATA_DIR = '../data/job_descriptions'


def files_at_location(path):
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    return files


def insert_values(url, key, python_count, java_count):
    with psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
    ) as conn:
        print(f' ----- Inserting values: count_python={python_count}; count_java={java_count} ----- ')
        sql = f"INSERT INTO {POSTGRES_TABLE}(url, key, count_python, count_java) values ('{url}', '{key}', {python_count}, {java_count})"
        run_sql(conn, sql)


def process_file(file):
    print(f' ----- Processing file: {file} ----- ')
    with open(f'{DATA_DIR}/{file}', 'r') as f:
        text_lines = f.readlines()
    url = text_lines[0]
    key = text_lines[1]
    # I'm combining the list of lines into one text,
    # making it lowercase and splitting it again this time into words not lines.
    text_words = '\n'.join(text_lines).lower().split()
    python_count = text_words.count('python')
    java_count = text_words.count('java')
    insert_values(url, key, python_count, java_count)


def process_files():
    print(' ----- Starting process to read flat files ----- ')
    files_processed = []
    while True:
        try:
            all_files = files_at_location(DATA_DIR)
            files_to_process = [f for f in all_files if f not in files_processed]
            for file in files_to_process:
                process_file(file)
                files_processed.append(file)
            sleep(2)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    process_files()
