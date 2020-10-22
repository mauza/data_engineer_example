import os
import sys
sys.path.append('..')
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from src.utils import create_driver, html_from_url, generate_kafka_consumer

load_dotenv()

DATA_DIR = '../data/job_descriptions'

KAFKA_BROKER = os.getenv('kafka_broker')
KAFKA_TOPIC = 'indeed_data_job_urls'
KAFKA_SCHEMA_STR = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "indeed_job_url",
      "description": "Job url from indeed.com",
      "type": "object",
      "properties": {
        "url": {
          "description": "a job url",
          "type": "string"
        }
      },
      "required": [ "url" ]
    }
    """


def consume_topic(group_name):
    print(f' ----- Consuming topic {KAFKA_TOPIC} with group name: {group_name} -----')
    consumer = generate_kafka_consumer(KAFKA_BROKER, KAFKA_SCHEMA_STR, group_name)
    consumer.subscribe([KAFKA_TOPIC])
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            url = msg.value()
            key = msg.key()
            offset = msg.offset()
            partition = msg.partition()
            if url is not None:
                print(f'received and processing url: {url}')
                process_url(url, key, offset, partition)
        except KeyboardInterrupt:
            break

    consumer.close()


def job_description_from_url(url):
    driver = create_driver()
    try:
        html = html_from_url(url, driver)
    finally:
        driver.close()
    soup = BeautifulSoup(html, 'html.parser')
    job_div = soup.find('div', {'class': 'jobsearch-JobComponent icl-u-xs-mt--sm jobsearch-JobComponent-bottomDivider'})
    job_description = '\n'.join(list(job_div.strings))
    return job_description


def save_to_flat_file(data, filename):
    with open(f'{DATA_DIR}/{filename}.txt', 'w') as f:
        f.write(data)


def process_url(url, key, offset, partition):
    description = job_description_from_url(url)
    data = '\n'.join([url, key, description])
    filename = f'{partition}__{offset}'
    save_to_flat_file(data, filename)


if __name__ == "__main__":
    group_name = 'parse_job_urls_v1'
    consume_topic(group_name)
