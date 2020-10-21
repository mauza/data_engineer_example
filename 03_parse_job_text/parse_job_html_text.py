import os
import sys
sys.path.append('..')
import urllib.parse
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from utils import create_driver, html_from_url, generate_kafka_consumer

load_dotenv()

INDEED_HOST = 'https://www.indeed.com'

KAFKA_BROKER = os.getenv('kafka_broker')
SCHEMA_REGISTRY = os.getenv('schema_registry')
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


def main():
    consumer = generate_kafka_consumer(KAFKA_BROKER, KAFKA_SCHEMA_STR, 'group2')
    consumer.subscribe([KAFKA_TOPIC])
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            url = msg.value()
            if url is not None:
                print(url)
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == "__main__":
    main()