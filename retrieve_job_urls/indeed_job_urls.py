import os
import urllib.parse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from dotenv import load_dotenv

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

load_dotenv()

KAFKA_BROKER = os.getenv('kafka_broker')
SCHEMA_REGISTRY = os.getenv('schema_registry')
KAFKA_TOPIC = 'indeed_data_job_urls'

INDEED_HOST = 'https://www.indeed.com'
JOBS_PER_PAGE = 10
GECKO_DRIVER_PATH = 'geckodriver'

def create_driver():
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options, executable_path=GECKO_DRIVER_PATH)
    return driver


def html_from_url(url, driver):
    driver.get(url)
    html = driver.page_source
    return html


def job_urls_from_html(html, ensure_query_contains='data'):
    soup = BeautifulSoup(html, 'html.parser')
    titles = soup.find_all('h2', {'class': 'title'})
    job_urls = []
    for job in titles:
        link = job.find('a')
        title = link.text
        if ensure_query_contains in title.lower():
            job_urls.append(link['href'])
    return job_urls


def num_jobs_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    job_count_div = soup.find('div', {'id': 'searchCountPages'})
    job_count = int(job_count_div.text.strip().split(' ')[3].replace(',', ''))
    return job_count


def find_job_urls(query, location, sort='date'):
    driver = create_driver()
    url_path = f"/jobs?q={urllib.parse.quote(query)}&l={location}&sort={sort}"
    try:
        html = html_from_url(f'{INDEED_HOST}{url_path}', driver)
        num_jobs = num_jobs_from_html(html)
        for i in range(0, num_jobs, JOBS_PER_PAGE):
            current_url_path = f'{url_path}&start={i}'
            url = f'{INDEED_HOST}{current_url_path}'
            html = html_from_url(url, driver)
            job_urls = job_urls_from_html(html)
            for job_url in job_urls:
                yield f'{INDEED_HOST}{job_url}'
    finally:
        driver.quit()


def retrieve_key_from_url(url):
    if 'jk=' in url:
        return url.split('jk=')[1].split('&')[0]
    elif 'fccid=' in url:
        return url.split('fccid=')[1].split('&')[0]
    else:
        print(f'failed url: {url}')
        return url


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def publish_job_url_to_kafka(url, kafka_producer):
    key = retrieve_key_from_url(url)
    kafka_producer.produce(topic=KAFKA_TOPIC, key=key, value=url, on_delivery=delivery_report)


def url_to_dict(url, ctx):
    return {'url': url}


def generate_kafka_producer():
    schema_str = """
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
    schema_registry_conf = {'url': SCHEMA_REGISTRY}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema_str, schema_registry_client, url_to_dict)

    producer_conf = {'bootstrap.servers': KAFKA_BROKER,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    return SerializingProducer(producer_conf)


def main():
    kafka_producer = generate_kafka_producer()
    for job_url in find_job_urls('data engineer', 'Utah'):
        publish_job_url_to_kafka(job_url, kafka_producer)


if __name__ == "__main__":
    main()
