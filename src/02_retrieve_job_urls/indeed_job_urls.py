import os
import sys
sys.path.append('..')
import urllib.parse
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from src.utils import create_driver, html_from_url, generate_kafka_producer, publish_job_url_to_kafka

load_dotenv()

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

INDEED_HOST = 'https://www.indeed.com'
JOBS_PER_PAGE = 10


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


def main():
    kafka_producer = generate_kafka_producer(KAFKA_BROKER, SCHEMA_REGISTRY, KAFKA_SCHEMA_STR)
    for job_url in find_job_urls('data engineer', 'Utah'):
        publish_job_url_to_kafka(job_url, KAFKA_TOPIC, kafka_producer)


if __name__ == "__main__":
    main()
