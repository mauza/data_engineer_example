from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import urllib.parse

INDEED_HOST = 'https://www.indeed.com'
JOBS_PER_PAGE = 10


def create_driver():
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)
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


def main():
    jobs = list(find_job_urls('data engineer', 'Utah'))
    print(jobs)
    print(len(jobs))

if __name__ == "__main__":
    main()