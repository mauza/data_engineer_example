from selenium import webdriver
from selenium.webdriver.firefox.options import Options

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
