from code import interact
import sys
import os
from pathlib import Path
import shutil

import io
import requests
from multiprocessing import Pool

from urllib.request import urlopen as uReq

from bs4 import BeautifulSoup as soup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import imaplib
import email
import base64
import time
import zipfile
import time
from io import StringIO
from html.parser import HTMLParser
import re
import pathlib

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[3]
sys.path.insert(0, str(p))

from scraper_utils import MunicipalUtils

scraper_utils = MunicipalUtils('us_sea_aq_meeting')
crawl_delay = scraper_utils.get_crawl_delay('http://clerk.seattle.gov/search/minutes/')

# load chrome driver info for selenium
# chrome driver comes from web_drivers folder
options = webdriver.ChromeOptions()

options.add_argument('--headless')
options.add_argument('--disable-extensions')
options.add_argument('--disable-gpu')
prefs = {'download.default_directory' : str(p)+'/scrapers/us/us-cities-aq'}
options.add_experimental_option('prefs',prefs)
driver = webdriver.Chrome(str(p)+'/web_drivers/chrome_mac_103.0.5060.134/chromedriver',options=options)        
print('driver found')

driver.get('http://clerk.seattle.gov/search/minutes/')

scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.XPATH, value='/html/body/div[2]/div/div[2]/div/div/div/div/div/div/div/main/div/form/div/div[3]/h4/a').click()
scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.ID, value='s6year1')
search.send_keys("2001")
search = driver.find_element(by=By.ID, value='s6year2')
search.send_keys("2022")
search.send_keys(Keys.ENTER)
scraper_utils.crawl_delay(crawl_delay)


links = []
dates = []
textp = []

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
for a in table:
    links.append(a.get_attribute('href'))
    dates.append(a.get_attribute('text'))
next = driver.find_element(by=By.XPATH,value='/html/body/div[2]/div/div[2]/div/div/p[2]/a[4]').click()

for i in range(1,6):
    table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
    for a in table:
        links.append(a.get_attribute('href'))
        dates.append(a.get_attribute('text'))
    next = driver.find_element(by=By.XPATH,value='/html/body/div[2]/div/div[2]/div/div/p[2]/a[5]').click()
    scraper_utils.crawl_delay(crawl_delay)

    

for link in links:
    driver.get(link)
    page = requests.get(driver.current_url)
    usoup = soup(page.content, 'lxml')
    text = usoup.find_all('p')
    textp.append(text)

driver.quit()

zipped = list(zip(dates,textp))
df = pd.DataFrame(zipped, columns=['meeting_date','meeting_minutes'])

path = pathlib.Path(__file__).parent.resolve() / ("Text//Seattle.csv")

df.to_csv(path, header=None, index=None, sep=' ', mode='a')