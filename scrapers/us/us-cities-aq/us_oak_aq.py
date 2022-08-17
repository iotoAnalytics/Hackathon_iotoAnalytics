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

scraper_utils = MunicipalUtils('us_oak_aq_meeting')
crawl_delay = scraper_utils.get_crawl_delay('https://oakland.legistar.com/Legislation.aspx')

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

driver.get('https://oakland.legistar.com/Legislation.aspx')

dates = []
textp = []
numh = []
key = []

scraper_utils.crawl_delay(crawl_delay)

select = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_tdYears').click()

scraper_utils.crawl_delay(crawl_delay)

select = driver.find_element(by=By.XPATH, value='/html/body/form/div[1]/div/div/ul/li[1]').click()

scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_txtSearch')

search.send_keys('"air pollution"')

search.send_keys(Keys.ENTER)

text = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(6)")
datet = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(4)")

i = 0
for a in text:
    t = strip_tags(a.get_attribute('innerHTML'))
    if re.search(r'\b' + 'air' + r'\b', t.lower()):
        textp.append(t)
        if strip_tags(datet[i].get_attribute('innerHTML')) == '\xa0':
            dates.append("Not Available")
        else:
            dates.append(strip_tags(datet[i].get_attribute('innerHTML')))
    i += 1

for k in range(i):
    numh.append(1)
    key.append("air pollution")

#//////////

search = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_txtSearch')

search.clear()

search.send_keys('"air quality"')

search.send_keys(Keys.ENTER)

text = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(6)")
datet = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(4)")

i = 0
for a in text:
    t = strip_tags(a.get_attribute('innerHTML'))
    if re.search(r'\b' + 'air' + r'\b', t.lower()):
        textp.append(t)
        if strip_tags(datet[i].get_attribute('innerHTML')) == '\xa0':
            dates.append("Not Available")
        else:
            dates.append(strip_tags(datet[i].get_attribute('innerHTML')))
    i += 1

for k in range(i):
    numh.append(1)
    key.append("air quality")

#//////////

search = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_txtSearch')

search.clear()

search.send_keys('"air pollutants"')

search.send_keys(Keys.ENTER)

text = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(6)")
datet = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(4)")

i = 0
for a in text:
    t = strip_tags(a.get_attribute('innerHTML'))
    if re.search(r'\b' + 'air' + r'\b', t.lower()):
        textp.append(t)
        if strip_tags(datet[i].get_attribute('innerHTML')) == '\xa0':
            dates.append("Not Available")
        else:
            dates.append(strip_tags(datet[i].get_attribute('innerHTML')))
    i += 1

for k in range(i):
    numh.append(1)
    key.append("air pollutants")

#//////////

search = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_txtSearch')

search.clear()

search.send_keys('"greenhouse"')

search.send_keys(Keys.ENTER)

text = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(6)")
datet = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(4)")

i = 0
for a in text:
    t = strip_tags(a.get_attribute('innerHTML'))
    if re.search(r'\b' + 'greenhouse' + r'\b', t.lower()):
        textp.append(t)
        if strip_tags(datet[i].get_attribute('innerHTML')) == '\xa0':
            dates.append("Not Available")
        else:
            dates.append(strip_tags(datet[i].get_attribute('innerHTML')))
    i += 1

for k in range(i):
    numh.append(1)
    key.append("greenhouse")

driver.quit()

zipped = list(zip(dates, numh, textp, key))
df = pd.DataFrame(zipped, columns=['meeting_date', 'num_matches', 'meeting_minutes', 'keyword']).groupby(['meeting_date']).agg(tuple).applymap(list).reset_index()
for index, row in df.iterrows():
    row['num_matches'] = len(row['num_matches'])

df_dict = df.to_dict('records')
scraper_utils.write_oak_aq_meeting(df_dict)