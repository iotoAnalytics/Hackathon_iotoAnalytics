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

scraper_utils = MunicipalUtils('us_la_aq_meeting')
crawl_delay = scraper_utils.get_crawl_delay('https://lacity.granicus.com/ViewPublisher.php?view_id=129')

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

driver.get('https://lacity.granicus.com/ViewPublisher.php?view_id=129')

scraper_utils.crawl_delay(crawl_delay)

textp = []
dates = []
numh = []
key = []

view = driver.find_element(by=By.LINK_TEXT, value='Advanced Search').click()

search = driver.find_element(by=By.ID, value='phrase')

search.send_keys("air pollution")

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
date = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td[headers^=Date] ')

scraper_utils.crawl_delay(crawl_delay)

previous = ''
i = 0
for a in table:
    t = strip_tags(a.get_attribute('text'))
    if re.search(r'\b' + 'air pollution' + r'\b', t.lower()):
        if not t == previous:
            textp.append(t.rstrip())
            dates.append(strip_tags((date[i].get_attribute('innerHTML'))[-8:]))
            key.append("air pollution")
    if not t == 'Agenda' and not t == 'Journal' and not t == 'Video' and not t == 'Open Audio Only in Windows Media Player' and not t == 'MP3' and not t == 'MP4':
        i += 1
    previous = t

####/////

search = driver.find_element(by=By.ID, value='phrase')

search.clear()

search.send_keys("air quality")

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
date = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td[headers^=Date] ')

scraper_utils.crawl_delay(crawl_delay)

previous = ''
i = 0
for a in table:
    t = strip_tags(a.get_attribute('text'))
    if re.search(r'\b' + 'air quality' + r'\b', t.lower()):
        if not t == previous:
            textp.append(t.rstrip())
            dates.append(strip_tags((date[i].get_attribute('innerHTML'))[-8:]))
            key.append("air quality")
    if not t == 'Agenda' and not t == 'Journal' and not t == 'Video' and not t == 'Open Audio Only in Windows Media Player' and not t == 'MP3' and not t == 'MP4' and not t[:10] == previous[:10]:
        i += 1
    previous = t
####/////

search = driver.find_element(by=By.ID, value='phrase')

search.clear()

search.send_keys("greenhouse gas")

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
date = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td[headers^=Date] ')

scraper_utils.crawl_delay(crawl_delay)

previous = ''
i = 0
for a in table:
    t = strip_tags(a.get_attribute('text'))
    if re.search(r'\b' + 'greenhouse gas' + r'\b', t.lower()):
        if not t == previous:
            textp.append(t.rstrip())
            dates.append(strip_tags((date[i].get_attribute('innerHTML'))[-8:]))
            key.append("greenhouse gas")
    if not t == 'Agenda' and not t == 'Journal' and not t == 'Video' and not t == 'Open Audio Only in Windows Media Player' and not t == 'MP3' and not t == 'MP4':
        i += 1
    previous = t

####/////

search = driver.find_element(by=By.ID, value='phrase')

search.clear()

search.send_keys("clean air")

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
date = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td[headers^=Date] ')

scraper_utils.crawl_delay(crawl_delay)

previous = ''
i = 0
for a in table:
    t = strip_tags(a.get_attribute('text'))
    if re.search(r'\b' + 'clean air' + r'\b', t.lower()):
        if not t == previous:
            textp.append(t.rstrip())
            dates.append(strip_tags((date[i].get_attribute('innerHTML'))[-8:]))
            key.append("clean air")
    if not t == 'Agenda' and not t == 'Journal' and not t == 'Video' and not t == 'Open Audio Only in Windows Media Player' and not t == 'MP3' and not t == 'MP4' and not t[:10] == previous[:10]:
        i += 1
    previous = t

driver.quit()

for i in textp:
    numh.append(1)

zipped = list(zip(dates, numh, textp, key))
df = pd.DataFrame(zipped, columns=['meeting_date', 'num_matches', 'meeting_minutes', 'keyword'])
df_dict = df.to_dict('records')

scraper_utils.write_la_aq_meeting(df_dict)