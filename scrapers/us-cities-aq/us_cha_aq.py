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
p = Path(os.path.abspath(__file__)).parents[2]
sys.path.insert(0, str(p))

from scraper_utils import MunicipalUtils

scraper_utils = MunicipalUtils('us_cha_aq_meeting')
crawl_delay = scraper_utils.get_crawl_delay('https://charlotte.granicus.com/ViewPublisher.php?view_id=22')

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

driver.get('https://charlotte.granicus.com/ViewPublisher.php?view_id=22')

scraper_utils.crawl_delay(crawl_delay)

textp = []
dates = []
numh = []
key = []

search = driver.find_element(by=By.ID, value='gas_keywords')

search.clear()

search.send_keys('"air quality"')

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')

scraper_utils.crawl_delay(crawl_delay)

date = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td[headers^=Date] ')


i=0
for a in table:
    p = a.get_attribute('text').lower()
    if "quality" in p:
        textp.append(p)
        dates.append(((strip_tags(date[i].get_attribute('innerHTML')).replace(" ","")).replace("\n","")[:12]).replace("-",""))
        key.append("air quality")
    if not p == "agenda" and not p == "video" and not p == "open video only in windows media player":
        i+=1



#/////////

search = driver.find_element(by=By.ID, value='gas_keywords')

search.clear()

search.send_keys("air pollution")

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')

scraper_utils.crawl_delay(crawl_delay)

for a in table:
    p = a.get_attribute('text').lower()
    if "pollution" in p:
        textp.append(p)


#/////////

search = driver.find_element(by=By.ID, value='gas_keywords')

search.clear()

search.send_keys("carbon dioxide")

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')

scraper_utils.crawl_delay(crawl_delay)

date = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td[headers^=Date] ')

i=0
for a in table:
    p = a.get_attribute('text').lower()
    if "present" in p or "haven" in p:
        textp.append(p)
        dates.append(((strip_tags(date[i].get_attribute('innerHTML')).replace(" ","")).replace("\n","")[:12]).replace("-",""))
        key.append("carbon dioxide")
    if not p == "agenda" and not p == "video" and not p == "open video only in windows media player":
        i+=1

driver.quit()

for i in dates:
    numh.append('1')


zipped = list(zip(dates, numh, textp, key))
df = pd.DataFrame(zipped, columns=['meeting_date', 'num_matches', 'meeting_minutes','keyword'])
df_dict = df.to_dict('records')

#scraper_utils.write_cha_aq_meeting(df_dict)
path = pathlib.Path(__file__).parent.resolve() / ("Text//Charlotte.csv")
df.to_csv(path, header=None, index=None, sep=' ', mode='a')