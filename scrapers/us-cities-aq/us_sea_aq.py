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
import imaplib
import email
import base64
import time
import zipfile
import time
import pathlib


# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[2]
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

search = driver.find_element(by=By.ID, value='s1')

search.send_keys('"air pollution"')

search.send_keys(Keys.ENTER)

links = []
dates = []
textp = []
numh = []
key = []

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
for a in table:
    links.append(a.get_attribute('href'))
    dates.append(a.get_attribute('text'))

for link in links:
    driver.get(link)
    page = requests.get(driver.current_url)
    usoup = soup(page.content, 'lxml')
    text = usoup.find_all('p')
    temp = []
    temp2 = ""
    cont = False
    num = 0
    for p in text:
        if "pollution" in p.get_text().lower() or cont == True:
            if ". " not in p.get_text():
                temp2 += p.get_text().replace("\r\n", " ")
                cont = True
            else:
                temp2 += p.get_text().replace("\r\n", " ")
                cont = False
                temp.append(temp2)
                temp2 = ""
                num += 1
    if not num == 0:
        textp.append(temp)
        numh.append(num)
        key.append("air pollution")

driver.get('http://clerk.seattle.gov/search/minutes/')

scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.ID, value='s1')

search.clear()

search.send_keys('"air quality"')

search.send_keys(Keys.ENTER)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
for a in table:
    links.append(a.get_attribute('href'))
    dates.append(a.get_attribute('text'))

for link in links:
    driver.get(link)
    page = requests.get(driver.current_url)
    usoup = soup(page.content, 'lxml')
    text = usoup.find_all('p')
    temp = []
    temp2 = ""
    cont = False
    num = 0
    for p in text:
        if "air quality" in p.get_text().lower() or cont == True:
            if ". " not in p.get_text():
                temp2 += p.get_text().replace("\r\n", " ")
                cont = True
            else:
                temp2 += p.get_text().replace("\r\n", " ")
                cont = False
                temp.append(temp2)
                temp2 = ""
                num += 1
    if not num == 0:
        textp.append(temp)
        numh.append(num)
        key.append("air quality")

driver.get('http://clerk.seattle.gov/search/minutes/')

scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.ID, value='s1')

search.clear()

search.send_keys('"greenhouse gas"')

search.send_keys(Keys.ENTER)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
for a in table:
    links.append(a.get_attribute('href'))
    dates.append(a.get_attribute('text'))

for link in links:
    driver.get(link)
    page = requests.get(driver.current_url)
    usoup = soup(page.content, 'lxml')
    text = usoup.find_all('p')
    temp = []
    temp2 = ""
    cont = False
    num = 0
    for p in text:
        if "greenhouse gas" in p.get_text().lower() or cont == True:
            if ". " not in p.get_text():
                temp2 += p.get_text().replace("\r\n", " ")
                cont = True
            else:
                temp2 += p.get_text().replace("\r\n", " ")
                cont = False
                temp.append(temp2)
                temp2 = ""
                num += 1
    if not num == 0:
        textp.append(temp)
        numh.append(num)
        key.append("greenhouse gas")

driver.get('http://clerk.seattle.gov/search/minutes/')

scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.ID, value='s1')

search.clear()

search.send_keys('"clean air"')

search.send_keys(Keys.ENTER)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
for a in table:
    links.append(a.get_attribute('href'))
    dates.append(a.get_attribute('text'))

for link in links:
    driver.get(link)
    page = requests.get(driver.current_url)
    usoup = soup(page.content, 'lxml')
    text = usoup.find_all('p')
    temp = []
    temp2 = ""
    cont = False
    num = 0
    for p in text:
        if "clean air" in p.get_text().lower() or cont == True:
            if ". " not in p.get_text():
                temp2 += p.get_text().replace("\r\n", " ")
                cont = True
            else:
                temp2 += p.get_text().replace("\r\n", " ")
                cont = False
                temp.append(temp2)
                temp2 = ""
                num += 1
    if not num == 0:
        textp.append(temp)
        numh.append(num)
        key.append("clean air")

driver.get('http://clerk.seattle.gov/search/minutes/')

scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.ID, value='s1')

search.clear()

search.send_keys('"carbon dioxide"')

search.send_keys(Keys.ENTER)

table = driver.find_elements(by=By.CSS_SELECTOR,value='table tr td a')
for a in table:
    links.append(a.get_attribute('href'))
    dates.append(a.get_attribute('text'))

for link in links:
    driver.get(link)
    page = requests.get(driver.current_url)
    usoup = soup(page.content, 'lxml')
    text = usoup.find_all('p')
    temp = []
    temp2 = ""
    cont = False
    num = 0
    for p in text:
        if "carbon dioxide" in p.get_text().lower() or cont == True:
            if ". " not in p.get_text():
                temp2 += p.get_text().replace("\r\n", " ")
                cont = True
            else:
                temp2 += p.get_text().replace("\r\n", " ")
                cont = False
                temp.append(temp2)
                temp2 = ""
                num += 1
    if not num == 0:
        textp.append(temp)
        numh.append(num)
        key.append("carbon dioxide")

driver.quit()

""" nums = []
num = 1
for i in dates:
    nums.append(num)
    num += 1 """

zipped = list(zip(dates, numh, textp, key))
df = pd.DataFrame(zipped, columns=['meeting_date', 'num_matches', 'meeting_minutes', 'keyword'])
df_dict = df.to_dict('records')

#scraper_utils.write_sea_aq_meeting(df_dict)
path = pathlib.Path(__file__).parent.resolve() / ("Text//Seattle.csv")
df.to_csv(path, header=None, index=None, sep=' ', mode='a')