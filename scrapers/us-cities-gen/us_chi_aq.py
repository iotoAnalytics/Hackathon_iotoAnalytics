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

scraper_utils = MunicipalUtils('us_chi_aq_meeting')
crawl_delay = scraper_utils.get_crawl_delay('https://chicago.legistar.com/Legislation.aspx')

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

driver.get('https://chicago.legistar.com/Legislation.aspx')

scraper_utils.crawl_delay(crawl_delay)

select = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_btnSwitch').click()

scraper_utils.crawl_delay(crawl_delay)

select = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_tdYears').click()

scraper_utils.crawl_delay(crawl_delay)

select = driver.find_element(by=By.XPATH, value='/html/body/form/div[1]/div/div/ul/li[1]').click()

scraper_utils.crawl_delay(crawl_delay)

search = driver.find_element(by=By.ID, value='ctl00_ContentPlaceHolder1_txtSearch')

search.send_keys(Keys.ENTER)

scraper_utils.crawl_delay(crawl_delay)

show = driver.find_element(by=By.XPATH, value='/html/body/form/div[3]/div[6]/div/div/div[5]/table[3]/tbody/tr/td/div/ul/li[2]/a').click()

scraper_utils.crawl_delay(crawl_delay)

show = driver.find_element(by=By.XPATH, value='/html/body/form/div[3]/div[6]/div/div/div[5]/table[3]/tbody/tr/td/div/ul/li[2]/div/ul/li[6]/a/span').click()

dates = []
textp = []
done = False

def search():

    text = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(7)")
    datet = driver.find_elements(by=By.CSS_SELECTOR,value="[id^='ctl00_ContentPlaceHolder1_gridMain_ctl00__'] td:nth-child(4)")

    i = 0
    for a in text:
        t = strip_tags(a.get_attribute('innerHTML'))
        if not t.startswith("Damage to vehicle") and not t.startswith("Handicapped") and not t.startswith("Congratulations") and not t.startswith("Tribute") and not t.startswith("Appointment") and not t.startswith("Reappointment") and not t.startswith("Issuance of permits") and not t.startswith("Recognition") and not t.startswith("Grant(s)"):
            textp.append(t.rstrip())
            if strip_tags(datet[i].get_attribute('innerHTML')) == '\xa0':
                dates.append("Not Available")
            else:
                dates.append(strip_tags(datet[i].get_attribute('innerHTML')))
            i += 1

def search_rest(start,next):
    for i in range(start,next):
        try:
            select = driver.find_element(by=By.XPATH, value='/html/body/form/div[3]/div[6]/div/div/div[5]/table[4]/tbody/tr/td/div/table/thead/tr[1]/td/table/tbody/tr/td/div[1]/a[' + str(i) + ']').click()
        except:
            done = True
            return
        scraper_utils.crawl_delay(crawl_delay)
        search()
    
    try:
        select = driver.find_element(by=By.XPATH, value='/html/body/form/div[3]/div[6]/div/div/div[5]/table[4]/tbody/tr/td/div/table/thead/tr[1]/td/table/tbody/tr/td/div[1]/a[' + str(next) + ']')
    except:
        done = True
    

search_rest(1,11)
while len(textp) <5000:
    search_rest(2,12)

driver.quit()

search_rest(1,11)
while len(textp) <5000 and not done == True:
    search_rest(2,12)

driver.quit()

zipped = list(zip(dates, textp))
df = pd.DataFrame(zipped, columns=['meeting_date', 'meeting_minutes'])

path = pathlib.Path(__file__).parent.resolve() / ("Text//Chicago.csv")

df.to_csv(path, header=None, index=None, sep=' ', mode='a')