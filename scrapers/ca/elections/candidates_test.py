import pandas as pd
from time import sleep
from bs4 import BeautifulSoup as soup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import numpy as np

pd.get_option("display.max_columns")
options = Options()
# options.headless = True
driver = webdriver.Chrome('web_drivers/chrome_win_92.0.4515.43/chromedriver.exe', options=options)
driver.switch_to.default_content()

driver.get('https://lop.parl.ca/sites/ParlInfo/default/en_CA/ElectionsRidings/Elections')
driver.maximize_window()
sleep(10)

expand_all = driver.find_element_by_css_selector('#gridContainer > div > div.dx-datagrid-header-panel > div > div > div.dx-toolbar-after > div:nth-child(2) > div > div > div')
expand_all.click()
sleep(15)

html = driver.page_source
html_soup = soup(html, 'html.parser')
html_data = html_soup.find_all('table', {'class': 'dx-datagrid-table dx-datagrid-table-fixed'})[1]
df = pd.read_html(str(html_data), index_col=[0, 1, 2, 3])[0]
print(df.columns)
df.columns = ['Province or Territory', 'Constituency', 'Candidate', 'Gender', 'Occupation', 'Political Affiliation', 'Result', 'Votes']
driver.close()
print(df)
df.to_csv('data.txt', sep='\t')