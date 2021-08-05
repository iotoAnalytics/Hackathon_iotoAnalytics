import pandas as pd
from time import sleep
from bs4 import BeautifulSoup as soup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import numpy as np

pd.get_option("display.max_columns")
options = Options()
options.headless = True
driver = webdriver.Chrome('web_drivers/chrome_win_92.0.4515.43/chromedriver.exe', options=options)
driver.switch_to.default_content()

driver.get('https://lop.parl.ca/sites/ParlInfo/default/en_CA/ElectionsRidings/Elections')
driver.maximize_window()
sleep(10)

def get_img_url_from_candidate_profile(url):
    driver.execute_script(f'''window.open("{url}", "_blank");''')
    sleep(4)
    tabs = 2
    driver.switch_to_window(driver.window_handles[tabs - 1])
    image_div = driver.find_element_by_id('PersonPic')
    img_url = image_div.find_element_by_tag_name('img').get_attribute('src')
    driver.close()
    sleep(1)
    tabs -= 1
    driver.switch_to_window(driver.window_handles[tabs - 1])
    sleep(2)
    return img_url

expand_all = driver.find_element_by_css_selector('#gridContainer > div > div.dx-datagrid-header-panel > div > div > div.dx-toolbar-after > div:nth-child(2) > div > div > div')
expand_all.click()
sleep(15)

html = driver.page_source
html_soup = soup(html, 'html.parser')
html_data = html_soup.find_all('table', {'class': 'dx-datagrid-table dx-datagrid-table-fixed'})[1]
df = pd.read_html(str(html_data), index_col=[0, 1, 2, 3])[0]
print(df.columns)
df.columns = ['Province or Territory', 'Constituency', 'Candidate', 'Gender', 'Occupation', 'Political Affiliation', 'Result', 'Votes']
df['Image URL'] = None

trs_with_images = html_soup.find_all('tr', {'class':'dx-row dx-data-row dx-column-lines'})

for tr in trs_with_images:
    name = tr.find_all('td')[6].text

    img_url = tr.find('img')
    if img_url is not None:
        img_url = img_url.get('src')
        img_url = 'https://lop.parl.ca' + img_url

    candidate_url = tr.find_all('td')[6].a
    if candidate_url is not None:
        candidate_url = candidate_url['href']
        candidate_url = 'https://lop.parl.ca' + candidate_url
        img_url = get_img_url_from_candidate_profile(candidate_url)
    df.loc[df["Candidate"] == name, ['Image URL']] = img_url


driver.close()
df.to_csv('data.txt', sep='\t')