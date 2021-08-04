'''
Before beginning, be sure to update values in the config file.

This template is meant to serve as a general outline, and will not necessarily work for
all pages. Feel free to modify the scripts as necessary.

Note that the functions in the scraper_utils.py and database_tables.py file should not
have to change. Please extend the classes in these files if you need to modify them.
'''
import sys
import os
from pathlib import Path

import io
from scraper_utils import CAProvinceTerrLegislationScraperUtils
import requests
from multiprocessing import Pool

from urllib.request import urlopen as uReq

from bs4 import BeautifulSoup as soup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]
sys.path.insert(0, str(p))

prov_terr_abbreviation = 'BC'

database_table_name = 'ca_bc_legislation'
legislator_table_name = 'ca_bc_legislators'

scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)
crawl_delay = scraper_utils.get_crawl_delay('https://www.leg.bc.ca')

# load chrome driver info for selenium
# chrome driver comes from web_drivers folder
chrome_options = webdriver.ChromeOptions()

chrome_options.add_argument('--headless')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('--disable-gpu')

driver = webdriver.Chrome('../../../../../web_drivers/chrome_win_89.0.4389.23/chromedriver.exe',
                          chrome_options=chrome_options)
print("driver found")


def get_urls(myurl):
    driver.get(myurl)
    timeout = 5

    try:
        element_present = EC.presence_of_element_located(
            (By.CLASS_NAME, 'BCLASS-Hansard-List'))
        WebDriverWait(driver, timeout).until(element_present)

    except:
        print("timeout")
        pass

    html = driver.page_source
    page_soup = soup(html, 'html.parser')
    url_infos = []
    hansardlist = page_soup.find("ul", {"class": "BCLASS-Hansard-List"})
    try:
        listings = hansardlist.findAll("li")
        for li in listings:
            url = (li.div.div.div.div.a["href"])

            if "/gov" in url:
                bill_num = "gov" + url.split("/gov")[1]
            else:
                bill_num = "m" + url.split("/m")[1]
            bill_num = bill_num.split("-")[0]
            info = {'source_url': url, 'bill_name': bill_num}
            # print(info)
            url_infos.append(info)
    except:
        print(myurl)
    scraper_utils.crawl_delay(crawl_delay)
    return url_infos


def scrape(url):

    driver.get(url)
    timeout = 5

    try:
        element_present = EC.presence_of_element_located(
            (By.CLASS_NAME, 'explannote'))
        WebDriverWait(driver, timeout).until(element_present)

    except:
        pass
        # print(timeout)
    html = driver.page_source

    page_soup = soup(html, 'html.parser')

    '''
    Insert logic here to scrape all URLs acquired in the get_urls() function.

    Do not worry about collecting the date_collected, state, and state_id values,
    as these have already been inserted by the initialize_row()
    function, or will be inserted when placed in the database.

    Do not worry about trying to insert missing fields as the initialize_row function will
    insert empty values for us.

    Be sure to insert the correct data type into each row. Otherwise, you will get an error
    when inserting data into database. Refer to the data dictionary to see data types for
    each column.
    '''

    row = scraper_utils.initialize_row()

    if "/gov" in url:
        bill_num = "gov" + url.split("/gov")[1]
    else:
        bill_num = "m" + url.split("/m")[1]
    bill_num = bill_num.split("-")[0]

    row.bill_name = bill_num
    row.source_url = url

    # Now you can begin collecting data and fill in the row. The row is a dictionary where the
    # keys are the columns in the data dictionary. For instance, we can insert the state_url,
    # like so:
    session = url.split("-parliament")[0]
    session = session.split("proceedings/")[1]
    row.session = session

    page_title = page_soup.find("h1", {"class": "BCLASS-PageTitleBody"})

    row.bill_title = page_title.span.text.strip()
    row.chamber_origin = 'Legislative Assembly'
    row.bill_type = 'Bill'
    row.source_id = bill_num
    row.goverlytics_id = "BC_" + session + "_" + bill_num
    if 'third-reading' in url:
        row.current_status = 'third reading'
        # transformed_url = 'www.leg.bc.ca/content/data-ldp/pages/42nd1st/3rd_read/' + bill_num + '-3.htm'
    elif 'first-reading' in url:
        row.current_status = 'first reading'
        # transformed_url = 'www.leg.bc.ca/content/data-ldp/pages/42nd1st/1st_read/' + bill_num + '-1.htm'

    elif 'amended' in url:
        row.current_status = 'amended'
    #     transformed_url = 'www.leg.bc.ca/content/data-ldp/pages/42nd1st/amended/' + bill_num + '-2.htm'
    # transformed_url = urllib.parse.quote(transformed_url)
    # transformed_url = 'https://' + transformed_url
    iframe = page_soup.find('iframe', {'id': 'BCLASS-Legacy-Frame'})
    url = iframe["src"]

    transformed_url = url.replace(' ', '%20')
    # print(transformed_url)
    try:
        uClient = uReq(transformed_url)
        page_html = uClient.read()
        uClient.close()
        # # # html parsing
        page_soup = soup(page_html, "html.parser")
        # print(page_soup)
        row.bill_text = " ".join(page_soup.text.split())

        center = page_soup.find("p", {"align": "center"})
        date = center.text.split("on the")[1]
        date = date.replace("day of", "").strip()
        date = date.split(" ")


    except:
        pass
        # print(transformed_url)
    # driver.get(transformed_url)
    # timeout = 5
    #
    # try:
    #     element_present = EC.presence_of_element_located((By.CLASS_NAME, 'explannote'))
    #     WebDriverWait(driver, timeout).until(element_present)
    #
    #
    # except:
    #     pass
    # print(timeout)
    # html = driver.page_source
    #
    # page_soup = soup(html, 'html.parser')
    # print(page_soup)

    #
    # try:
    #     header = page_soup.find('div', {'id': 'BCLASS-Legacy-Wrapper'})
    #     print(header)
    #     # center = header.find('p', {'align': 'center'})
    #     # print(center.text)
    #
    # except:
    #     print(url)
    # print(page_soup)
    # try:
    #     pdf_id = page_soup.find("div", {"id": "PrintPDF"})
    #
    #     pdf_link = pdf_id.a["href"]
    #     # print(pdf_link)
    #     r = scraper_utils.request(pdf_link)
    #     f = io.BytesIO(r.content)
    #     reader = PyPDF2.PdfFileReader(f, strict=False)
    #     if reader.isEncrypted:
    #         reader.decrypt('')
    #
    #     contents = reader.getPage(0).extractText()
    #     # print(contents)
    # except Exception as ex:
    #
    #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    #     message = template.format(type(ex).__name__, ex.args)
    #
    # print(message)
    # print(url)
    # titles pa= page_soup.findAll('p')
    # print(titles)
    # for title in titles:
    #     print(title.text)
    # if "Explanatory Note" in title.text:
    #     print(title.text)
    scraper_utils.crawl_delay(crawl_delay)
    return row


if __name__ == '__main__':
    # bc bills are currently under 3 categories:

    # having completed first reading on this page
    first_reading_url = 'https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings/42nd-parliament' \
                        '/1st-session/bills/first-reading#Default={%22o%22:[{%22d%22:0,' \
                        '%22p%22:%22BillTypeOWSCHCS%22},{%22d%22:0,%22p%22:%22BillNumber%22}]} '
    # ammendments on this page
    ammendement_url = 'https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings/42nd-parliament' \
                      '/1st-session/bills/amended '
    # having completed third reading on this page
    third_reading_url = 'https://www.leg.bc.ca/parliamentary-business/legislation-debates-proceedings/42nd-parliament/1st-session/bills/third-reading#Default={%22o%22:[{%22d%22:0,%22p%22:%22BillTypeOWSCHCS%22},{%22d%22:0,%22p%22:%22BillNumber%22}]} '

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.options.display.max_colwidth = 200
    # First we'll get the URLs we wish to scrape:
    first_urls = pd.DataFrame(get_urls(first_reading_url))

    ammendment_urls = pd.DataFrame(get_urls(ammendement_url))

    third_urls = pd.DataFrame(get_urls(third_reading_url))

    # put all the bill infos into one dataframe
    url_df = pd.concat((ammendment_urls, first_urls)).sort_index().drop_duplicates(
        'bill_name')  # .reset_index(drop=True)
    url_df = pd.concat((third_urls, url_df)).sort_index(
    ).drop_duplicates('bill_name')  # .reset_index(drop=True)

    less_urls = url_df['source_url'][:11]
    urls = url_df['source_url']
    print(len(urls))

    with Pool() as pool:
        data = pool.map(scrape, urls)
    print(*data, sep='\n')

    # Once we collect the data, we'll  write it to the database.
    scraper_utils.write_data(data)

    print('Complete!')