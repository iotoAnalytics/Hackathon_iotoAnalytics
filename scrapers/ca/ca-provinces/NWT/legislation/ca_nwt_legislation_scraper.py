import sys
import os
from pathlib import Path

from requests.sessions import Session

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
from scraper_utils import CAProvinceTerrLegislationScraperUtils
from scraper_utils import PDF_Reader
import requests
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
from multiprocessing import Pool
import re

# These two will be updated by the program.
CURRENT_SESSION = 0
MAIN_PAGE_SOUP = None

PROV_TERR_ABBREVIATION = 'NT'
DATABASE_TABLE_NAME = 'ca_nt_legislation'
LEGISLATOR_TABLE_NAME = 'ca_nt_legislators'
BASE_URL = 'https://www.ntassembly.ca'
BILLS_URL = BASE_URL + '/documents-proceedings/bills'
THREADS_FOR_POOL = 12

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

scraper_utils = CAProvinceTerrLegislationScraperUtils(PROV_TERR_ABBREVIATION,
                                                      DATABASE_TABLE_NAME,
                                                      LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def program_driver():
    main = Main_Functions()
    bill_info_scraper = Bill_Main_Page_Scraper()
    
    print("Getting data from bill PDFs...")
    bill_pdf_links = bill_info_scraper.get_bill_pdf_links()

    bill_data = main.get_data_from_all_links(main.get_bill_data, bill_pdf_links)
    print(bill_data)

class Main_Functions:
    def set_main_page_soup_global(self, url):
        global MAIN_PAGE_SOUP
        page_html = self.__get_site_as_html(url)
        MAIN_PAGE_SOUP = soup(page_html, 'html.parser')

    def __get_site_as_html(self, url):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

    def get_data_from_all_links(self, function, links):
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = pool.map(func=function,
                            iterable=links)
        return data

    def get_bill_data(self, link):
        return Bill_PDF_Scraper(link).get_bill_data()

class Bill_Main_Page_Scraper:
    def __init__(self):
        self.page_soup = MAIN_PAGE_SOUP

    def get_bill_pdf_links(self):
        bills_and_legislation_table = self.__get_bills_and_legislation_table()
        all_a_links = bills_and_legislation_table.findAll('a')
        return [a['href'] for a in all_a_links]

    def __get_bills_and_legislation_table(self):
        main_container = self.page_soup.find('div', {'class' : 'view-id-documents_proceedings'})
        return main_container.find('tbody')    

    def set_current_session_global(self):
        global CURRENT_SESSION
        bills_container = self.__get_bills_and_legislation_table()
        CURRENT_SESSION = self.__extract_session_from_bill_container(bills_container)

    def __extract_session_from_bill_container(self, container):
        first_bill_info = container.find('td').text
        session_info = first_bill_info.split('-')[1]
        session_info = session_info.replace('(', '-').replace(')', '')
        return session_info.strip()

class Bill_PDF_Scraper:
    def __init__(self, link):
        self.row = scraper_utils.initialize_row()
        self.bill_url = link
        self.pdf_reader = PDF_Reader()
        self.__initialize_pdf_reader()
        self.__set_row_data()

    def get_bill_data(self):
        return self.row

    def __initialize_pdf_reader(self):
        pdf_response = requests.get(self.bill_url, headers=scraper_utils._request_headers, stream=True)
        scraper_utils.crawl_delay(crawl_delay)

        self.pdf_pages = self.pdf_reader.get_pdf_pages(pdf_response.content)
        self.pdf_reader.set_page_width_ratio(width_in_inch=8.5)
        self.pdf_reader.set_page_half(page_half_in_inch=4.32)
        self.pdf_reader.set_page_height_ratio(height_in_inch=11.0)
        self.pdf_reader.set_page_top_margin_in_inch(top_margin_in_inch=1.15)
        self.pdf_reader.set_left_column_end_and_right_column_start(column1_end=4.24, column2_start=4.60)

    def __set_row_data(self):
        self.row.session = CURRENT_SESSION
        self.row.source_url = self.bill_url
        self.row.chamber_origin = 'Legislative Assembly'
        self.row.bill_name = self.__get_bill_name()
        self.row.goverlytics_id = self.__get_goveryltics_id()
        self.row.bill_title = self.__get_bill_title()

    def __get_bill_name(self):
        return 'Bill' + self.__extract_bill_number_from_url()

    def __extract_bill_number_from_url(self):
        bill = re.findall(r'[A-Za-z]{4}_[0-9]{1,3}', self.bill_url)[0]
        return re.findall(r'[0-9]{1,3}', bill)[0].strip()

    def __get_goveryltics_id(self):
        session_split = CURRENT_SESSION.split('-')
        session = session_split[0] + '(' + session_split[1] + ')'
        return PROV_TERR_ABBREVIATION + '_' + session + '_' + self.row.bill_name
    
    def __get_bill_title(self):
        first_page = self.pdf_pages[0]
        first_page_text = self.__get_pdf_text(first_page)
        bill_title = self.__extract_bill_title(first_page_text)
        return bill_title.lower().capitalize()

    def __extract_bill_title(self, text):
        return_text = re.split(r'BILL [0-9]{1,3}', text)
        return_text = return_text[1].split('Summary')[0]
        return_text = return_text.split('DISPOSI')[0]
        return self.__clean_up_text(return_text)
            
    def __get_pdf_text(self, page):
        if self.pdf_reader.is_column(page):
            return self.pdf_reader.get_eng_half(page)
        else:
            return page.extract_text()

    def __clean_up_text(self, text):
        text = text.replace('\n', ' ')
        text = text.replace('  ', ' ')
        return text.strip()

Main_Functions().set_main_page_soup_global(BILLS_URL)
Bill_Main_Page_Scraper().set_current_session_global()

if __name__ == '__main__':
    program_driver()