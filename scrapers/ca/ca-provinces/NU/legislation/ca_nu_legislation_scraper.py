import enum
import sys
import os
from pathlib import Path

import pdfplumber

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
from scraper_utils import CAProvinceTerrLegislationScraperUtils, PDF_Table_Reader
from scraper_utils import PDF_Reader
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
from multiprocessing import Pool
import re
import datetime

PROV_TERR_ABBREVIATION = 'NU'
DATABASE_TABLE_NAME = 'ca_nu_legislation'
LEGISLATOR_TABLE_NAME = 'ca_nu_legislators'
BASE_URL = 'https://www.assembly.nu.ca'
BILLS_URL = BASE_URL + '/bills-and-legislation'
TABLED_DOCUMENT_URL = BASE_URL + '/tabled-documents'
THREADS_FOR_POOL = 12

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

scraper_utils = CAProvinceTerrLegislationScraperUtils(PROV_TERR_ABBREVIATION,
                                                      DATABASE_TABLE_NAME,
                                                      LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def program_driver():
    main_functions = MainFunctions()
    
    print("Getting data from bill PDFs...")
    main_page_soup = main_functions.get_page_as_soup(BILLS_URL)
    bill_table_rows = main_functions.get_bill_rows(main_page_soup)

    bill_data = main_functions.get_data_from_all_links(main_functions.get_bill_data, bill_table_rows)
    # print(bill_data)
    # print('Writing data to database...')
    # scraper_utils.write_data(bill_data)
    # print("Complete")

class PreProgramFunctions:
    def set_current_session(self):
        global CURRENT_SESSION

        tabled_document_soup = MainFunctions().get_page_as_soup(TABLED_DOCUMENT_URL)
        CURRENT_SESSION = self.__get_current_session(tabled_document_soup)

    def __get_current_session(self, soup):
        assembly_session_select_box = soup.find('select')
        all_options = assembly_session_select_box.findAll('option')[::-1]
        index_of_current_session = 0
        current_session = self.__extract_session(all_options[index_of_current_session].text)
        current_assembly = self.__get_current_assembly(all_options)
        return f'{current_assembly}-{current_session}'

    def __extract_session(self, text):
        session = re.findall(r'\d', text)[0]
        return session

    def __get_current_assembly(self, options):
        for option in options:
            if 'Assembly' in option.text:
                return self.__extract_assembly(option.text)
    
    def __extract_assembly(self, text):
        index_of_assembly_number = 0
        return text[index_of_assembly_number]
        

class MainFunctions:
    def get_page_as_soup(self, url):
        page_html = self.__get_site_as_html(url)
        return soup(page_html, 'html.parser')

    def __get_site_as_html(self, url):
        uClient = urlopen(url)
        page_html = uClient.read()
        uClient.close()
        scraper_utils.crawl_delay(crawl_delay)
        return page_html

    def get_bill_rows(self, soup):
        main_content = soup.find('div', {'class' : 'view-content'})
        bills_table = main_content.find('tbody')
        return bills_table.findAll('tr')

    def get_data_from_all_links(self, function, rows):
        row_texts = [str(row) for row in rows]
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = pool.map(func=function,
                            iterable=row_texts)
        return data

    def get_bill_data(self, table_row_soup):
        row = soup(table_row_soup, 'html.parser')
        return BillScraper(row).get_bill_data()

class BillScraper:
    column_description_to_index = {
        'Bill Number/Title' : 0,
        'Date of Notice' : 1,
        'First Reading' : 2,
        'Second Reading' : 3,
        'Reported from Standing Committee' : 4,
        'Reported from Committee of the Whole' : 5,
        'Third Reading' : 6,
        'Assent' : 7,
    }

    def __init__(self, bill_row_soup):
        self.row = scraper_utils.initialize_row()
        self.bill_columns_from_site = self.__get_columns(bill_row_soup)
        self.row.source_url = self.__get_source_url()
        self.pdf_reader = PDF_Reader()
        self.summary_page_index = 0 #this value will get updated
        self.__initialize_pdf_reader()
        self.__set_row_data()

    def __get_columns(self, bill_row_soup):
        return bill_row_soup.findAll('td')

    def get_bill_data(self):
        return self.row

    def __initialize_pdf_reader(self):
        self.pdf_pages = self.pdf_reader.get_pdf_pages(self.row.source_url, scraper_utils._request_headers)
        self.pdf_reader.set_page_width_ratio(width_in_inch=8.5)
        self.pdf_reader.set_page_half(page_half_in_inch=4.25)
        self.pdf_reader.set_page_height_ratio(height_in_inch=11.0)
        self.pdf_reader.set_page_top_spacing_in_inch(top_spacing_in_inch=1.19)
        self.pdf_reader.set_left_column_end_and_right_column_start(column1_end=4.25, column2_start=4.30)
        self.pdf_reader.set_page_bottom_spacing_in_inch(bottom_spacing_in_inch=0.90)
        scraper_utils.crawl_delay(crawl_delay)

    def __set_row_data(self):
        self.row.bill_name = self.__get_bill_name()
        self.row.bill_title = self.__get_bill_title()
        self.row.session = CURRENT_SESSION
        self.row.goverlytics_id = self.__get_goverlytics_id()
        self.row.chamber_origin = 'Legislative Assembly'
        self.row.actions = self.__get_actions()
        self.row.date_introduced = self.__get_date_introduced()
        self.row.current_status = self.__get_current_status()
        self.row.bill_summary = self.__get_bill_summary()
        print(f'{self.row.bill_name}: {self.row.bill_summary}')
        # TODO
        # self.row.bill_type = self.__get_bill_type() 
    
    def __get_bill_name(self):
        first_column = self.bill_columns_from_site[self.column_description_to_index.get('Bill Number/Title')]
        bill_name = first_column.find('h2').text
        return self.__format_bill_name(bill_name)

    def __format_bill_name(self, text):
        text = text.title()
        text = text.replace(' ', '')
        return text

    def __get_bill_title(self):
        first_column = self.bill_columns_from_site[self.column_description_to_index.get('Bill Number/Title')]
        title = first_column.find('a').text
        return title.title()

    def __get_source_url(self):
        first_column = self.bill_columns_from_site[self.column_description_to_index.get('Bill Number/Title')]
        return first_column.find('a')['href']

    def __get_goverlytics_id(self):
        return f'{PROV_TERR_ABBREVIATION}_{CURRENT_SESSION}_{self.row.bill_name}'

    def __get_actions(self):
        actions = []
        for index, cell in enumerate(self.column_description_to_index):
            action = self.__add_action_attribute(index, cell)
            if action:
                actions.append(action)
        return actions

    def __add_action_attribute(self, index, cell):
        if index == 0:
            return
            
        text = self.bill_columns_from_site[index].text
        text = self.__clean_up_text(text)
        if len(text) == 0:
            return

        date = datetime.datetime.strptime(text, '%B %d, %Y')
        date = date.strftime('%Y-%m-%d')

        description = cell
        action_by = self.__get_action_by(description)
        return {'date' : date, 'action_by' : action_by, 'description' : description}

    def __clean_up_text(self, text):
        text = text.replace('\n', '')
        return text.strip()

    def __get_action_by(self, description):
        if 'Committee' in description:
            return 'Standing Committee'
        if 'Assent' in description:
            return 'Commissioner'
        return 'Legislative Assembly'

    def __get_date_introduced(self):
        index_of_first_reading_in_actions = 1
        first_reading = self.row.actions[index_of_first_reading_in_actions]
        return first_reading['date']

    def __get_current_status(self):
        last_action = self.row.actions[-1]
        return last_action['description']

    def __get_bill_summary(self):
        pages_to_search_for = self.pdf_pages[0:2]
        if self.row.bill_name == 'Bill75':
            return self.__extract_bill_summary(pages_to_search_for)
        return self.__extract_bill_summary(pages_to_search_for[::-1])

## TODO
## FIX THIS UP

    def __extract_bill_summary(self, pages):
        for page in pages:
            text = self.__get_pdf_text(page)
            if re.search('Summary', text):
                return self.__get_summary_text(text)

            # page_entire_text = page.extract_text()
            # page_lines = page_entire_text.split('\n')
            # return_string = ''
            # for text_block in page_lines:
            #     text_block = text_block.split('  ')[0]
            #     return_string = return_string + ' ' + text_block
            #     return_string = self.__clean_up_text(return_string)
            # print(return_string)
            # page_text = page.extract_text()

    def __get_pdf_text(self, page):
        return self.pdf_reader.get_text(page, True)

    def __get_summary_text(self, text):
        if self.row.bill_name == 'Bill75':
            text = text.split('Summary')[1:]
            text = ''.join(text)
        else:
            text = text.split('Summary')[1]
        text = text.split('Date')[0]
        return text

    def __get_bill_type(self):
        if 'amend' in self.row.bill_title.lower():
            return 'Ammendment'
        return 'Bill'

PreProgramFunctions().set_current_session()

if __name__ == '__main__':
    program_driver()