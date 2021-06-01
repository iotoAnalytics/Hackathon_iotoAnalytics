import sys
import os
from pathlib import Path

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
    print('Writing data to database...')
    scraper_utils.write_data(bill_data)
    print("Complete")

class Status_Of_Bills:
    def __init__(self):
        self.status_of_bills_link = self.__get_status_of_bills_link()
        self.columns = ['bill_number', 'name_of_act', 'notice', 'first_reading', 'second_reading', 'to_standing_committee',
                        'amendment_date', 'reported', 'third_reading', 'assent']
        self.table_reader = PDF_Table_Reader()

    def __get_status_of_bills_link(self):
        status_div = MAIN_PAGE_SOUP.find('div', {'class' : 'view-status-of-bills'})
        return status_div.a['href']
    
    def get_status_of_bills_data(self):
        pdf_pages = self.table_reader.get_pdf_pages(self.status_of_bills_link, scraper_utils._request_headers)
        self.__initialize_pdf_reader()
        tables = self.table_reader.get_table(pdf_pages)
        tables_in_df = [pd.DataFrame(table, columns=self.columns) for table in tables]
        merged_df = pd.concat(tables_in_df)
        return merged_df.drop(0)

    def __initialize_pdf_reader(self):
        self.table_reader.set_page_width_ratio(width_in_inch=11)
        self.table_reader.set_page_height_ratio(height_in_inch=8.5)
        self.table_reader.set_page_top_spacing_in_inch(top_spacing_in_inch=1.15)
        self.table_reader.set_page_bottom_spacing_in_inch(bottom_spacing_in_inch=1.45)

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
        self.summary_page_number = 0 #This is updated in extract summary. This value is index 1, not index 0
        self.__initialize_pdf_reader()
        self.__set_row_data()

    def get_bill_data(self):
        return self.row

    def __initialize_pdf_reader(self):
        self.pdf_pages = self.pdf_reader.get_pdf_pages(self.bill_url, scraper_utils._request_headers)
        self.pdf_reader.set_page_width_ratio(width_in_inch=8.5)
        self.pdf_reader.set_page_half(page_half_in_inch=4.32)
        self.pdf_reader.set_page_height_ratio(height_in_inch=11.0)
        self.pdf_reader.set_page_top_spacing_in_inch(top_spacing_in_inch=1.15)
        self.pdf_reader.set_left_column_end_and_right_column_start(column1_end=4.28, column2_start=4.40)
        self.pdf_reader.set_page_bottom_spacing_in_inch(bottom_spacing_in_inch=1.15)
        scraper_utils.crawl_delay(crawl_delay)

    def __set_row_data(self):
        self.row.session = CURRENT_SESSION
        self.row.source_url = self.bill_url
        self.row.chamber_origin = 'Legislative Assembly'
        self.row.bill_name = self.__get_bill_name()
        self.row.goverlytics_id = self.__get_goveryltics_id()
        self.row.bill_title = self.__get_bill_title()
        self.row.bill_summary = self.__get_bill_summary()
        self.row.bill_text = self.__get_bill_text()
        self.row.actions = self.__get_bill_actions()
        self.row.current_status = self.__get_current_status()
        self.row.bill_type = self.__get_bill_type()
        self.row.date_introduced = self.__get_date_introduced()

    def __get_bill_name(self):
        return 'Bill' + self.__extract_bill_number_from_url()

    def __extract_bill_number_from_url(self):
        bill = re.findall(r'[A-Za-z]{4}_[0-9]{1,3}', self.bill_url)[0]
        return re.findall(r'[0-9]{1,3}', bill)[0].strip()

    def __get_goveryltics_id(self):
        return PROV_TERR_ABBREVIATION + '_' + CURRENT_SESSION + '_' + self.row.bill_name
    
    def __get_bill_title(self):
        first_page = self.pdf_pages[0]
        first_page_text = self.__get_pdf_text(first_page)
        bill_title = self.__extract_bill_title(first_page_text)
        return bill_title.lower().capitalize()

    def __extract_bill_title(self, text):
        return_text = re.split(r'BILL [0-9]{1,3}', text)
        return_text = return_text[1].split('Summary')[0]
        return_text = return_text.split('DISPOSI')[0]
        return_text = return_text.split('REPRINT')[0]
        return self.__clean_up_text(return_text)

    def __get_bill_summary(self):
        # The summary is likely in the first few pages
        pages_to_search_for = self.pdf_pages[0:2]
        return self.__extract_bill_summary(pages_to_search_for)
        
    def __extract_bill_summary(self, pages):
        for page in pages:
            text = self.__get_pdf_text(page)
            if 'Summary' in text:
                return_text = text.split('Summary')[1]
                return_text = return_text.split('DISPOSI')[0]
                self.summary_page_number = page.page_number
                return self.__clean_up_text(return_text)
        return None

    def __get_bill_text(self):
        pages_to_get = self.pdf_pages[self.summary_page_number:]
        return_text = ""
        for page in pages_to_get:
            text = self.__get_pdf_text(page)
            return_text += self.__clean_up_text(text)
        return return_text

    def __get_pdf_text(self, page):
        return self.pdf_reader.get_text(page)

    def __clean_up_text(self, text):
        text = text.replace('\n', ' ')
        text = text.replace('  ', ' ')
        return text.strip()

    def __get_bill_actions(self):
        bill_name_that_matches_status_of_bills = self.__adjust_bill_name_to_match_status_of_bills()
        bill_row_from_df = self.__get_bill_row_from_df(bill_name_that_matches_status_of_bills)
        relevant_cells = bill_row_from_df[:1:-1]
        actions = []
        for index, cell in enumerate(relevant_cells):
            action = self.__add_action_attribute(cell, index)
            if action:
                actions.append(action)
        return actions
    
    def __adjust_bill_name_to_match_status_of_bills(self):
        bill_number = self.row.bill_name.split('Bill')[1]
        session_split = CURRENT_SESSION.split('-')
        session = session_split[0] + '(' + session_split[1] + ')'
        return bill_number + '-' + session

    def __get_bill_row_from_df(self, bill_name):
        row = BILL_STATUS_DATA_FRAME.loc[BILL_STATUS_DATA_FRAME['bill_number'] == bill_name]
        return row.values[0]

    def __add_action_attribute(self, cell, cell_position):
        if (len(cell) == 0 or cell == 'N/A'):
            return
        cell = self.__remove_unneccessary_text(cell)
        try:
            date = datetime.datetime.strptime(cell, '%B %d, %Y')
        except Exception:
            date = datetime.datetime.strptime(cell, '%b. %d, %Y')
        date = date.strftime('%Y-%m-%d')
        description = self.__match_cell_position_to_action(cell_position)
        action_by = self.__get_action_by(description)
        return {'date' : date, 'action_by' : action_by, 'description' : description}

    def __remove_unneccessary_text(self, cell):
        cell = cell.replace('\n', '')
        cell = cell.replace('RV', '').strip()
        cell_dates = re.findall(r'[A-Za-z]{3}\. [0-9]{1,2}, [0-9]{4}', cell)
        if len(cell_dates) >= 1:
            return cell_dates[-1]

        cell_dates = re.findall(r'[A-Za-z]{3,12} [0-9]{1,2}, [0-9]{4}', cell)
        if len(cell_dates) >= 1:
            return cell_dates[-1]

    def __match_cell_position_to_action(self, position):
        if position == 0:
            return 'Assent'
        if position == 1:
            return 'Third Reading'
        if position == 2:
            return 'Reported From C of W'
        if position == 3:
            return 'Amendment'
        if position == 4:
            return 'To Standing Committee'
        if position == 5:
            return 'Second Reading'
        if position == 6:
            return 'First Reading'
        if position == 7:
            return 'Notice'

    def __get_action_by(self, action_description):
        if action_description == 'Assent':
            return 'Commissioner'
        if 'Committee' in action_description:
            return 'Committee'
        return 'Legislative Assembly'

    def __get_current_status(self):
        return self.row.actions[0]['description']

    def __get_bill_type(self):
        if 'amend' in self.row.bill_title.lower():
            return 'Amendment'
        else:
            return 'Bill'

    def __get_date_introduced(self):
        for action in self.row.actions[::-1]:
            if action['description'] == 'First Reading':
                return action['date']

Main_Functions().set_main_page_soup_global(BILLS_URL)
Bill_Main_Page_Scraper().set_current_session_global()
BILL_STATUS_DATA_FRAME = Status_Of_Bills().get_status_of_bills_data()

if __name__ == '__main__':
    program_driver()