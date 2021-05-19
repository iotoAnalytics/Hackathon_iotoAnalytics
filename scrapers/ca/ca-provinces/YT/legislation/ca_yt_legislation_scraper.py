import sys
import os
from pathlib import Path
from time import sleep
import re
from enum import Enum
import datetime

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import CAProvinceTerrLegislationScraperUtils

import pandas as pd
import pdfplumber
import io
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

'''
Change the current legislature to the most recent
This scraper will not work if the current session isn't populated with 4 bills
    for government and non-government bills
'''
CURRENT_LEGISLATURE = 35
PROV_TERR_ABBREVIATION = 'YT'
DATABASE_TABLE_NAME = 'ca_yt_legislation'
LEGISLATOR_TABLE_NAME = 'ca_yt_legislators'
BASE_URL = 'https://yukonassembly.ca'
BILLS_URL = BASE_URL + '/house-business/progress-bills'


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

options = Options()
options.headless = True

driver = webdriver.Chrome('web_drivers/chrome_win_90.0.4430.24/chromedriver.exe', options=options)
driver.switch_to.default_content()

scraper_utils = CAProvinceTerrLegislationScraperUtils(PROV_TERR_ABBREVIATION,
                                                      DATABASE_TABLE_NAME,
                                                      LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

class Main_Program:
    def program_driver(self):
        self.__open_url()
        current_session = self.__get_current_session()
        try:
            scraper = SessionScraper(current_session)
        finally:
            self.__end_driver()

        print('Writing data to database...')
        scraper_utils.write_data(scraper.data)
        print("Complete")

    def __open_url(self):
        driver.get(BILLS_URL)
        driver.maximize_window()
        sleep(2) 

    def __get_current_session(self):
        dropdown_menu = driver.find_element_by_tag_name('select')
        return self.__find_current_sessions(dropdown_menu)

    def __find_current_sessions(self, dropdown_menu):
        dropdown_options = dropdown_menu.find_elements_by_tag_name('option')
        for legislature in dropdown_options:
            if self.__is_current_legislature(legislature):
                return legislature

    def __is_current_legislature(self, legislature):
        return str(CURRENT_LEGISLATURE) in legislature.text

    def __end_driver(self):
        driver.close()
        driver.quit()

class SessionScraper:
    class TableRow(Enum):
        Name = 0
        Title = 1
        Sponsor = 3
        First_Reading = 4
        Second_Reading = 5
        Reported_By_Committee = 6
        Third_Reading = 7
        Assent = 8

    def __init__(self, session):
        session.click()
        self.current_session = self.__get_current_session_as_num(session)
        self.data = self.__scrape()
    
    def __get_current_session_as_num(self, session):
        session_as_text = session.text.split(' - ')[1]
        return self.__check_session(session_as_text)

    def __check_session(self, session_text):
        if 'Third' in session_text:
            return 3
        if'Second' in session_text:
            return 2
        if 'First' in session_text:
            return 1

    def __scrape(self):
        active_page_content = self.__get_active_page()
        bill_rows = self.__get_relevant_table_rows(active_page_content)
        bill_data = []
        for bill_row in bill_rows:
            if not self.__bill_already_exists(bill_row, bill_data):
                bill_data.append(self.__get_row_data(bill_row))
        return bill_data
        
    def __get_active_page(self):
        main_content = driver.find_element_by_css_selector('body > div > div > div > section > div.region.region-content > article > div > div.paragraph.paragraph--type--tabbed-content.paragraph--view-mode--default > div > div.tabs--content')
        main_content_direct_child_divs = main_content.find_elements_by_xpath('./*')
        for div in main_content_direct_child_divs:
            if self.__is_not_hidden_div(div):
                return div

    def __is_not_hidden_div(self, div):
        return 'hidden' not in div.get_attribute('class')

    def __get_relevant_table_rows(self, active_page_content):
        all_tables = active_page_content.find_elements_by_tag_name('table')
        rows = []
        for table in all_tables:
            self.__get_rows(table, rows)
        return rows

    def __get_rows(self, table, rows):
        all_trs = table.find_elements_by_tag_name('tr')[2:]
        bill_type = 'Government Bill'
        for tr in all_trs:
            if len(tr.find_elements_by_xpath('./*')) != 1:
                rows.append({'type' : bill_type,
                             'row' : tr})
            else:
                bill_type = 'Non-Government Bill'
    
    def __bill_already_exists(self, row, data_list):
        for data_set in data_list:
            if data_set.bill_name == self.__get_bill_name(row['row']):
                return True
        return False

    def __get_row_data(self, bill_row):
        row = scraper_utils.initialize_row()
        row.source_id = self.__get_source_id(bill_row['row'])
        row.bill_name =self.__get_bill_name(bill_row['row'])
        row.session = str(CURRENT_LEGISLATURE) + "-" + str(self.current_session)
        row.goverlytics_id = self.__get_goveryltics_id(row)
        row.date_introduced = self.__get_date(bill_row['row'])
        # All bills are on one page.
        row.source_url = self.__get_pdf_link(bill_row['row'])
        row.chamber_origin = "Legislative Assembly"
        row.bill_type = bill_row['type']
        row.bill_title = self.__get_bill_title(bill_row['row'])
        row.principal_sponsor = self.__get_principal_sponsor(bill_row['row'])
        row.principal_sponsor_id = self.__get_principal_sponsor_id(row)
        row.actions = self.__get_actions(bill_row['row'])
        row.current_status = self.__get_current_status(row.actions)
        self.__get_bill_info(row)
        row.region = scraper_utils.get_region(PROV_TERR_ABBREVIATION)
        return row
    
    def __get_source_id(self, bill_row):
        bill_pdf_link = self.__get_pdf_link(bill_row)
        return re.findall(r'[0-9]{2}-[0-9]-[A-Za-z]{4}[0-9]{3}', bill_pdf_link)[0]

    def __get_pdf_link(self, bill_row):
        a_tag = bill_row.find_element_by_tag_name('a')
        return a_tag.get_attribute('href')

    def __get_bill_name(self, bill_row):
        return "Bill-" + bill_row.find_elements_by_tag_name('td')[SessionScraper.TableRow.Name.value].text
    
    def __get_goveryltics_id(self, data_row):
        bill_name = data_row.bill_name.replace('-', '').upper()
        return PROV_TERR_ABBREVIATION + '_' + data_row.session + '_' + bill_name

    def __get_date(self, bill_row):
        first_reading = bill_row.find_elements_by_tag_name('td')[SessionScraper.TableRow.First_Reading.value].text
        date = datetime.datetime.strptime(first_reading, '%B %d, %Y')
        date = date.strftime('%Y-%b-%d')
        return date

    def __get_bill_title(self, bill_row):
        bill_title = bill_row.find_elements_by_tag_name('td')[SessionScraper.TableRow.Title.value].text
        if '\n' in bill_title:
            return bill_title.split('\n')[0]
        return bill_title

    def __get_principal_sponsor(self, bill_row):
        name = bill_row.find_elements_by_tag_name('td')[SessionScraper.TableRow.Sponsor.value].text
        return re.findall('[A-Za-z]+', name)[0]

    def __get_principal_sponsor_id(self, row):
        search_for = {'name_last' : row.principal_sponsor}
        try:
            sponsor_id = scraper_utils.get_legislator_id(** search_for)
            sponsor_id = int(sponsor_id)
        except Exception:
            sponsor_id = 0
        return sponsor_id

    def __get_actions(self, bill_row):
        relevant_cells = bill_row.find_elements_by_tag_name('td')[:3:-1]
        for index in range(len(relevant_cells)):
            relevant_cells[index] = self.__remove_new_line_from_text_if_empty(relevant_cells[index])

        actions = []
        for index in range(len(relevant_cells)):
            current_cell = relevant_cells[index]
            action = (self.__add_action_attribute(current_cell, index))
            if action:
                actions.append(action)
        return actions
    
    def __remove_new_line_from_text_if_empty(self, cell):
        return cell.text.strip()

    def __add_action_attribute(self, cell, cell_position):
        if (len(cell) == 0):
            return
        cell = self.__remove_unneccessary_text(cell)
        date = datetime.datetime.strptime(cell, '%B %d, %Y')
        date = date.strftime('%Y-%b-%d')
        description = self.__match_cell_position_to_action(cell_position)
        action_by = self.__get_action_by(description)
        return {'date' : date, 'action_by' : action_by, 'description' : description}
    
    def __remove_unneccessary_text(self, cell):
        cell = cell.replace('Amendments', '')
        return cell.replace('Amendment', '').strip()

    def __match_cell_position_to_action(self, position):
        if position == 0:
            return 'Assent'
        if position == 1:
            return 'Third Reading'
        if position == 2:
            return 'Reported by Committee'
        if position == 3:
            return 'Second Reading'
        if position == 4:
            return 'First Reading'

    def __get_action_by(self, action_description):
        if action_description == 'Assent':
            return 'Commissioner'
        if 'Committee' in action_description:
            return 'Committee'
        return 'Legislative Assembly'

    def __get_current_status(self, actions):
        return actions[0]['description']

    def __get_bill_info(self, row):
        pdf_url = row.source_url
        pdf = self.__open_pdf(pdf_url)
        pages = self.__get_pdf_pages(pdf)
        if not self.__explanatory_note_exists(pages[0:5]):
            row.bill_text = self.__get_bill_text_with_no_explanatory_note(pages)
        else:
            self.__set_bill_summary_and_text(row, pages)

    def __open_pdf(self, pdf_url):
        response = requests.get(pdf_url, headers=scraper_utils._request_headers, stream=True)
        scraper_utils.crawl_delay(crawl_delay)
        return pdfplumber.open(io.BytesIO(response.content))   

    def __get_pdf_pages(self, pdf):
        # The 0th page for these pdfs are all a title page, hence why we splice from 1st
        return pdf.pages[1:]

    def __explanatory_note_exists(self, pages):
        '''
        Some pages have a blank page then an explanatory note...
        '''
        for page in pages:
            eng_half_of_page = self.__get_eng_half(page)
            if eng_half_of_page == None:
                return False
            if 'EXPLANATORY NOTE' in eng_half_of_page:
                return True
        return False

    def __get_eng_half(self, page):
        APPROX_PAGE_HALF = 0.485
        eng_half = page.crop((0, 0, float(page.width) * APPROX_PAGE_HALF, page.height))
        return eng_half.extract_text()

    def __get_bill_text_with_no_explanatory_note(self, pages):
        text = ''
        for page in pages:
            text += self.__extract_bill_text_with_no_explanatory_note(page)
        return self.__clean_up_text(text)

    def __extract_bill_text_with_no_explanatory_note(self, page):
        '''
        Some PDFs are bilingual and are divided by a column, where some are not.
        This will cause some troubles in extracting PDFs that have no columns.
        '''
        eng_half = self.__get_eng_half(page)
        if eng_half == None:
            return ''
        else:
            return eng_half

    def __set_bill_summary_and_text(self, row, pages):
        text = ''
        is_getting_summary = True
        for page in pages:
            if is_getting_summary:
                text += self.__extract_explanatory_note(page)
                if self.__check_if_end_of_explanatory_note(page):
                    row.bill_summary = self.__clean_up_text(text)
                    is_getting_summary = False
                    text = self.__extract_bill_text(page)
            else:
                text += self.__extract_bill_text(page)
        row.bill_text = self.__clean_up_text(text)

    def __extract_explanatory_note(self, page):
        eng_half_of_page = self.__get_eng_half(page)
        if 'EXPLANATORY NOTE' in eng_half_of_page:
            eng_half_of_page = eng_half_of_page.split('EXPLANATORY NOTE')[1]
        if 'BILL NO.' in eng_half_of_page:
            eng_half_of_page = eng_half_of_page.split('BILL NO.')[0]
        return eng_half_of_page

    def __extract_bill_text(self, page):
        return self.__get_eng_half(page)

    def __check_if_end_of_explanatory_note(self, page):
        eng_half_of_page = self.__get_eng_half(page)
        if 'BILL NO.' in eng_half_of_page:
            return True
        else:
            return False

    def __clean_up_text(self, text):
        text = text.replace('\n', ' ')
        text = text.replace('  ', ' ')
        return text.strip()

if __name__ == '__main__':
    main_program = Main_Program()
    main_program.program_driver()