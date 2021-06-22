import datetime
import sys
import os
from pathlib import Path

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
import re
from scraper_utils import USStateLegislationScraperUtils
from urllib.request import urlopen
from multiprocessing import Pool
from langdetect import detect
from bs4 import BeautifulSoup as soup
import requests

STATE_ABBREVIATION = 'WA'
DATABASE_TABLE_NAME = 'us_wa_legislation'
LEGISLATOR_TABLE_NAME = 'us_wa_legislators'

BASE_URL = 'http://wslwebservices.leg.wa.gov/'
REQUEST_URL_FOR_GETTING_BILLS = BASE_URL + 'LegislativeDocumentService.asmx/GetAllDocumentsByClass'
REQUEST_URL_FOR_GETTING_SPONSORS = BASE_URL + 'LegislationService.asmx/GetSponsors'
REQUEST_URL_FOR_GETTING_BILL_DETAILS = BASE_URL + 'LegislationService.asmx/GetLegislation'

THREADS_FOR_POOL = 12
CURRENT_DAY = datetime.date.today()
CURRENT_YEAR = CURRENT_DAY.year

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION,
                                               DATABASE_TABLE_NAME,
                                               LEGISLATOR_TABLE_NAME)
crawl_delay = scraper_utils.get_crawl_delay(BASE_URL)

def program_driver():
    all_bills = AllDocumentsByClass().get_data()
    all_bills = MainFunctions().append_data_to_bills(SponsorFromBillId().add_sponsor_info_to_bill,
                                                     all_bills)
    all_bills = MainFunctions().append_data_to_bills(BillDetailsFromBillId().add_bill_details_to_bill,
                                                     all_bills)
    print(all_bills[600:610])

class PreProgramFunction:
    def get_biennium(self, year: int):
        if year % 2 == 0:
            past_year = year - 1
            return str(past_year) + '-' + str(year)[2:]
        else:
            next_year = year + 1
            return str(year) + '-' + str(next_year)[2:]

class MainFunctions:
    def request_page(self, url, params):
        return requests.get(url, params=params)

    def append_data_to_bills(self, function, iterable):
        data = []
        with Pool(THREADS_FOR_POOL) as pool:
            data = (pool.map(func=function,
                        iterable=iterable))
        return data

class AllDocumentsByClass:
    def get_data(self):
        return self.get_relevant_bill_information()

    def get_relevant_bill_information(self):
        bill_info_as_lxml = self.get_all_bill_information_lxml()
        bill_info = [self.__extract_relevant_info(bill_lxml) for bill_lxml in bill_info_as_lxml]
        while None in bill_info:
            bill_info.remove(None)
        return bill_info

    def __extract_relevant_info(self, bill_lxml):
        name = bill_lxml.find('name').text
        if re.search(r'\D', name):
            return None

        htmlurl = bill_lxml.find('htmurl').text
        pdfurl = bill_lxml.find('pdfurl').text
        billid = bill_lxml.find('billid').text
        return {
            'bill_number': name,
            'htmurl': htmlurl,
            'pdfurl': pdfurl,
            'bill_id': billid 
        }

    def get_all_bill_information_lxml(self):
        params = {
            "biennium": CURRENT_BIENNIUM,
            "documentClass": "Bills"
        }
        request = MainFunctions().request_page(REQUEST_URL_FOR_GETTING_BILLS, params=params)
        page_soup = soup(request.text, 'lxml')
        return page_soup.findAll('legislativedocument')

class SponsorFromBillId:
    def add_sponsor_info_to_bill(self, bill: dict):
        sponsor_info = self.get_relevant_bill_information(bill.get('bill_id'))
        bill['sponsors'] = sponsor_info
        return bill

    def get_relevant_bill_information(self, bill_id):
        sponsor_info_as_lxml = self.get_sponsor_information_for_bill_lxml(bill_id)
        return [self.__extract_relevant_info(sponsor_lxml) for sponsor_lxml in sponsor_info_as_lxml]
   
    def __extract_relevant_info(self, sponsor_lxml):
            fname = sponsor_lxml.find('firstname').text
            lname = sponsor_lxml.find('lastname').text
            type = sponsor_lxml.find('type').text

            return {
                'firstname': fname,
                'lastname': lname,
                'type': type
            }

    def get_sponsor_information_for_bill_lxml(self, bill_id):
        params = {
            "biennium": CURRENT_BIENNIUM,
            "billId": bill_id
        }
        request = MainFunctions().request_page(REQUEST_URL_FOR_GETTING_SPONSORS, params=params)
        page_soup = soup(request.text, 'lxml')
        return page_soup.findAll('sponsor')

class BillDetailsFromBillId:
    def add_bill_details_to_bill(self, bill: dict):
        bill_details = self.get_relevant_bill_information(bill.get('bill_number'))
        bill['bill_title'] = bill_details['legal_title']
        bill['chamber_origin'] = bill_details['bill_origin']
        bill['current_status'] = bill_details['bill_current_status']
        bill['bill_description'] = bill_details['bill_description']
        bill['bill_type'] = bill_details['bill_type']
        return bill

    def get_relevant_bill_information(self, bill_number):
        bill_details_as_lxml = self.get_bill_details_lxml(bill_number)
        return self.__extract_relevant_info(bill_details_as_lxml)

    def __extract_relevant_info(self, bill_lxml):
            billtype = bill_lxml.find('longlegislationtype').text
            billorigin = bill_lxml.find('originalagency').text
            try:
                currentstatus = bill_lxml.find('historyline').text
            except:
                currentstatus = ''
            billdescription = bill_lxml.find('longdescription').text
            legaltitle = bill_lxml.find('legaltitle').text

            return {
                'bill_type': billtype,
                'bill_origin': billorigin,
                'bill_current_status': currentstatus,
                'bill_description': billdescription,
                'legal_title': legaltitle,
            }

    def get_bill_details_lxml(self, bill_number):
        try:
            params = {
                "biennium": CURRENT_BIENNIUM,
                "billNumber": int(bill_number)
            }
        except:
            print(bill_number)
        request = MainFunctions().request_page(REQUEST_URL_FOR_GETTING_BILL_DETAILS, params=params)
        page_soup =  soup(request.text, 'lxml')
        return page_soup.find('legislation')

CURRENT_BIENNIUM = PreProgramFunction().get_biennium(CURRENT_YEAR)

if __name__ == '__main__':
    program_driver()
    SponsorFromBillId()