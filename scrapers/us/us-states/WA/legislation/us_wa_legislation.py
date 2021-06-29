import datetime
import sys
import os
from pathlib import Path
import data_collector

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

STATE_ABBREVIATION = 'WA'
DATABASE_TABLE_NAME = 'us_wa_legislation'
LEGISLATOR_TABLE_NAME = 'us_wa_legislators'

def program_driver():
    print("collecting data...")
    all_bills = data_collector.program_driver()
    print("processing data...")
    data = process_data(all_bills)
    print("Writing data to database...")
    data_collector.scraper_utils.write_data(data)
    print("Complete")

    
def process_data(bills):
    data = []
    for bill in bills:
        data.append(DataOrganize(bill).get_rows())
    return data

class DataOrganize:
    def __init__(self, bill):
        self.row = data_collector.scraper_utils.initialize_row()
        self.set_rows(bill)

    def get_rows(self):
        return self.row

    def set_rows(self, bill):
        self.row.session = data_collector.CURRENT_BIENNIUM
        self.row.bill_name = bill['bill_number']
        self.row.goverlytics_id = self.__set_goverlytics_id()
        self.row.source_url = self.__format_source_url(bill['pdfurl'])
        self.row.chamber_origin = bill['chamber_origin']
        self.row.committees = bill['committees']
        self.row.bill_type = bill['bill_type']
        self.row.bill_description = bill['bill_description']
        self.row.bill_title = bill['bill_title']
        self.row.current_status = bill['current_status']
        self.__set_principal_sponsor_data(bill['sponsors'])
        self.__set_sponsor_info(bill['sponsors'])
        self.row.actions = bill['actions']
        self.__format_actions()
        try:
            self.row.date_introduced = self.row.actions[0]['date']
        except:
            self.row.date_introduced = None
        self.row.votes = bill['votes']
        self.__format_votes()
        self.row.bill_text = 'lorem ipsum'
    
    def __set_goverlytics_id(self):
        session = self.row.session.split('-')[0] + '20' + self.row.session.split('-')[1]
        return STATE_ABBREVIATION + '_' + session + '_' +self.row.bill_name

    def __format_source_url(self, url):
        return url.replace(' ', '_')

    def __set_principal_sponsor_data(self, sponsors):
        primary_sponsor = self.__find_primary_sponsor(sponsors)
        full_name = primary_sponsor['firstname'] + ' ' + primary_sponsor['lastname']
        self.row.principal_sponsor = full_name
        self.row.principal_sponsor_id = data_collector.scraper_utils.get_legislator_id(name_full=full_name)

    def __find_primary_sponsor(self, sponsors):
        for sponsor in sponsors:
            if sponsor['type'] == 'Primary':
                return sponsor

    def __set_sponsor_info(self, sponsors):
        self.row.sponsors = [sponsor['lastname'] for sponsor in sponsors]
        self.row.sponsors_id = self.__get_sponsors_id(sponsors)

    def __get_sponsors_id(self, sponsors):
        sponsors_ids = []
        for sponsor in sponsors:
            full_name = sponsor['firstname'] + ' ' + sponsor['lastname']
            sponsors_ids.append(data_collector.scraper_utils.get_legislator_id(name_full=full_name))
        return sponsors_ids

    def __format_actions(self):
        for action in self.row.actions:
            self.__format_date(action)
            self.__set_action_by(action)

    def __format_date(self, action):
        date = action['date'].split('T')[0]
        action['date'] = date

    def __set_action_by(self, action):
        description = action['description'].lower()

        if 'reading' in description:
            action['action_by'] = 'chamber'
        elif 'committee' in description:
            action['action_by'] = 'committee'
        elif 'speaker' in description:
            action['action_by'] = 'speaker'
        elif 'governor' in description:
            action['action_by'] = 'governor'
        elif 'president' in description:
            action['action_by'] = 'president'
        elif 'effective date' in description  or 'chapter' in description:
            action['action_by'] = 'secretary of state'
        else:
            action['action_by'] = 'chamber'

    def __format_votes(self):
        for vote in self.row.votes:
            vote['date'] = vote['date'].split('T')[0]
    
if __name__ == "__main__":
    program_driver()