import sys
import os
from pathlib import Path
from . import data_collector

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
from scraper_utils import USStateLegislationScraperUtils

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

STATE_ABBREVIATION = 'WA'
DATABASE_TABLE_NAME = 'us_wa_legislation'
LEGISLATOR_TABLE_NAME = 'us_wa_legislators'

scraper_utils = USStateLegislationScraperUtils(STATE_ABBREVIATION,
                                               DATABASE_TABLE_NAME,
                                               LEGISLATOR_TABLE_NAME)

def program_driver():
    all_bills = data_collector.program_driver()
    
def process_data(bills):
    data = []
    for bill in bills:
        data.append(DataOrganize().set_rows(bill))
    return data

class DataOrganize:
    def __init__(self):
        self.row = scraper_utils.initialize_row()

    def set_rows(self, bill):
        # self.row.session = data_collector.CURRENT_BIENNIUM
        pass

if __name__ == "__main__":
    program_driver()