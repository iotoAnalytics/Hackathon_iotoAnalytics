'''
'''
import sys, os
from pathlib import Path

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

from legislator_scraper_utils import CadProvTerrLegislatorScraperUtils
from bs4 import BeautifulSoup
import requests
from multiprocessing import Pool
from database import Database
import configparser
from pprint import pprint
from nameparser import HumanName
import re
import boto3


scraper_utils = CadProvTerrLegislatorScraperUtils('BC', 'cad_bc_legislators')

def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    pass


def scrape(url):
    pass


if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # data = [scrape(url) for url in urls]
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database.
    scraper_utils.insert_legislator_data_into_db(data)

    print('Complete!')
