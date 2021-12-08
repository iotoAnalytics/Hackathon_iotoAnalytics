'''
This template is meant to serve as a general outline, and will not necessarily work for
all collectors. Feel free to modify the script as necessary.
'''
import os
import sys
from pathlib import Path

# Get path to the root directory so we can import necessary modules
NODES_TO_ROOT = 5
p = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(p))

from bs4 import BeautifulSoup as soup
from scraper_utils import USStateLegislatorScraperUtils # This tempalte is for an American State
from multiprocessing import Pool
from nameparser import HumanName

state_abbreviation = 'AZ'
database_table_name = 'legislator_template_test'

scraper_utils = USStateLegislatorScraperUtils(
    state_abbreviation, database_table_name)

base_url = 'https://webscraper.io'
# Get the crawl delay specified in the website's robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)

def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    path = '/test-sites/e-commerce/allinone'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    page_soup = soup(page.content, 'html.parser')
    urls = [base_url + prod_path['href']
            for prod_path in page_soup.findAll('a', {'class': 'title'})]

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return urls

def scrape(url):
    # Send request to website
    page = scraper_utils.request(url)
    page_soup = soup(page.content, 'html.parser')

    row = scraper_utils.initialize_row()

    # ... Collect data from page

    # Delay so we don't overburden web servers
    scraper_utils.crawl_delay(crawl_delay)

    return row

if __name__ == '__main__':
    # First we'll get the URLs we wish to scrape:
    urls = get_urls()

    # Scrape data from collected URLs serially, which is slower:
    # data = [scrape(url) for url in urls]
    # Speed things up using pool.
    with Pool() as pool:
        data = pool.map(scrape, urls)

    # Once we collect the data, we'll write it to the database:
    scraper_utils.write_data(data)

    print('Complete!')
