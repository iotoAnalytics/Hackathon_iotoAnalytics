
import sys
import os
from pathlib import Path
from nameparser import HumanName
from multiprocessing import Pool
from bs4 import BeautifulSoup
from scraper_utils import CAProvinceTerrLegislationScraperUtils
import dateutil.parser as dparser

# Get path to the root directory so we can import necessary modules
p = Path(os.path.abspath(__file__)).parents[5]

sys.path.insert(0, str(p))

prov_terr_abbreviation = 'NS'
database_table_name = 'ca_ns_legislation'
legislator_table_name = 'ca_ns_legislators'
scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)

base_url = 'https://nslegislature.ca'
# Get scraper delay from website robots.txt file
crawl_delay = scraper_utils.get_crawl_delay(base_url)


def get_urls():
    '''
    Insert logic here to get all URLs you will need to scrape from the page.
    '''
    urls = []

    # Logic goes here! Some sample code:
    path = '/legislative-business/bills-statutes/bills/'
    scrape_url = base_url + path
    page = scraper_utils.request(scrape_url)
    soup = BeautifulSoup(page.content, 'html.parser')

    table = soup.findAll('td', {'class': 'views-field-field-short-title-1'})

    for td in table:
        urls.append(td.a['href'])
    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)
    return urls


def get_bill_name(url, row):
    bill_number = url.split('bill-')[1]
    zero_filled_number = bill_number.zfill(3)
    bill_name = 'B' + zero_filled_number
    row.bill_name = bill_name
    return bill_name


def get_session(main_div, row):
    table = main_div.find('table', {'class': 'views-table'})
    session = table.findAll('td')[2].text
    session = session.replace(',', '')
    session = session.split(' ')
    return_session = session[1] + "-{s}".format(s=session[3])
    row.session = return_session
    return return_session


def get_bill_type(main_div, row):
    table = main_div.find('table', {'class': 'views-table'})
    bill_type = table.findAll('td')[1].text
    row.bill_type = bill_type


def get_bill_title(main_div, row):
    title = main_div.find('h1')
    title = title.text.split(' - ')[0]
    try:
        title = title.split('- Bill')[0]
    except Exception:
        pass
    title = title.replace('\n', '')
    row.bill_title = title


def get_current_status(main_div, row):
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    status = table_row[-1].findAll('td')[0].text
    if "Law Amendments" in status:
        status = table_row[-2].findAll('td')[0].text
    row.current_status = status


def get_actions(main_div, row):
    actions = []
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    try:
        for tr in reversed(table_row):
            status = tr.findAll('td')[0].text
            date = tr.findAll('td')[1].text
            try:
                date = dparser.parse(date, fuzzy=True)
                date = date.strftime("%Y-%m-%d")
            except Exception:
                date = None
            if status:
                if date:
                    action = {'date': date, 'action_by': 'Legislative Assembly', 'description': status}
                    actions.append(action)
    except Exception:
        pass

    row.actions = actions


def get_get_date_introduced(main_div, row):
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    introduced_text = table_row[1].findAll('td')[1].text
    date = dparser.parse(introduced_text, fuzzy=True)
    date = date.strftime("%Y-%m-%d")
    row.date_introduced = date


def get_bill_description(main_div, row):
    text = main_div.find('div', {'class': 'pane-ns-leg-bill-metadata'}).text
    text = text.split('Introduced')[0]
    text = text[text.index('An'):]
    text = text.replace('\n', ' ')
    row.bill_description = text


def get_bill_text(url, row):
    page = scraper_utils.request(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    bill_text = soup.find('div', {'class': 'bill_text'}).text.strip()
    scraper_utils.crawl_delay(crawl_delay)
    row.bill_text = bill_text


def get_bill_link(main_div, row):
    table = main_div.find('table', {'class': 'bill-metadata-table'})
    table_row = table.findAll('tr')
    try:
        for tr in table_row:
            text = tr.text
            if "Statute" in text:
                link = tr.find('a').get('href')
            elif "Bill as introduced" in text:
                link = tr.find('a').get('href')
        url = base_url + link
        get_bill_text(url, row)
    except Exception:
        pass


def get_sponsor_id(name_first, name_last):
    search_for = dict(name_last=name_last, name_first=name_first)
    sponsor_id = scraper_utils.get_legislator_id(**search_for)
    return sponsor_id


def get_principal_sponsor(main_div, row):
    text = main_div.find('div', {'class': 'pane-ns-leg-bill-metadata'}).text
    text = text.split('Introduced by ')[1]
    name = text.split(',')[0]
    hn = HumanName(name)
    name_full = name
    name_last = hn.last
    name_first = hn.first

    row.principal_sponsor = name_full
    row.principal_sponsor_id = get_sponsor_id(name_first, name_last)


def scrape(url):

    row = scraper_utils.initialize_row()

    source_url = base_url + url
    row.source_url = source_url
    row.region = scraper_utils.get_region(prov_terr_abbreviation)
    row.chamber_origin = 'Legislative Assembly'

    page = scraper_utils.request(source_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    main_div = soup.find('div', {'class': 'panel-display panel-1col clearfix'})

    bill_name = get_bill_name(source_url, row)
    bill_name = bill_name.upper()
    session = get_session(main_div, row)

    goverlytics_id = f'{prov_terr_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id

    get_bill_type(main_div, row)
    get_bill_title(main_div, row)
    get_current_status(main_div, row)
    get_actions(main_div, row)
    get_bill_description(main_div, row)
    get_bill_link(main_div, row)
    get_principal_sponsor(main_div, row)
    get_get_date_introduced(main_div, row)

    # Delay so we do not overburden servers
    scraper_utils.crawl_delay(crawl_delay)

    return row


if __name__ == '__main__':
    print('NOTE: This demo will provide warnings since some legislators are missing from the database.\n\
If this occurs in your scraper, be sure to investigate. Check the database and make sure things\n\
like names match exactly, including case and diacritics.\n~~~~~~~~~~~~~~~~~~~')

    urls = get_urls()

    with Pool() as pool:
        data = pool.map(scrape, urls)

    scraper_utils.write_data(data)

    print('Complete!')
