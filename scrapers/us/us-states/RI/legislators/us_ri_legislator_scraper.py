
from bs4 import BeautifulSoup
from request_url import UrlRequest
import pandas as pd
from multiprocessing import Pool
from nameparser import HumanName
import configparser
from scraper_utils import USStateLegislatorScraperUtils
import re
from urllib.request import urlopen as uReq
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'}
sen_url = 'http://www.rilegislature.gov/senators/default.aspx'
rep_url = 'http://www.rilegislature.gov/representatives/default.aspx'
wiki_url = 'https://en.wikipedia.org/wiki/Rhode_Island_General_Assembly'
base_url = 'http://www.rilegislature.gov'

configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

state_abbreviation = str(configParser.get('scraperConfig', 'state_abbreviation'))
database_table_name = str(configParser.get('scraperConfig', 'database_table_name'))
country = str(configParser.get('scraperConfig', 'country'))
scraper_utils = USStateLegislatorScraperUtils(state_abbreviation, database_table_name)

crawl_delay = scraper_utils.get_crawl_delay(base_url)


def sen_wiki(url):
    sen_wiki_lst = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    table = url_soup.find('div', {'aria-labelledby': 'Current_members_of_the_Rhode_Island_Senate'})
    for item in table.find_all('tr')[-2].find_all('li'):
        if item.find('a').get('title'):
            party = ''
            if 'page does not exist' not in item.find('a').get('title'):
                if '(D)' in item.text:
                    party = 'Democrat'
                elif 'R' in item.text:
                    party = 'Republican'
                sen_wiki_lst.append({
                    'wiki_link': item.find('a').get('href'),
                    'party': party
                })
    return sen_wiki_lst


def rep_wiki(url):
    rep_wiki_lst = []
    url_request = UrlRequest.make_request(url, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    table = url_soup.find('div', {'aria-labelledby': 'Current_members_of_the_Rhode_Island_House_of_Representatives'})
    for item in table.find_all('tr')[-2].find_all('li'):
        if item.find('a').get('title'):
            party = ''
            if 'page does not exist' not in item.find('a').get('title'):
                if '(D)' in item.text:
                    party = 'Democrat'
                elif 'R' in item.text:
                    party = 'Republican'
                rep_wiki_lst.append({
                    'wiki_link': item.find('a').get('href'),
                    'party': party
                })
    return rep_wiki_lst



def get_party(link):
    try:
        url_request = UrlRequest.make_request(link, header)
        url_soup = BeautifulSoup(url_request.content, 'lxml')
        table = url_soup.find('table', {'class': 'infobox vcard'}).find_all('tr')
        for item in table:
            if 'Political party' in item.text:
                return item.find('a').text
    except AttributeError:
        pass


def get_sen_info(link):
    url_request = UrlRequest.make_request(link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    text = []
    try:
        p_text = url_soup.find('div', {'class': 'bio-welcome-note'}).find_all('p')
    except:
        p_text = url_soup.find_all('p')
    for item in p_text:
        if '\xa0' not in item.text and item.text != '\xa0\u200b\xa0':
            text.append(item.text)
    for item in text:
        if 'Sincerely' in item:
            num = text.index(item) + 1
    return text[num:]


def get_wiki_url(row):

    wikipage_reps = "https://ballotpedia.org/Rhode_Island_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Rhode_Island_State_Senate"

    if row.role == "Representative":
        try:
            uClient = uReq(wikipage_reps)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()
                party = party.replace('\n', '')
                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip() and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                except:
                        pass
                if not row.wiki_url:
                    for person in rows[1:]:
                        tds = person.findAll("td")
                        name_td = tds[1]
                        name = name_td.text
                        name = name.replace('\n', '')
                        party = tds[2].text
                        party = party.strip()

                        if party == "Democratic":
                            party = "Democrat"

                        if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                            row.wiki_url = name_td.a['href']
                            break
                        elif row.party == party and row.name_last in name.strip().split()[-1]:
                            row.wiki_url = name_td.a['href']
                            break
        except Exception as e:
            print(e)
    if row.role == "Senator":

        try:
            uClient = uReq(wikipage_senate)
            page_html = uClient.read()
            uClient.close()

            page_soup = BeautifulSoup(page_html, "lxml")
            tables = page_soup.findAll("table")
            rows = tables[3].findAll("tr")

            for person in rows[1:]:
                tds = person.findAll("td")
                name_td = tds[1]
                name = name_td.text
                name = name.replace('\n', '')
                party = tds[2].text
                party = party.strip()

                if party == "Democratic":
                    party = "Democrat"

                try:
                    if row.party == party and row.name_last in name.strip().split()[-1] and name.strip().split(" ")[0] in row.name_first:
                        row.wiki_url = name_td.a['href']
                        break
                except:
                    pass
            if not row.wiki_url:
                for person in rows[1:]:
                    tds = person.findAll("td")
                    name_td = tds[1]
                    name = name_td.text
                    name = name.replace('\n', '')
                    party = tds[2].text
                    party = party.strip()

                    if party == "Democratic":
                        party = "Democrat"

                    if row.party == party and row.name_last in name.strip() and row.name_first in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
                    elif row.party == party and row.name_last in name.strip():
                        row.wiki_url = name_td.a['href']
                        break
        except Exception as e:
            print(e)
            pass


def scrape_sen(sen_item):
    info = get_sen_info(sen_item['link'])
    row = scraper_utils.initialize_row()
    row.name_full = sen_item['name']
    row.name_first = sen_item['name_first']
    row.name_middle = sen_item['name_middle']
    row.name_last = sen_item['name_last']
    row.source_url = sen_item['link']
    row.role = sen_item['Role']

    row.district = info[1]
    row.areas_served = [x.strip() for x in info[2].split(',')]

    if 'party' in sen_item:
        party = sen_item['party']
        row.party = party
        if party != '':
            try:
                row.party_id = scraper_utils.get_party_id(party)
            except:
                pass

    if 'wiki_link' in sen_item:
        # party = get_party(sen_item['wiki_link'])
        wiki_info = scraper_utils.scrape_wiki_bio(sen_item['wiki_link'])
        row.birthday = wiki_info['birthday']
        row.education = wiki_info['education']
        row.occupation = wiki_info['occupation']
        row.years_active = wiki_info['years_active']
        row.most_recent_term_id = wiki_info['most_recent_term_id']
        get_wiki_url(row)
        gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
        if not gender:
            gender = 'O'
        row.gender = gender
    print(row)
    return row


def make_sen_lst(sen_link):
    url_request = UrlRequest.make_request(sen_link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    print(sen_link)
    try:
        table = url_soup.find('table', {'summary': 'Senators '}).find_all('a')
        sen_lst = []
        for item in table:
            if 'Senator' in item.text:
                name = item.text.replace('Senator', '').strip()
                link = 'http://www.rilegislature.gov' + item.get('href')
                sen_lst.append({
                    'link': link,
                    'name': name
                })

        return sen_lst
    except:
        pass


def make_rep_lst(rep_link):
    url_request = UrlRequest.make_request(rep_link, header)
    url_soup = BeautifulSoup(url_request.content, 'lxml')
    try:
        table = url_soup.find('table', {'summary': 'Representatives '}).find_all('a')
        rep_lst = []
        for item in table:
            if 'Rep.' in item.text:
                name = item.text.replace('Rep.','').strip()
                link = 'http://www.rilegislature.gov'+item.get('href')
                rep_lst.append({
                    'link': link,
                    'name': name,
                })

        return rep_lst
    except:
        pass


def get_rep_info(link):
    try:
        url_request = UrlRequest.make_request(link, header)
        url_soup = BeautifulSoup(url_request.content, 'lxml')
        # list index out of range error
        text = url_soup.text.replace('\n', ' ').split('Sincerely,')[1].split('Officials')[0]
        text = text.replace('\xa0', ' ').strip()
        try:
            district = re.search('[A-Z] - District [0-9]+', text).group()
        except:
            district = re.search('District [0-9]+', text).group()
        name = text.split(district)[0].strip()
        try:
            email = 'rep' + text.split('rep')[1].split('\r')[0]
        except:
            email = ''
        return {
            'name': name,
            'district': district,
            'email': email,
            'link': link
        }
    except:
        return {
            'district': '',
            'email': ''
        }


def scrape_rep(rep_item):
    info = get_sen_info(rep_item['link'])
    row = scraper_utils.initialize_row()
    row.name_full = rep_item['name']
    row.name_first = rep_item['name_first']
    row.name_middle = rep_item['name_middle']
    row.name_last = rep_item['name_last']
    row.source_url = rep_item['link']
    row.role = rep_item['Role']

    if 'party' in rep_item:
        party = rep_item['party']
        row.party = party
        if party != '':
            try:
                row.party_id = scraper_utils.get_party_id(party)
            except Exception:
                pass

    try:
        row.district = info['district']
        row.email = info['email']
    except:
        pass

    if 'wiki_link' in rep_item:
        # party = get_party(rep_item['wiki_link'])
        wiki_info = scraper_utils.scrape_wiki_bio(rep_item['wiki_link'])
        row.birthday = wiki_info['birthday']
        row.education = wiki_info['education']
        row.occupation = wiki_info['occupation']
        row.years_active = wiki_info['years_active']
        row.most_recent_term_id = wiki_info['most_recent_term_id']
        get_wiki_url(row)
        gender = scraper_utils.get_legislator_gender(row.name_first, row.name_last)
        if not gender:
            gender = 'O'
        row.gender = gender
    print(row)
    return row


def find_individual_wiki(wiki_page_link):
    bio_lnks = []
    uClient = uReq(wiki_page_link)
    page_html = uClient.read()
    uClient.close()

    page_soup = BeautifulSoup(page_html, "lxml")
    tables = page_soup.findAll("table")
    rows = tables[3].findAll("tr")

    for person in rows[1:]:
        info = person.findAll("td")
        try:
            biolink = info[1].a["href"]

            bio_lnks.append(biolink)

        except Exception:
            pass
    scraper_utils.crawl_delay(crawl_delay)
    return bio_lnks


if __name__ == '__main__':
    sen_wiki_lst = sen_wiki(wiki_url)
    rep_wiki_lst = rep_wiki(wiki_url)
    print('done wiki lists')
    sen_lst = make_sen_lst(sen_url)
    rep_lst = make_rep_lst(rep_url)
    for item in sen_lst:
        hn = HumanName(item['name'])
        name_first = hn.first
        name_middle = hn.middle
        name_last = hn.last
        item['name_first'] = name_first
        item['name_middle'] = name_middle
        item['name_last'] = name_last
        item['Role'] = 'Senator'

        for el in sen_wiki_lst:
            if name_first in el['wiki_link'] and name_last in el['wiki_link']:
                item['wiki_link'] = 'https://en.wikipedia.org' + el['wiki_link']
                item['party'] = el['party']
        print(item)

    for item in rep_lst:
        hn = HumanName(item['name'])
        name_first = hn.first
        name_middle = hn.middle
        name_last = hn.last
        item['name_first'] = name_first
        item['name_middle'] = name_middle
        item['name_last'] = name_last
        item['Role'] = 'Representative'

        for el in sen_wiki_lst:
            if name_first in el['wiki_link'] and name_last in el['wiki_link']:
                item['wiki_link'] = 'https://en.wikipedia.org' + el['wiki_link']
                item['party'] = el['party']
        print(item)

    with Pool() as pool:
        sen_data = pool.map(scrape_sen, sen_lst)

    with Pool() as pool:
        rep_data = pool.map(scrape_rep, rep_lst)

    data = sen_data + rep_data
    print(len(data))
    leg_df = pd.DataFrame(data)

    # getting urls from ballotpedia
    wikipage_reps = "https://ballotpedia.org/Rhode_Island_House_of_Representatives"
    wikipage_senate = "https://ballotpedia.org/Rhode_Island_State_Senate"

    all_wiki_links = (find_individual_wiki(wikipage_reps) + find_individual_wiki(wikipage_senate))

    with Pool() as pool:
        wiki_data = pool.map(scraper_utils.scrape_ballotpedia_bio, all_wiki_links)
    wiki_df = pd.DataFrame(wiki_data)[
        ['name_last', 'wiki_url']]

    big_df = pd.merge(leg_df, wiki_df, how='left',
                      on=["name_last", 'wiki_url'])

    big_df.drop(big_df.index[big_df['wiki_url'] == ''], inplace=True)

    big_list_of_dicts = big_df.to_dict('records')

    print('Writing data to database...')

    scraper_utils.write_data(big_list_of_dicts)

    print('complete!')
