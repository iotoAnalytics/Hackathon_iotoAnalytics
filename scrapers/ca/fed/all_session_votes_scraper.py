import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import urllib3
from multiprocessing import Pool
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from database import Persistence

base_api = 'https://api.goverlytics.com'
headers = {'x-api-key': 'Nli5LOs3CY6R306m1njj44SJS58v8y6w68zMBZyU'}

base_link = 'https://www.ourcommons.ca/members/en/'

def grab_sessions(link):
    soup = BeautifulSoup(requests.get(link).content, 'lxml')
    dropdown = soup.find('div', {'id':'search-refiner'}).find('ul', {'class':'dropdown-menu'}).find_all('li')
    return [x.find('a').get('data-value') for x in dropdown]
    
    
def get_data(legislator_dict):
    legislator_id = legislator_dict['source_id']
    goverlytics_id = legislator_dict['goverlytics_id']
    data = []
    name = legislator_dict['name_full']
    print(f'Getting data for {name}')
    try:
        sessions = grab_sessions(f'{base_link}{legislator_id}/votes')
        for session in tqdm(sessions):
            soup = BeautifulSoup(requests.get(f'{base_link}{legislator_id}/votes?parlSession={session}#').content, 'lxml')
            table = soup.find('div', {'id':'vote-list-view'}).find('table')
            pd_table = pd.read_html(str(table))[0].rename(columns={'Vote\xa0Number': 'Vote Number', 'Subject':'Bill Summary'})
            table_dict = pd_table.to_dict('records')
            for dictionary in table_dict:
                dictionary['goverlytics_id'] = goverlytics_id
                dictionary['Session'] = session
            data += table_dict
    except Exception:
        pass
    if type(data)==list and len(data) > 0:
        vote_pd = pd.DataFrame(data)
        data = vote_pd.where(pd.notnull(vote_pd), None).to_dict('records')
    return {
            'name': name,
            'goverlytics_id': goverlytics_id,
            'source_id': legislator_id,
            'voting_data': data
        }

def get_api_data(qs):
    response = requests.get(base_api + qs, headers=headers).json()
    data = response['data']
    next_url = response['pagination']['next_url']
    if next_url:
        return data + get_api_data(next_url)
    else:
        return data


if __name__ == '__main__':
    # # First we'll get the URLs we wish to scrape:
    legislator_data = get_api_data('/v1/federal-legislators/ca')
    with Pool() as pool:
        data = pool.map(get_data, legislator_data)
    data = [x for x in data if x]
    
    Persistence.write_ca_vote_data(data, 'ca_fed_voting_data_all_sessions')
    print('Complete!')
