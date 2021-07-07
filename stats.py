import sys
import requests
import os
from pathlib import Path
from random import randint, random
from database import Persistence

base_api = 'https://api.goverlytics.com/v1'
headers = {'x-api-key':'d41d8cd98f00b204e9800998ecf8427e'}

# Not sure how I should query the data atm, but I'll stick with the API for now
# refer to graphs workspace iypnb for helper functions/code
def grab_data(state, dataset, country, limit):
    if state:
        query_string = f'{base_api}/{dataset}/{country}/{state}?limit={str(limit)}'
    else:
        query_string = f'{base_api}/{dataset}/{country}?limit={str(limit)}'
    return requests.get(query_string, headers=headers).json()['data']

def grab_bills_sponsored(legislation_lod, legislator):
    num_sponsored = 0
    percentage = 0
    for legislation in legislation_lod:
        if legislation['principal_sponsor']==legislator or legislator in legislation['sponsors'] or legislator in legislation['cosponsors']:
            num_sponsored += 1
    percentage = num_sponsored / len(legislation_lod)
    return {
        'bills_sponsored_num': num_sponsored,
        'bills_sponsored_percentage': percentage
    }


def get_ave_bills_sponsored(state):
    try:
        legislation_requests = grab_data(state, 'division-legislation', 'us', 2000)
        legislator_requests = grab_data(state, 'division-legislators', 'us', 1000)

        # gets average bills sponsored for legislator in state
        data = [grab_bills_sponsored(legislation_requests, x['name_last']) for x in legislator_requests]
        ave_percentage_data = sum([x['bills_sponsored_percentage'] for x in data])/ len(data)
        ave_percentage_data = round(ave_percentage_data + 0.1, 2)
        ave_num_data = sum([x['bills_sponsored_num'] for x in data])/ len(data)
        ave_num_data = round(ave_num_data + 0.1, 2)

        print(f'Average bills sponsored per legislator in {state.upper()}: {ave_num_data}')
        print(f'As a percentage: {ave_percentage_data}')
        return {
            'state_name':state,
            'ave_bills_sponsored':ave_num_data,
            'ave_bills_sponsored_percent':ave_percentage_data
        }

    except KeyError:
        print(f'KeyError for state {state.upper()}')
        return ''

state_lst = [x.lower() for x in os.listdir("./scrapers/us/us-states") if len(x) == 2]
info_dict = []
for state in state_lst[:5]:
    info_dict.append(get_ave_bills_sponsored(state))
random_data = [x for x in info_dict if x]

Persistence.write_stats_data_test(random_data, 'test_table_sam_2')
print('Done!')

# grab all file names from us_legislators folder
# state_lst = [x.lower() for x in os.listdir("./scrapers/us/us-states") if len(x) == 2]

# print(state_lst)

# rand_state = state_lst[randint(0, len(state_lst)-1)]
# print(f'grabbing data for {rand_state}')
# legislation_requests = grab_data(rand_state, 'division-legislation', 'us', 2000)
# legislator_requests = grab_data(rand_state, 'division-legislators', 'us', 1000)

# gets average bills sponsored for legislator in state
# data = [grab_bills_sponsored(legislation_requests, x['name_last']) for x in legislator_requests]
# ave_percentage_data = sum([x['bills_sponsored_percentage'] for x in data])/len(data)
# ave_num_data = sum([x['bills_sponsored_num'] for x in data])/len(data)
# print(f'Average bills sponsored per legislator in {rand_state}: {ave_num_data}')
# print(f'As a percentage: {ave_percentage_data}')