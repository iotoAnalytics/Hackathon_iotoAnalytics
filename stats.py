import sys
import requests
import os
from pathlib import Path
import boto3
from random import randint
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
# Get path to the root directory so we can import necessary modules

# makes dummy table in database
# Persistence.write_sam_data_test('test_table_sam')
# print('Done!')

# grab all file names from us_legislators folder
state_lst = [x.lower() for x in os.listdir("./scrapers/us/us-states") if len(x) == 2]

# print(state_lst)

rand_state = state_lst[randint(0, len(state_lst)-1)]
print(f'grabbing data for {rand_state}')
legislation_requests = grab_data(rand_state, 'division-legislation', 'us', 2000)
legislator_requests = grab_data(rand_state, 'division-legislators', 'us', 1000)

# gets average bills sponsored for legislator in state
data = [grab_bills_sponsored(legislation_requests, x['name_last']) for x in legislator_requests]
ave_percentage_data = sum([x['bills_sponsored_percentage'] for x in data])/len(data)
ave_num_data = sum([x['bills_sponsored_num'] for x in data])/len(data)
print(f'Average bills sponsored per legislator in {rand_state}: {ave_num_data}')
print(f'As a percentage: {ave_percentage_data}')