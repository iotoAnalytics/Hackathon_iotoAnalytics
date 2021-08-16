import sys
import requests
import os
from pathlib import Path
from random import randint, random
from database import Persistence
from datetime import datetime

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


def get_legislator_count(data):
    current_year = datetime.today().year
    legislator_lst = []
    for legislator in data:
        most_recent_term_id = legislator['most_recent_term_id']
        years_active = legislator['years_active']
        try:
            if most_recent_term_id == current_year or current_year in years_active:
                legislator_lst.append(legislator)
        except TypeError:
            pass
    return len(legislator_lst)


def get_bday_and_activity(legislator_data):
    bday_lst, years_active_lst = [], []
    for legislator in legislator_data:
        birthday = legislator['birthday']
        years_active = legislator['years_active']
        if years_active:
            years_active = len(legislator['years_active'])
            years_active_lst.append(years_active)
        if birthday:
            bday_lst.append(datetime.today().year - int(birthday.split('-')[0]))
        # print(f'\nBirthday: {birthday}\nYears Active: {years_active}')
    return [bday_lst, years_active_lst]


def get_topics(country, state):
    # print(f'getting state: {state}')
    topic_dict = {}
    data =requests.get(f'{base_api}/division-legislation/{country}/{state}?limit=2000', headers=headers).json()['data']
    for legislation in data:
        topic = legislation['topic']
        if topic:
            if topic in topic_dict: 
                topic_dict[topic] += 1
            else:
                topic_dict[topic] = 1
    return topic_dict


def get_all_data(country, state):
    ave_num_data = None
    ave_percentage_data = None
    ave_age = 0
    ave_years_active = 0
    legislator_count = None
    topic_dict = get_topics(country, state)
    try:
        legislator_requests = grab_data(state, 'division-legislators', country, 1000)
        legislator_count = get_legislator_count(legislator_requests) if len(legislator_requests) > 200 else len(legislator_requests)
        birthday, years_active = get_bday_and_activity(legislator_requests)
        ave_age = round(sum(birthday)/ len(birthday) + 0.1, 2) if len(birthday) != 0 else 0
        ave_years_active = round(sum(years_active)/ len(years_active) + 0.1, 2) if len(years_active) != 0 else 0

        legislation_requests = grab_data(state, 'division-legislation', country, 2000)

        # gets average bills sponsored for legislator in state
        data = [grab_bills_sponsored(legislation_requests, x['name_last']) for x in legislator_requests]
        ave_percentage_data = sum([x['bills_sponsored_percentage'] for x in data])/ len(data)
        ave_percentage_data = round(ave_percentage_data + 0.1, 2)
        ave_num_data = sum([x['bills_sponsored_num'] for x in data])/ len(data)
        ave_num_data = round(ave_num_data + 0.1, 2)

        print(f'Average bills sponsored per legislator in {state.upper()}: {ave_num_data}')
        print(f'As a percentage: {ave_percentage_data}')

    except KeyError:
        print(f'KeyError for state {state.upper()}')
    return {
        'state_name':state,
        'ave_bills_sponsored':ave_num_data,
        'ave_bills_sponsored_percent':ave_percentage_data,
        'legislator_count': legislator_count,
        'ave_age': ave_age,
        'ave_years_active': ave_years_active,
        'topics_count':topic_dict
    }


# def get_all_data(state):
#     info_dict = get_ave_bills_sponsored(state)
#     return info_dict
state_dir = "./scrapers/ca/ca-provinces"
state_lst = [x.lower() for x in os.listdir(state_dir) if len(x) == 2]
info_dict = []
for state in state_lst:
    info_dict.append(get_all_data('ca', state))
data = [x for x in info_dict if x]

print(data)
Persistence.write_stats_data_test(data, 'canadian_summary_data')
print('Done!')