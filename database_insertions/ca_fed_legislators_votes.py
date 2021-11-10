# import database as db
import sys
import os
from pathlib import Path

from psycopg2.extras import RealDictCursor
from psycopg2 import sql
import pandas as pd

p = Path(os.path.abspath(__file__)).parents[1]

sys.path.insert(0, str(p))

import scraper_utils
import database as db

with db.CursorFromConnectionFromPool() as cur:
    try:
        table_1, table_2 = 'ca_fed_legislation', 'ca_fed_legislation_topic_probs'
        joined_table = f'{table_1} LEFT JOIN {table_2} ON {table_1}.goverlytics_id = {table_2}.goverlytics_id'
        query_string = sql.SQL(""" 
            SELECT ca_fed_legislation.goverlytics_id, 
                   ca_fed_legislation.bill_name, 
                   ca_fed_legislation.current_status, 
                   ca_fed_legislation.votes, ca_fed_legislation_topic_probs.topic 
            FROM {table} WHERE ca_fed_legislation.votes != '[]' LIMIT 25    
            """).format(
                table=sql.SQL(joined_table)
            )
        cur.execute(query_string)
        data = cur.fetchall()

        legislator_dict = {}

        for item in data:
            temp_vote_obj = item['votes'][0]
            for vote in temp_vote_obj['votes']:
                vote_info_lst = [{
                        'name': vote['legislator'],
                        'vote_text': vote['vote_text'],
                        'goverlytics_id': vote['goverlytics_id'],
                        'vote_session_date': temp_vote_obj['date'],
                        'vote_description': temp_vote_obj['description'],
                        'goverlytics_id': item['goverlytics_id'],   
                        'bill_name': item['bill_name'],
                        'current_status': item['current_status'],
                        'topic': item['topic']
                    }]

                govId = vote['goverlytics_id']
                if govId in legislator_dict:
                    legislator_dict[govId] += vote_info_lst
                else:
                    legislator_dict[govId] = vote_info_lst

        print(legislator_dict)
        # count up legislator votes, along with relevant info 
    except Exception as e:
        print(f'ERROR: \n {e} \n')
        cur.connection.rollback()
